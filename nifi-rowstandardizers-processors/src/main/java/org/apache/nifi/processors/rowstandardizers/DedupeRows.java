/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.rowstandardizers;


import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.AbstractObject2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.util.StopWatch;
import org.bouncycastle.crypto.digests.Blake2bDigest;
import org.bouncycastle.util.encoders.Hex;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"text formating standardization deduplication"})
@CapabilityDescription("Performs row based deduplication of the flow file content")
@WritesAttribute(attribute = "Duplicates Removed", description = "This Processor adds an attribute whose value is the " +
        "number of duplicates removed from the flow file content")

public class DedupeRows extends AbstractProcessor {

    private static final AllowableValue SINGLE_QUOTE = new AllowableValue(Character.toString('\''), "Single Quote");
    private static final AllowableValue TICK_MARK = new AllowableValue(Character.toString('`'), "Tick Mark");
    private static final AllowableValue DOUBLE_QUOTE = new AllowableValue(Character.toString('"'), "Double Quote");

    private static final AllowableValue EQUAL_TO = new AllowableValue(Character.toString('='), "Equal To");
    private static final AllowableValue GREATER_THAN = new AllowableValue(Character.toString('>'), "Greater Than");
    private static final AllowableValue LESS_THAN = new AllowableValue(Character.toString('<'), "Less Than");

    private static final AllowableValue NUMERIC = new AllowableValue("n", "Numeric");
    private static final AllowableValue DATE = new AllowableValue("d", "Date");


    private static final PropertyDescriptor INPUT_DELIMITER = new PropertyDescriptor
            .Builder().name("Input Delimiter")
            .description("The delimiter used to demarcate columns in the flow file content")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor OUTPUT_NEWLINE_DELIMITER = new PropertyDescriptor
            .Builder().name("Output Newline Delimiter")
            .description("The delimiter used to demarcate lines in the resulting flow file content")
            .required(false)
            .build();

    private static final PropertyDescriptor INPUT_QUOTE_CHARACTER = new PropertyDescriptor
            .Builder().name("Input Quote Character")
            .description("Example Property")
            .required(true)
            .allowableValues(SINGLE_QUOTE, DOUBLE_QUOTE, TICK_MARK)
            .build();

    private static final PropertyDescriptor COMPARISON_OPERATOR = new PropertyDescriptor
            .Builder().name("Comparison Operator")
            .description("The comparison operator to be used when comparing the rows using the comparison column")
            .required(true)
            .allowableValues(EQUAL_TO, GREATER_THAN, LESS_THAN)
            .build();

    private static final PropertyDescriptor COMPARISON_COLUMN = new PropertyDescriptor
            .Builder().name("Comparison Column")
            .description("The one's based comparison column to be used when comparing the rows using the comparison operator")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    private static final PropertyDescriptor COMPARISON_TYPE = new PropertyDescriptor
            .Builder().name("Comparison Column Data Type")
            .description("The data type of of the comparison column")
            .required(true)
            .allowableValues(NUMERIC, DATE)
            .build();

    private static final Validator DATEFORMAT_VALIDATOR = (subject, value, context) -> {
        if (null == value || value.isEmpty()) {
            return new ValidationResult.Builder().subject(subject).input(value).valid(false).explanation(subject + " cannot be empty").build();
        }
        try {
            java.time.format.DateTimeFormatter.ofPattern(value);
        } catch (IllegalArgumentException exception) {
            return new ValidationResult.Builder().subject(subject).input(value).valid(false).explanation(exception.getMessage()).build();
        }
        return new ValidationResult.Builder().subject(subject).input(value).valid(true).build();
    };


    private static final PropertyDescriptor DATE_FORMAT = new PropertyDescriptor
            .Builder().name("Date Format")
            .description("The Java date format pattern to use when parsing the comparison column if it is a date")
            .required(false)
            .addValidator(DATEFORMAT_VALIDATOR)
            .build();

    private static final PropertyDescriptor KEY_COLUMNS = new PropertyDescriptor
            .Builder().name("Key Columns")
            .description("The columns that make up the key for the row, used to identify duplicates")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();

    private static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure relationship")
            .build();

    private static final List<PropertyDescriptor> properties;
    private static final Set<Relationship> relationships;

    static {
        properties = Collections.unmodifiableList(Arrays.asList(
                OUTPUT_NEWLINE_DELIMITER,
                INPUT_DELIMITER,
                INPUT_QUOTE_CHARACTER,
                COMPARISON_OPERATOR,
                COMPARISON_COLUMN,
                DATE_FORMAT,
                COMPARISON_TYPE,
                KEY_COLUMNS));

        relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(new Relationship[]{
                REL_SUCCESS,
                REL_FAILURE
        })));
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


    /*
        The following is based on the various Fixed-Sized Chunking algorithms that you may find in
        the Backup Literature.

        First Pass
        1. Read through the file
        2. Determine the Key for detecting duplicates
        3. Split the line to grab the Key
        4. Hash the key
        5. Store the key and comparison column(s)
        6. If the key is found do the following:
            1. The configured comparison --
                1. Grab the comparison columns
            2. For the losing row set the bit for its offset


        Second Pass
        1. Read through the file
        2. If the offest is not set in the bitmap, right it out otherwise skip it.

     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }


        final StopWatch stopWatch = new StopWatch(true);
        final char columnDelimiter = context.getProperty(INPUT_DELIMITER).getValue().charAt(0);
        final char inputQuoteCharacter = context.getProperty(INPUT_QUOTE_CHARACTER).getValue().charAt(0);
        final String outputNewLineSeperator = context.getProperty(OUTPUT_NEWLINE_DELIMITER).isSet() ?
                context.getProperty(OUTPUT_NEWLINE_DELIMITER).getValue() :
                System.lineSeparator();
        final char comparisonOperator = context.getProperty(COMPARISON_OPERATOR).getValue().charAt(0);
        final int comparisonColumn = context.getProperty(COMPARISON_COLUMN).asInteger();
        final char comparisonType = context.getProperty(COMPARISON_TYPE).getValue().charAt(0);
        final String dateFormat = context.getProperty(DATE_FORMAT).getValue();
        final String[] rawKeyColumns = context.getProperty(KEY_COLUMNS).getValue().split(",");

        int[] keyColumns = new int[rawKeyColumns.length];
        for (int i = 0; i < rawKeyColumns.length; i++) {
            keyColumns[i] = Integer.parseInt(rawKeyColumns[i].trim());
        }

        final AtomicReference<String> duplicateCountValueHolder = new AtomicReference<>(null);
        final AtomicReference<LongArrayList> atomicDiscardList = new AtomicReference<>(null);

        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream inputStream) throws IOException {

                    //first build the index
                    LongArrayList discardList = buildIndex(inputStream, columnDelimiter, inputQuoteCharacter, comparisonColumn, comparisonType,
                            dateFormat, comparisonOperator, keyColumns);

                    duplicateCountValueHolder.set(Integer.toString(discardList.size()));
                    atomicDiscardList.set(discardList);
                }
            });
        } catch (final ProcessException e) {
            session.transfer(flowFile, REL_FAILURE);
            throw e;
        }
        try {
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream inputStream, final OutputStream outputStream) throws IOException {
                    writeOutput(inputStream, outputStream, outputNewLineSeperator, atomicDiscardList.get());
                    outputStream.flush();
                }
            });
            stopWatch.stop();
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getDuration(TimeUnit.MILLISECONDS));
            flowFile = session.putAttribute(flowFile, "duplicate.rows.removed", duplicateCountValueHolder.get());
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final ProcessException e) {
            session.transfer(flowFile, REL_FAILURE);
            throw e;
        }

    }

    /**
     * This will walk the flowfile content and do the following:
     * 1. Split it line by line
     * 2. Grab the key columns
     * 3. Hash the key columns
     * 4. Check and see if they are in the duplicate list
     * 5. If it is in the Duplicate list then check the comparison columns and see if it stays in the dup..
     */
    protected LongArrayList buildIndex(final InputStream inputStream,
                                       final char columnDelimiter,
                                       final char inputQuoteCharacter,
                                       final int comparisonColumn,
                                       final char comparisonColumnType, final String dateFormat, final char comparisonOperator,
                                       int[] keyColumns) throws IOException {

        final Blake2bDigest blake2bDigest = new Blake2bDigest();
        final LongArrayList discardList = new LongArrayList();

        java.time.format.DateTimeFormatter dateFormatter = null;
        if (null != dateFormat) {
            dateFormatter = java.time.format.DateTimeFormatter.ofPattern(dateFormat);
        }

        Object2ObjectOpenHashMap<String, Object2LongMap.Entry<String>> keyToComparisonMapping = new Object2ObjectOpenHashMap<>();
        //need to store the key, the comparison colunmns, and the line number, and will then do a key check against
        //the key and a comparison check
        //as a dup is found the dup list will be updated to ignore certain lines


        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setDelimiter(columnDelimiter);
        settings.getFormat().setQuote(inputQuoteCharacter);
        CsvParser parser = new CsvParser(settings);

        String nextLine;
        long lineNumber = 0;
        try (LineNumberReader lineNumberReader = new LineNumberReader(new InputStreamReader(inputStream))) {
            while ((nextLine = lineNumberReader.readLine()) != null) {
                lineNumber = lineNumberReader.getLineNumber();

                final String[] columns = parser.parseAll(new StringReader(nextLine)).get(0);

                //get the comparison column
                //if it is larger then the number of columns, we need to bail on this line and perhaps even the file!!!
                if (comparisonColumn > columns.length) {
                    getLogger().error("The comparison column {} is outside the bounds than the number of columns in the line.",
                            new Object[]{comparisonColumn});
                    getLogger().error("Skipping line {} due to invalid comparison column.", new Object[]{lineNumber});
                } else {
                    final String comparisonColumnValue = columns[comparisonColumn];
                    //get the hash columns, just stitch it together as a string
                    for (int i = 0; i < keyColumns.length; i++) {
                        if (keyColumns[i] > columns.length) {
                            //This is an error and we need to do something to this line... perhaps bail on the key -- perhaps
                            //bail on the whole file!!!!
                            //At a minimum we need to log it
                            getLogger().error("The key column {} is longer than the number of columns in the line.",
                                    new Object[]{keyColumns[i]});
                        } else {
                            blake2bDigest.update(columns[keyColumns[i]].getBytes(StandardCharsets.UTF_8), 0, columns[keyColumns[i]].length());
                        }
                    }

                    byte[] keyDigest = new byte[blake2bDigest.getDigestSize()];
                    blake2bDigest.doFinal(keyDigest, 0);
                    final String hexKey = Hex.toHexString(keyDigest);

                    //if the key exists, then we need to do the comparison, otherwise just put it in map
                    if (!keyToComparisonMapping.containsKey(hexKey)) {
                        keyToComparisonMapping.put(hexKey, new AbstractObject2LongMap.BasicEntry<>(comparisonColumnValue, lineNumber));
                    } else {
                        //get the comparison and if this is greater then we put this in the
                        Object2LongMap.Entry<String> currentEntry = keyToComparisonMapping.get(hexKey);

                        if (NUMERIC.getValue().charAt(0) == comparisonColumnType) {

                            long existingLineComparisonCol = Long.parseLong(currentEntry.getKey());
                            long currentLineComparisonCol = Long.parseLong(comparisonColumnValue);

                            //now do the comparison
                            if (comparisonOperator == GREATER_THAN.getValue().charAt(0)) {
                                if (currentLineComparisonCol > existingLineComparisonCol) {
                                    // need to replace the mapping and add the existing to the discard list
                                    keyToComparisonMapping.put(hexKey, new AbstractObject2LongMap.BasicEntry<>(comparisonColumnValue, lineNumber));
                                    discardList.add(currentEntry.getLongValue());
                                }
                            }
                            if (comparisonOperator == LESS_THAN.getValue().charAt(0)) {
                                if (currentLineComparisonCol > existingLineComparisonCol) {
                                    // leave the existing and add the current line to the discard
                                    discardList.add(lineNumber);
                                }
                            }
                            if (comparisonOperator == EQUAL_TO.getValue().charAt(0)) {
                                if (currentLineComparisonCol == existingLineComparisonCol) {
                                    // discard the current line
                                    discardList.add(lineNumber);
                                }
                            }
                        } else {
                            //it is a date, so lets use JODA time to parse and compare...
                            LocalDateTime existingDateTime = LocalDateTime.parse(currentEntry.getKey(), dateFormatter);
                            LocalDateTime currentDateTime = LocalDateTime.parse(comparisonColumnValue, dateFormatter);
                            if (comparisonOperator == GREATER_THAN.getValue().charAt(0)) {
                                if (currentDateTime.isAfter(existingDateTime)) {
                                    keyToComparisonMapping.put(hexKey, new AbstractObject2LongMap.BasicEntry<>(comparisonColumnValue, lineNumber));
                                    discardList.add(currentEntry.getLongValue());
                                }
                            }
                            if (comparisonOperator == LESS_THAN.getValue().charAt(0)) {
                                if (currentDateTime.isAfter(existingDateTime)) {
                                    discardList.add(lineNumber);
                                }
                            }
                            if (comparisonOperator == EQUAL_TO.getValue().charAt(0)) {
                                if (currentDateTime.isEqual(existingDateTime)) {
                                    // discard the current line
                                    discardList.add(lineNumber);
                                }
                            }
                        }
                    }
                }
            }
        }

        return discardList;
    }

    protected void writeOutput(final InputStream inputStream, final OutputStream outputStream, final String outputNewLineSeperator, final LongArrayList discardList) throws IOException {

        try (final OutputStream bufferedOut = new BufferedOutputStream(outputStream, 65536);
             final InputStream bufferedIn = new BufferedInputStream(inputStream, 65536)) {

            try (LineNumberReader lineNumberReader = new LineNumberReader(new InputStreamReader(bufferedIn))) {
                String nextLine;
                long lineNumber = 0;
                while ((nextLine = lineNumberReader.readLine()) != null) {
                    lineNumber = lineNumberReader.getLineNumber();
                    if (!discardList.contains(lineNumber)) {
                        bufferedOut.write(nextLine.getBytes());
                        bufferedOut.write(outputNewLineSeperator.getBytes());
                    }
                }
            }
        }
    }
}

