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

import com.google.common.base.Joiner;
import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Tags({"text formating standardization "})
@CapabilityDescription("Removes embedded newlines from a delimited row in a file")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class EmbeddedNewLineRemover extends AbstractProcessor {

    private static final AllowableValue TAB = new AllowableValue(Character.toString('\t'), "Tab");
    private static final AllowableValue COMMA = new AllowableValue(",", "Comma");
    private static final AllowableValue COLON = new AllowableValue(":", "Colon");
    private static final AllowableValue SEMI_COLON = new AllowableValue(";", "Semi-colon");
    private static final AllowableValue PIPE = new AllowableValue("|", "Pipe");


    private static final PropertyDescriptor NUM_COLUMNS = new PropertyDescriptor
            .Builder().name("Number of Columns")
            .description("Number of columns per line")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final PropertyDescriptor INPUT_DELIMITER = new PropertyDescriptor
            .Builder().name("Input Delimiter")
            .description("Example Property")
            .required(true)
            .allowableValues(TAB, COMMA, COLON, SEMI_COLON, PIPE)
            .build();

    private static final PropertyDescriptor INPUT_CHARSET = new PropertyDescriptor
            .Builder().name("Input Charset")
            .description("The input character set")
            .required(false)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();

    private static final Relationship VALIDATION_ERRORS = new Relationship.Builder()
            .name("validation errors")
            .description("validation errors")
            .build();




    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(NUM_COLUMNS);
        descriptors.add(INPUT_DELIMITER);
        descriptors.add(INPUT_CHARSET);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(VALIDATION_ERRORS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final char inputDelimiter = context.getProperty(INPUT_DELIMITER).getValue().charAt(0);
        final int numColumns = context.getProperty(NUM_COLUMNS).evaluateAttributeExpressions().asInteger();

        final StringBuilder cleanLines = new StringBuilder();
        session.read(flowFile, in -> {
            try (BufferedInputStream streamReader = new BufferedInputStream(in)) {
                cleanLines.append(stripNewLines(streamReader, inputDelimiter, numColumns));
            }
        });

        flowFile = session.write(flowFile, out -> out.write(cleanLines.toString().getBytes()));
        session.transfer(flowFile, SUCCESS);

    }

    protected String stripNewLines(BufferedInputStream streamReader, final char delimiter, int numColumns) throws IOException {

        final StringBuilder out = new StringBuilder();
        int columnsSeen = 0;
        int currentChar = streamReader.read();

        while(-1 != currentChar ){
            if (columnsSeen < numColumns) {
                if ((char)currentChar == delimiter) {
                    columnsSeen++;
                    out.append((char)currentChar);
                } else {
                    if (currentChar != 10 && currentChar != 13) {
                        out.append((char)currentChar);
                    }

                    streamReader.mark(1);
                    int nextChar = streamReader.read();

                    if((nextChar == 10 || nextChar == 13) && (columnsSeen +1 == numColumns)) {
                        out.append((char)nextChar);
                        columnsSeen++;
                    }else{
                        streamReader.reset();
                    }
                }

            }else{
                //This should handle valid non-newline characters, as there maybe new-lines
                if(currentChar != 10 && currentChar != 13) {
                    out.append((char) currentChar);
                    columnsSeen = 0;
               }
            }

            currentChar = streamReader.read();
        }
        return out.toString();
    }
}
