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
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Tags({"text formating standardization"})
@CapabilityDescription("Performs standardization of a flow file content")
@EventDriven
@WritesAttributes({
        @WritesAttribute(attribute = "fragment.out.header"),
        @WritesAttribute(attribute = "fragment.out.delimiter"),
        @WritesAttribute(attribute = "fragment.out.separator")
})
public class FormatDelimitedRow extends AbstractProcessor {

    private static final AllowableValue TAB = new AllowableValue(Character.toString('\t'), "Tab");
    private static final AllowableValue COMMA = new AllowableValue(",", "Comma");
    private static final AllowableValue COLON = new AllowableValue(":", "Colon");
    private static final AllowableValue SEMI_COLON = new AllowableValue(";", "Semi-colon");
    private static final AllowableValue PIPE = new AllowableValue("|", "Pipe");

    private static final AllowableValue SINGLE_QUOTE = new AllowableValue(Character.toString('\''), "Single Quote");
    private static final AllowableValue TICK_MARK = new AllowableValue(Character.toString('`'), "Tick Mark");
    private static final AllowableValue DOUBLE_QUOTE = new AllowableValue(Character.toString('"'), "Double Quote");

    private static final AllowableValue TRUE = new AllowableValue("True");
    private static final AllowableValue FALSE = new AllowableValue("False");

    public static final PropertyDescriptor HEADER_LINE = new PropertyDescriptor.Builder()
            .name("Header Line")
            .description("The first line of the file contains the header row.")
            .required(false)
            .allowableValues(TRUE, FALSE)
            .defaultValue(TRUE.getValue())
            .build();

    private static final PropertyDescriptor INPUT_DELIMITER = new PropertyDescriptor
            .Builder().name("Input Delimiter")
            .description("Example Property")
            .required(true)
            .allowableValues(TAB, COMMA, COLON, SEMI_COLON, PIPE)
            .build();

    private static final PropertyDescriptor INPUT_CHARSET = new PropertyDescriptor
            .Builder().name("Input Charset")
            .description("Example Property")
            .required(false)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor INPUT_QUOTE_CHARACTER = new PropertyDescriptor
            .Builder().name("Input Quote Character")
            .description("Example Property")
            .required(false)
            .allowableValues(SINGLE_QUOTE, DOUBLE_QUOTE, TICK_MARK)
            .build();

    private static final PropertyDescriptor AVRO_SCHEMA = new PropertyDescriptor
            .Builder().name("Avro Schema")
            .description("Example Property")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    private static final PropertyDescriptor INPUT_ISO_LANGUAGE = new PropertyDescriptor
            .Builder().name("ISO Language")
            .description("An ISO 639 alpha-2 or alpha-3 language code, or a language subtag up to 8 characters in length")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(Locale.ENGLISH.getLanguage())
            .build();

    private static final PropertyDescriptor INPUT_ISO_COUNTRY = new PropertyDescriptor
            .Builder().name("ISO Country")
            .description("An ISO 3166 alpha-2 country code or a UN M.49 numeric-3 area code.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(Locale.US.getCountry())
            .build();

    private static final PropertyDescriptor OUTPUT_DELIMITER = new PropertyDescriptor
            .Builder().name("Output Delimiter")
            .description("Example Property")
            .required(false)
            .allowableValues(TAB, COMMA, COLON, SEMI_COLON, PIPE)
            .defaultValue(PIPE.getValue())
            .build();

    private static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();

    private static final Relationship VALIDATION_ERRORS = new Relationship.Builder()
            .name("validation errors")
            .description("validation errors")
            .build();

    private static final DateTimeFormatter OUTPUT_DATE_FORMAT = DateTimeFormat.forPattern("MM-dd-yyyy");


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(HEADER_LINE);
        descriptors.add(INPUT_DELIMITER);
        descriptors.add(AVRO_SCHEMA);
        descriptors.add(INPUT_QUOTE_CHARACTER);
        descriptors.add(INPUT_CHARSET);
        descriptors.add(INPUT_ISO_COUNTRY);
        descriptors.add(INPUT_ISO_LANGUAGE);
        descriptors.add(OUTPUT_DELIMITER);
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
        FlowFile errorFlow = session.clone(flowFile);


        final AtomicReference<String> fileContent = new AtomicReference<>();
        final AtomicReference<String> fileHeader = new AtomicReference<>();

        // don't support expressions
        final boolean firstLineIsHeader = context.getProperty(HEADER_LINE).asBoolean();
        final char inputDelimiter = context.getProperty(INPUT_DELIMITER).getValue().charAt(0);
        final char outputDelimiter = context.getProperty(OUTPUT_DELIMITER).getValue().charAt(0);
        final String inputQuoteCharacter = context.getProperty(INPUT_QUOTE_CHARACTER).getValue();

        // support expressions
        final String inputCharset = context.getProperty(INPUT_CHARSET).evaluateAttributeExpressions(flowFile).getValue();
        final String schemaString = context.getProperty(AVRO_SCHEMA).evaluateAttributeExpressions(flowFile).getValue();
        final String isoCountry = context.getProperty(INPUT_ISO_COUNTRY).evaluateAttributeExpressions(flowFile).getValue();
        final String isoLanguage = context.getProperty(INPUT_ISO_LANGUAGE).evaluateAttributeExpressions(flowFile).getValue();

        final Locale locale = new Locale(isoLanguage, isoCountry);

        final NumberFormat numberFormat = NumberFormat.getNumberInstance(locale);

        final String recordSeparator = System.lineSeparator();

        final Schema schema = new Schema.Parser().parse(schemaString);

        final long flowFileId = flowFile.getId();
        final String filename = flowFile.getAttribute("filename");


        // we don't want to print the header with the new FlowFile

        List<ValidationError> errors = new ArrayList<>();

        if(firstLineIsHeader) {

            final CSVFormat parserFormat = CSVFormat.newFormat(inputDelimiter)
                    .withIgnoreEmptyLines()
                    .withHeader()
                    .withIgnoreSurroundingSpaces()
                    .withQuote(getCharacter(inputQuoteCharacter))
                    .withTrim();

            final CSVFormat printerFormat = CSVFormat.newFormat(outputDelimiter)
                    .withRecordSeparator(recordSeparator);

            session.read(flowFile, in -> {

                StringBuilder output = new StringBuilder();

                try (
                        InputStreamReader streamReader = new InputStreamReader(in, Charset.forName(inputCharset));
                        CSVParser parser = parserFormat.parse(streamReader);
                        CSVPrinter printer = new CSVPrinter(output, printerFormat)
                ) {

                    final Set<String> rawHeaders = parser.getHeaderMap().keySet();

                    // todo: this is kind of ugly; can't we clean headers once & use everywhere
                    fileHeader.set(Joiner.on(outputDelimiter).join(cleanHeaders(rawHeaders)));

                    final List<CSVRecord> records = parser.getRecords();

                    for (CSVRecord record : records) {

                        List<String> values = new ArrayList<>();

                        for (String rawHeader : rawHeaders) {

                            // todo: this is kind of ugly; can't we clean headers once & use everywhere
                            Schema.Field field = schema.getField(cleanHeader(rawHeader));

                            String formattedValue = formatValue(record, field, rawHeader, numberFormat, filename, flowFileId, errors);

                            values.add(formattedValue);
                        }

                        printer.printRecord(values);

                    }

                    fileContent.set(output.toString());

                }
            });

            flowFile = session.putAttribute(flowFile, "fragment.out.header", fileHeader.get());
        }else{
            //the first line does not have the header, so we can deal with the values differently.
            final CSVFormat parserFormat = CSVFormat.newFormat(inputDelimiter)
                    .withIgnoreEmptyLines()
                    .withIgnoreSurroundingSpaces()
                    .withQuote(getCharacter(inputQuoteCharacter))
                    .withTrim();

            final CSVFormat printerFormat = CSVFormat.newFormat(outputDelimiter)
                    .withRecordSeparator(recordSeparator);

            session.read(flowFile, in -> {

                StringBuilder output = new StringBuilder();

                try (
                        InputStreamReader streamReader = new InputStreamReader(in, Charset.forName(inputCharset));
                        CSVParser parser = parserFormat.parse(streamReader);
                        CSVPrinter printer = new CSVPrinter(output, printerFormat)
                ) {

                    final List<CSVRecord> records = parser.getRecords();

                    for (CSVRecord record : records) {

                        List<String> values = new ArrayList<>();

                        for (Schema.Field field : schema.getFields()) {
                            String formattedValue = formatValue(record, field, field.name(), numberFormat, filename, flowFileId, errors);
                            values.add(formattedValue);
                        }
                        printer.printRecord(values);
                    }
                    fileContent.set(output.toString());
                }
            });
        }
        flowFile = session.putAttribute(flowFile, "fragment.out.delimiter", Character.toString(outputDelimiter));
        flowFile = session.putAttribute(flowFile, "fragment.out.separator", recordSeparator);

        flowFile = session.write(flowFile, out -> out.write(fileContent.get().getBytes()));

        session.transfer(flowFile, SUCCESS);

        if (!errors.isEmpty()) {

            try {

                StringBuilder output = buildErrorFile(recordSeparator, errors);

                errorFlow = session.putAttribute(errorFlow, "filename", "errors." + System.currentTimeMillis() + ".txt");

                errorFlow = session.write(errorFlow, out -> out.write(output.toString().getBytes()));

                session.transfer(errorFlow, VALIDATION_ERRORS);

            } catch (Exception e) {
                getLogger().error(e.getMessage(), e);
            }

        } else {
            session.remove(errorFlow);
        }


    }

    private StringBuilder buildErrorFile(String recordSeparator, List<ValidationError> errors) throws IOException {
        StringBuilder output = new StringBuilder();

        final CSVFormat errorFormat = CSVFormat.newFormat('|')
                .withHeader("flowFieldId", "filename", "columnName", "originalValue", "currentValue", "error", "errorClass")
                .withRecordSeparator(recordSeparator);

        try (CSVPrinter errorPrinter = new CSVPrinter(output, errorFormat)) {

            for (ValidationError error : errors) {
                errorPrinter.print(error.getFlowFieldId());
                errorPrinter.print(error.getFilename());
                errorPrinter.print(error.getColumnName());
                errorPrinter.print(error.getOriginalValue());
                errorPrinter.print(error.getCurrentValue());
                errorPrinter.print(error.getError());
                errorPrinter.print(error.getErrorClass());
                errorPrinter.println();
            }
        }

        return output;
    }

    private List<String> cleanHeaders(Set<String> headers) {
        return headers.stream().map(this::cleanHeader).collect(Collectors.toList());
    }

    private String cleanHeader(String header) {
        header = StringUtils.upperCase(header);
        header = StringUtils.removeStart(header, "/");
        header = StringUtils.replace(header, "/", "_");

        // todo: found this word in one file.  hive doesn't like.  getting rid of it
        if (header.equalsIgnoreCase("rollup")) {
            header = "XROLLUP";
        }
        return header;
    }

    private String formatValue(CSVRecord record, Schema.Field field, String rawHeader, NumberFormat numberFormat, String filename, long flowFieldId, List<ValidationError> errors) {

        Exception exception = null;

        String originalValue = record.get(rawHeader);

        String formattedValue = StringUtils.trimToNull(originalValue);

        if (formattedValue != null && field != null) {

            Schema.Type fieldType = AvroUtils.getFieldType(field);

            final String datePattern = field.getProp("datePattern");

            switch (fieldType) {

                case RECORD:
                case ENUM:
                case ARRAY:
                case MAP:
                case UNION:
                case FIXED:
                case BYTES:
                case BOOLEAN:
                case NULL:
                    exception = new NoSuchMethodException(fieldType.getName() + " is not a currently supported FieldType");
                    break;
                case INT:

                    try {
                        Number number = numberFormat.parse(formattedValue);

                        formattedValue = Integer.toString(number.intValue());
                    } catch (ParseException e) {
                        exception = e;
                        formattedValue = null;
                    }

                    break;
                case LONG:
                    try {
                        Number number = numberFormat.parse(formattedValue);

                        formattedValue = Long.toString(number.longValue());
                    } catch (ParseException e) {
                        exception = e;
                        formattedValue = null;
                    }
                    break;
                case FLOAT:
                    try {
                        Number number = numberFormat.parse(formattedValue);

                        formattedValue = Float.toString(number.floatValue());
                    } catch (ParseException e) {
                        exception = e;
                        formattedValue = null;
                    }

                    break;
                case DOUBLE:
                    try {
                        Number number = numberFormat.parse(formattedValue);

                        formattedValue = Double.toString(number.doubleValue());
                    } catch (ParseException e) {
                        exception = e;
                        formattedValue = null;
                    }
                    break;
                case STRING:
                    if (datePattern != null) {

                        try {
                            final DateTimeFormatter dateFormatter = DateTimeFormat.forPattern(datePattern);

                            LocalDate inputLocalDate = dateFormatter.parseLocalDate(formattedValue);

                            formattedValue = OUTPUT_DATE_FORMAT.print(inputLocalDate);
                        } catch (Exception e) {
                            exception = e;
                            formattedValue = null;
                        }
                    }
                    break;

            }
        }

        if (exception != null) {

            ValidationError error = new ValidationError();
            error.setFlowFieldId(flowFieldId);
            error.setFilename(filename);
            error.setColumnName(rawHeader);
            error.setOriginalValue(originalValue);
            error.setCurrentValue(formattedValue);
            error.setError(exception.getMessage());
            error.setErrorClass(exception.getClass().getName());

            errors.add(error);
        }


        return formattedValue;
    }


    private static Character getCharacter(String string) {
        if (StringUtils.isNotBlank(string)) {
            return string.charAt(0);
        }

        return null;
    }
    private class ValidationError {
        private long flowFieldId;
        private String filename;
        private String columnName;
        private String originalValue;
        private String currentValue;
        private String error;
        private String errorClass;

        long getFlowFieldId() {
            return flowFieldId;
        }

        void setFlowFieldId(long flowFieldId) {
            this.flowFieldId = flowFieldId;
        }

        String getFilename() {
            return filename;
        }

        void setFilename(String filename) {
            this.filename = filename;
        }

        String getColumnName() {
            return columnName;
        }

        void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        String getOriginalValue() {
            return originalValue;
        }

        void setOriginalValue(String originalValue) {
            this.originalValue = originalValue;
        }

        String getError() {
            return error;
        }

        void setError(String error) {
            this.error = error;
        }

        String getCurrentValue() {
            return currentValue;
        }

        void setCurrentValue(String currentValue) {
            this.currentValue = currentValue;
        }

        String getErrorClass() {
            return errorClass;
        }

        void setErrorClass(String errorClass) {
            this.errorClass = errorClass;
        }
    }

}

