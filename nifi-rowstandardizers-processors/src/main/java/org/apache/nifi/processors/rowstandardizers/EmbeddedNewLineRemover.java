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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"text formatting standardization"})
@CapabilityDescription("Removes embedded newlines from a delimited row in a file")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
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

    private static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure relationship")
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

        final StopWatch stopWatch = new StopWatch(true);
        final char inputDelimiter = context.getProperty(INPUT_DELIMITER).getValue().charAt(0);
        final int numColumns = context.getProperty(NUM_COLUMNS).evaluateAttributeExpressions().asInteger();

        try {
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream rawOut) throws IOException {

                    try (final OutputStream bufferedOut = new BufferedOutputStream(rawOut, 65536);
                         final InputStream bufferedIn = new BufferedInputStream(rawIn, 65536)) {
                        stripNewLines(bufferedIn, bufferedOut, inputDelimiter,numColumns);
                        bufferedOut.flush();
                    }
                }
            });
            stopWatch.stop();
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getDuration(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, SUCCESS);
        } catch (final ProcessException e) {
            session.transfer(flowFile, REL_FAILURE);
            throw e;
        }
    }



    protected void stripNewLines(final InputStream bufferedIn, final OutputStream bufferedOut, final char delimiter, int numColumns) throws IOException {

        int currentColumn = 1;
        int lastNewLinePosition = 0;
        int currentChar = bufferedIn.read();

        //assumes a column is not more than 1K in size, if so we may need to double it
        int[] currentColumnBuffer = new int[2048];
        int currentColumnPos = 0;
        while (-1 != currentChar) {
            if (currentColumn < numColumns) {
                if ((char) currentChar == delimiter) {
                    currentColumn++;
                    //need to drain the currentColumn buffer and ignore all \n that we may see.
                    for(int i = 0; i < currentColumnPos; i++) {
                        if (currentColumnBuffer[i] != 10 && currentColumnBuffer[i] != 13) {
                            bufferedOut.write((char) currentColumnBuffer[i]);
                        }
                    }
                    bufferedOut.write((char) currentChar);
                    currentColumnPos = 0;
                } else {
                    currentColumnBuffer[currentColumnPos] = currentChar;
                    currentColumnPos++;
                }

            } else {
                //we are at the last column and need to keep processing till we hit a delimiter, at which time we
                //need to write out the buffer, only including the last \n and the first column, then we need to mark
                //the currentColumn = 2
                if ((char) currentChar == delimiter) {

                    //need to drain the currentColumn buffer and ignore all \n that we may see.
                    for(int i = 0; i < currentColumnPos; i++) {
                        if (currentColumnBuffer[i] != 10 && currentColumnBuffer[i] != 13) {
                            bufferedOut.write((char) currentColumnBuffer[i]);
                        }else{
                            if(i == lastNewLinePosition){
                                bufferedOut.write((char) currentColumnBuffer[i]);
                                currentColumn = 2;
                                lastNewLinePosition = 0;
                            }
                        }
                    }
                    bufferedOut.write((char) currentChar);
                    currentColumnPos = 0;
                } else {
                    currentColumnBuffer[currentColumnPos] = currentChar;
                    if (currentChar == 10 || currentChar == 13) {
                        lastNewLinePosition = currentColumnPos;
                    }

                    currentColumnPos++;
                }
            }

            currentChar = bufferedIn.read();
        }
        //need to drain any data left in the buffer as we may have been reading the last column.
        for(int i = 0; i < currentColumnPos; i++) {
            if (currentColumnBuffer[i] != 10 && currentColumnBuffer[i] != 13) {
                bufferedOut.write((char) currentColumnBuffer[i]);
            }
        }

    }
}
