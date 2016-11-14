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

import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;


public class EmbeddedNewLineRemoverTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(EmbeddedNewLineRemover.class);
    }

    @Test
    public void testProcessor() {

    }

    @Test
    public void TestSingleRowStripNewLines() throws IOException {

        EmbeddedNewLineRemover formatDelimitedRow = new EmbeddedNewLineRemover();

        String dirtyLine = "11/3/16 15:43:12,4,I,ASDF,\"qw\\n ert \\n y126\",ABC,103.40000,tsf4,20.68000";
        String cleanRow = formatDelimitedRow.stripNewLines(new BufferedInputStream(new ByteArrayInputStream(dirtyLine.getBytes())),',',9);
        String[] splits = cleanRow.split("\n");
        Assert.assertTrue(1 == splits.length);
        dirtyLine = "11/3/16 15:43:14,6,I,ASDF,\"qwerty\\n 128\",ABC\n" +
                ",105.60000,tsf6,\n" +
                "21.12000";

        cleanRow = formatDelimitedRow.stripNewLines(new BufferedInputStream(new ByteArrayInputStream(dirtyLine.getBytes())),',',9);
        splits = cleanRow.split("\n");
        Assert.assertTrue(1 == splits.length);
    }

    @Test
    public void TestMultipleRowStripNewLines() throws IOException {

        EmbeddedNewLineRemover formatDelimitedRow = new EmbeddedNewLineRemover();

        String dirtyLine = "11/3/16 15:43:17,10,I,ASDF,qwerty132,ABC\n" +
                ",110.00000,tsf10,22.0000G\n" +
                "\n" +
                "\n" +
                "11/3/16 15:43:18,11,I,ASDF,\n" +
                "\n" +
                "qwerty133,XZY,111.10000,tsf11,22.22000";
        String cleanRow = formatDelimitedRow.stripNewLines(new BufferedInputStream(new ByteArrayInputStream(dirtyLine.getBytes())),',',9);
        final String[] splits = cleanRow.split("\n");
        Assert.assertTrue(2 == splits.length);


    }
}
