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
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;


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

        final ByteArrayOutputStream bufferedOut = new ByteArrayOutputStream();
        String dirtyLine = "11/3/16 15:43:12,4,I,ASDF,\"qw\\n ert \\n y126\",ABC,103.40000,tsf4,20.68000";
        formatDelimitedRow.stripNewLines(new BufferedInputStream(new ByteArrayInputStream(dirtyLine.getBytes())),bufferedOut,',',9);

        //bufferedOut.toString(StandardCharsets.UTF_8.name());
        String cleanRow = bufferedOut.toString(StandardCharsets.UTF_8.name());
        String[] splits = cleanRow.split("\n");
        Assert.assertTrue(1 == splits.length);
        dirtyLine = "11/3/16 15:43:14,6,I,ASDF,\"qwerty\\n 128\",ABC\n" +
                ",105.60000,tsf6,\n" +
                "21.12000";

        bufferedOut.reset();
        formatDelimitedRow.stripNewLines(new BufferedInputStream(new ByteArrayInputStream(dirtyLine.getBytes())),bufferedOut,',',9);
        cleanRow = bufferedOut.toString(StandardCharsets.UTF_8.name());
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
        final ByteArrayOutputStream bufferedOut = new ByteArrayOutputStream();
        formatDelimitedRow.stripNewLines(new BufferedInputStream(new ByteArrayInputStream(dirtyLine.getBytes())),bufferedOut,',',9);
        final String cleanRow = bufferedOut.toString(StandardCharsets.UTF_8.name());
        final String[] splits = cleanRow.split("\n");
        Assert.assertTrue(2 == splits.length);
    }

    @Test
    public void TestDanglingLastColumn() throws IOException {
        String input = "2016/11/14,abcde,oxoooo,abc,Anne,29568,82569,abc1,2016/11/14,2016/11/14,abcde,2016/11/14,2016/11/14\n" +
                "2016/11/14,abcde,oxoooo,abc,Anne,80111,52966,abc1,2016/11/14,2016/11/14,abcde,2016/11/14,2016/11/14\n" +
                "2016/11/14,abcde,oxoooo,abc,Anne,3844,62645,abc1,2016/11/14,2016/11/14,abcde,2016/11/1K,\n" +
                "\n" +
                "2016/11/1G\n" +
                "2016/11/14,abcde,oxoooo,abc,Anne,78991,62974,abc1,2016/11/14,2016/11/14,abcde,2016/11/14,2016/11/14\n" +
                "2016/11/14,abcde,oxoooo,abc,Anne,23773,17965,abc1,2016/11/14,2016/11/14,abcde,2016/11/14,2016/11/14\n" +
                "2016/11/14,abcde,oxoooo,abc,Anne,90656,17434,abc1,2016/11/14,2016/11/14,abcde,2016/11/14,2016/11/14";

        EmbeddedNewLineRemover formatDelimitedRow = new EmbeddedNewLineRemover();
        final ByteArrayOutputStream bufferedOut = new ByteArrayOutputStream();
        formatDelimitedRow.stripNewLines(new BufferedInputStream(new ByteArrayInputStream(input.getBytes())),bufferedOut,',',13);
        final String cleanRow = bufferedOut.toString(StandardCharsets.UTF_8.name());
        final String[] splits = cleanRow.split("\n");
        Assert.assertTrue(6 == splits.length);
        //now walk each line and make sure there is text for each of the 13 columns...
    }
}
