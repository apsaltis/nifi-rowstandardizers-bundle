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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class DedupeRowsTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(DedupeRows.class);
    }

    @Test
    public void testProcessor() {

    }

    @Test
    public void TestDateDupesLastWins() throws IOException {

        DedupeRows dedupeRows = new DedupeRows();

        final ByteArrayOutputStream bufferedOut = new ByteArrayOutputStream();
        String duplicateLines = "2016-10-12 03:20:52,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer\n" +
                "2016-10-12 03:20:53,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer\n" +
                "2016-10-12 03:20:54,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer";

        LongArrayList discardList = dedupeRows.buildIndex(new BufferedInputStream(new ByteArrayInputStream(duplicateLines.getBytes())),
              //  "\n",
                ',',
                '"',
                0,
                'd',
                "yyyy-MM-dd' 'HH:mm:ss",
                '>',
                new int[]{4, 5, 6, 7, 8});


        dedupeRows.writeOutput(new ByteArrayInputStream(duplicateLines.getBytes()),bufferedOut,System.lineSeparator(),discardList);

        //bufferedOut.toString(StandardCharsets.UTF_8.name());
        String cleanRow = bufferedOut.toString(StandardCharsets.UTF_8.name());
        String[] rows = cleanRow.split("\n");
        Assert.assertTrue(1 == rows.length);
        Assert.assertTrue(rows[0].contentEquals("2016-10-12 03:20:54,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer"));

    }
    @Test
    public void TestDateDupesFirstWins() throws IOException {

        DedupeRows dedupeRows = new DedupeRows();

        final ByteArrayOutputStream bufferedOut = new ByteArrayOutputStream();
        String duplicateLines = "2016-10-12 03:20:52,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer\n" +
                "2016-10-12 03:20:53,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer\n" +
                "2016-10-12 03:20:54,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer";

        LongArrayList discardList = dedupeRows.buildIndex(new BufferedInputStream(new ByteArrayInputStream(duplicateLines.getBytes())),
             //   "\n",
                ',',
                '"',
                0,
                'd',
                "yyyy-MM-dd' 'HH:mm:ss",
                '<',
                new int[]{4, 5, 6, 7, 8});


        dedupeRows.writeOutput(new ByteArrayInputStream(duplicateLines.getBytes()),bufferedOut,System.lineSeparator(),discardList);

        //bufferedOut.toString(StandardCharsets.UTF_8.name());
        String cleanRow = bufferedOut.toString(StandardCharsets.UTF_8.name());
        String[] rows = cleanRow.split("\n");
        Assert.assertTrue(1 == rows.length);
        Assert.assertTrue(rows[0].contentEquals("2016-10-12 03:20:52,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer"));

    }

    @Test
    public void TestNumberDupesFirstWins() throws IOException {

        DedupeRows dedupeRows = new DedupeRows();

        final ByteArrayOutputStream bufferedOut = new ByteArrayOutputStream();
        String duplicateLines = "1,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer\n" +
                "2,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer\n" +
                "3,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer";

        LongArrayList discardList = dedupeRows.buildIndex(new BufferedInputStream(new ByteArrayInputStream(duplicateLines.getBytes())),
               // "\n",
                ',',
                '"',
                0,
                'n',
                null,
                '<',
                new int[]{4, 5, 6, 7, 8});


        dedupeRows.writeOutput(new ByteArrayInputStream(duplicateLines.getBytes()),bufferedOut,System.lineSeparator(),discardList);

        //bufferedOut.toString(StandardCharsets.UTF_8.name());
        String cleanRow = bufferedOut.toString(StandardCharsets.UTF_8.name());
        String[] rows = cleanRow.split("\n");
        Assert.assertTrue(1 == rows.length);
        Assert.assertTrue(rows[0].contentEquals("1,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer"));

    }

    @Test
    public void TestNumberDupesLastWins() throws IOException {

        DedupeRows dedupeRows = new DedupeRows();

        final ByteArrayOutputStream bufferedOut = new ByteArrayOutputStream();
        String duplicateLines = "1,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer\n" +
                "2,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer\n" +
                "3,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer";

        LongArrayList discardList = dedupeRows.buildIndex(new BufferedInputStream(new ByteArrayInputStream(duplicateLines.getBytes())),
                //"\n",
                ',',
                '"',
                0,
                'n',
                null,
                '>',
                new int[]{4, 5, 6, 7, 8});


        dedupeRows.writeOutput(new ByteArrayInputStream(duplicateLines.getBytes()),bufferedOut,System.lineSeparator(),discardList);

        //bufferedOut.toString(StandardCharsets.UTF_8.name());
        String cleanRow = bufferedOut.toString(StandardCharsets.UTF_8.name());
        String[] rows = cleanRow.split("\n");
        Assert.assertTrue(1 == rows.length);
        Assert.assertTrue(rows[0].contentEquals("3,0,I,NOT SET,OB50200610010006,999,0.000,2008-02-22-00.32.34.556184,rewwer"));

    }
    @Test
    public void TestNumberDupes25Lines() throws IOException {

        DedupeRows dedupeRows = new DedupeRows();

        final ByteArrayOutputStream bufferedOut = new ByteArrayOutputStream();
        final String lineDelimiter = "\n";
        LongArrayList discardList = dedupeRows.buildIndex(DedupeRowsTest.class.getClassLoader().getResourceAsStream("100LinesWithComma"),
           //     lineDelimiter,
                ',',
                '"',
                0,
                'n',
                null,
                '>',
                new int[]{4, 5, 6, 7, 8});


        dedupeRows.writeOutput(DedupeRowsTest.class.getClassLoader().getResourceAsStream("100LinesWithComma"),bufferedOut,System.lineSeparator(),discardList);


        String cleanRow = bufferedOut.toString(StandardCharsets.UTF_8.name());

        String[] rows = cleanRow.split(lineDelimiter);
        Assert.assertTrue(25 == rows.length);
    }
    @Test
    public void TestNumberSame25Lines() throws IOException {

        DedupeRows dedupeRows = new DedupeRows();

        final ByteArrayOutputStream bufferedOut = new ByteArrayOutputStream();
        final String lineDelimiter = "\n";
        LongArrayList discardList = dedupeRows.buildIndex(DedupeRowsTest.class.getClassLoader().getResourceAsStream("100LinesTabEqual"),
              //  lineDelimiter,
                '\t',
                '"',
                0,
                'n',
                null,
                '=',
                new int[]{4, 5, 6, 7, 8});


        dedupeRows.writeOutput(DedupeRowsTest.class.getClassLoader().getResourceAsStream("100LinesTabEqual"),bufferedOut,System.lineSeparator(),discardList);


        String cleanRow = bufferedOut.toString(StandardCharsets.UTF_8.name());

        String[] rows = cleanRow.split(lineDelimiter);
        Assert.assertTrue(25 == rows.length);
    }

}
