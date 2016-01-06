/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kodebeagle.parser;

import org.junit.Before;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public abstract class AbstractParseTest {
    protected String oneMethod;

    @Before
    public void setUp() {
        oneMethod = readInputStream(this.getClass().getResourceAsStream(
                "/OneMethod.java"));
    }

    protected String readInputStream(InputStream is) {
        BufferedReader br = null;
        StringBuffer contents = new StringBuffer();
        try {
            br = new BufferedReader(new java.io.InputStreamReader(is));
            while (br.ready()) {
                contents.append(br.readLine() + "\n");
            }
        } catch (FileNotFoundException e) {
            return "";
        } catch (IOException e) {
            System.err.println("ioexception: " + e);
            return "";
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return contents.toString();

    }
}
