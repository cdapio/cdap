/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


package io.cdap.cdap.runtime.spi.common;

import com.google.common.base.Splitter;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Parse String values from KeyValue Widgets into individual Key Value entries
 */

public class KeyValueListParser {

    private final Pattern pairDelimiter;
    private final Pattern keyValDelimiter;
    public static final KeyValueListParser DEFAULT = new KeyValueListParser(",", ":");

    public KeyValueListParser(String pairDelimiterRegex, String keyValDelimiterRegex) {
        this.pairDelimiter = Pattern.compile(pairDelimiterRegex);
        this.keyValDelimiter = Pattern.compile(keyValDelimiterRegex);
    }

    public Iterable<KeyValue<String, String>> parse(String kvList) {
        return new KeyValueListParser.KeyValueIterable(kvList);
    }

    private class KeyValueIterator implements Iterator<KeyValue<String, String>> {
        private final Iterator<String> pairIter;

        private KeyValueIterator(String kvList) {
            this.pairIter = Splitter.on(KeyValueListParser.this.pairDelimiter).trimResults().split(kvList).iterator();
        }

        public boolean hasNext() {
            return this.pairIter.hasNext();
        }

        public KeyValue<String, String> next() {
            String pair = (String) this.pairIter.next();
            Iterator<String> keyValIter = Splitter.on(KeyValueListParser.this.keyValDelimiter)
                    .trimResults().split(pair).iterator();
            String key = (String) keyValIter.next();
            if (!keyValIter.hasNext()) {
                throw new IllegalArgumentException(String.format("Invalid syntax for key-value pair in list: %s. " +
                        "It is expected to be a string separated by exactly one %s",
                        pair, KeyValueListParser.this.keyValDelimiter));
            } else {
                String val = (String) keyValIter.next();
                if (keyValIter.hasNext()) {
                    throw new IllegalArgumentException(String.format("Invalid syntax for key-value pair in list: %s. " +
                            "It is expected to be a string separated by exactly one %s",
                            pair, KeyValueListParser.this.keyValDelimiter));
                } else {
                    return new KeyValue(key, val);
                }
            }
        }

        public void remove() {
        }
    }

    private class KeyValueIterable implements Iterable<KeyValue<String, String>> {
        private final String kvList;

        private KeyValueIterable(String kvList) {
            this.kvList = kvList;
        }

        public Iterator<KeyValue<String, String>> iterator() {
            return KeyValueListParser.this.new KeyValueIterator(this.kvList);
        }
    }
















}
