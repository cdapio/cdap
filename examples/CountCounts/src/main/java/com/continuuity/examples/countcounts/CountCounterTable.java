/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.countcounts;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDataset;
import com.continuuity.api.dataset.module.EmbeddedDataset;
import com.continuuity.api.dataset.table.Get;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Table;

import java.util.Map;
import java.util.TreeMap;

/**
 *
 */
public class CountCounterTable extends AbstractDataset {

  private Table table;

  private static final byte[] KEY_ONLY_COLUMN = new byte[]{'c'};

  public CountCounterTable(DatasetSpecification spec, @EmbeddedDataset("cct") Table table) {
    super(spec.getName(), table);
    this.table = table;
  }

  // Word count methods

  private static final byte[] WORD_COUNT_KEY = Bytes.toBytes("word_count");
  private static final byte[] WORD_COUNT_COUNTS_KEY = Bytes.toBytes("count_counts");

  public void incrementWordCount(long count) {
    // Increment the total word count
    increment(WORD_COUNT_KEY, count);
    // Increment the counts count
    table.increment(WORD_COUNT_COUNTS_KEY, Bytes.toBytes(count), 1L);
  }

  public long getTotalWordCount() {
    return get(WORD_COUNT_KEY);
  }

  public Map<Long, Long> getWordCountCounts() {
    Row result = this.table.get(WORD_COUNT_COUNTS_KEY);
    Map<Long, Long> counts = new TreeMap<Long, Long>();

    if (result.isEmpty()) {
      return counts;
    }

    for (Map.Entry<byte[], byte[]> entry : result.getColumns().entrySet()) {
      counts.put(Bytes.toLong(entry.getKey()), Bytes.toLong(entry.getValue()));
    }
    return counts;
  }

  // Line count methods
  private static final byte[] LINE_COUNT_KEY = Bytes.toBytes("line_count");

  public void incrementLineCount() {
    increment(LINE_COUNT_KEY, 1L);
  }

  public long getLineCount() {
    return get(LINE_COUNT_KEY);
  }

  // Line length methods

  private static final byte[] LINE_LENGTH_KEY = Bytes.toBytes("line_length");

  public void incrementLineLength(long length) {
    increment(LINE_LENGTH_KEY, length);
  }

  public long getLineLength() {
    return get(LINE_LENGTH_KEY);
  }

  // Private helpers

  private void increment(byte[] key, long count) {
    table.increment(key, KEY_ONLY_COLUMN, count);
  }

  private long get(byte[] key) {
    return table.get(new Get(key, KEY_ONLY_COLUMN)).getLong(KEY_ONLY_COLUMN, 0);
  }
}
