/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.continuuity.examples.countcounts;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.dataset.table.Get;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.metrics.Metrics;

import java.util.Map;
import java.util.TreeMap;

/**
 *
 */
public class CountCounterTable extends DataSet {

  private Table table;

  private static final byte[] KEY_ONLY_COLUMN = new byte[]{'c'};

  public CountCounterTable(String name) {
    super(name);
    this.table = new Table("cct_" + getName());
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
