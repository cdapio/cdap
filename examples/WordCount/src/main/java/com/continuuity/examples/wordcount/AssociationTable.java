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
package com.continuuity.examples.wordcount;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDataset;
import com.continuuity.api.dataset.module.EmbeddedDataset;
import com.continuuity.api.dataset.table.Get;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Table;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 *
 */
public class AssociationTable extends AbstractDataset {

  private Table table;

  public AssociationTable(DatasetSpecification spec, @EmbeddedDataset("word_assoc") Table table) {
    super(spec.getName(), table);
    this.table = table;
  }

  /**
   * Stores associations between the specified set of words. That is, for every
   * word in the set, an association will be stored for each of the other words
   * in the set.
   * @param words words to store associations between
   */
  public void writeWordAssocs(Set<String> words) {

    // For sets of less than 2 words, there are no associations
    int n = words.size();

    if (n < 2) {
      return;
    }

    // Every word will get (n-1) increments (one for each of the other words)
    long[] values = new long[n - 1];
    Arrays.fill(values, 1);

    // Convert all words to bytes
    byte[][] wordBytes = new byte[n][];
    int i = 0;
    for (String word : words) {
      wordBytes[i++] = Bytes.toBytes(word);
    }

    // Generate an increment for each word
    for (int j = 0; j < n; j++) {
      byte[] row =  wordBytes[j];
      byte[][] columns = new byte[n - 1][];
      System.arraycopy(wordBytes, 0, columns, 0, j);
      System.arraycopy(wordBytes, j + 1, columns, j, n - j - 1);
      this.table.increment(row, columns, values);
    }
  }

  /**
   * Returns the top words associated with the specified word and the number
   * of times the words have appeared together.
   * @param word the word of interest
   * @param limit the number of associations to return, at most
   * @return a map of the top associated words to their co-occurrence count
   */
  public Map<String, Long> readWordAssocs(String word, int limit) {

    // Retrieve all columns of the word’s row
    Row result = this.table.get(new Get(word));
    TopKCollector collector = new TopKCollector(limit);
    if (!result.isEmpty()) {
      
      // Iterate over all columns
      for (Map.Entry<byte[], byte[]> entry : result.getColumns().entrySet()) {
        collector.add(Bytes.toLong(entry.getValue()),
                      Bytes.toString(entry.getKey()));
      }
    }
    return collector.getTopK();
  }

  /**
   * Returns how many times two words occured together.
   * @param word1 the first word
   * @param word2 the other word
   * @return how many times word1 and word2 occurred together
   */
  public long getAssoc(String word1, String word2) {
    Long val = table.get(new Get(word1, word2)).getLong(word2);
    return val == null ? 0 : val;
  }
}

class TopKCollector {

  class Entry implements Comparable<Entry> {
    final long count;
    final String word;

    Entry(long count, String word) {
      this.count = count;
      this.word = word;
    }

    @Override
    public int compareTo(Entry other) {
      if (count == other.count) {
        return word.compareTo(other.word);
      }
      return Long.signum(count - other.count);
    }
  }

  final int limit;
  TreeSet<Entry> entries = new TreeSet<Entry>();

  TopKCollector(int limit) {
    this.limit = limit;
  }

  void add(long count, String word) {
    if (entries.size() < limit) {
      entries.add(new Entry(count, word));
    } else {
      if (entries.first().count < count) {
        entries.pollFirst();
        entries.add(new Entry(count, word));
      }
    }
  }

  Map<String, Long> getTopK() {
    TreeMap<String, Long> topK = new TreeMap<String, Long>();
    for (int i = 0; i < limit; i++) {
      Entry entry = entries.pollLast();
      if (entry == null) {
        break;
      } else {
        topK.put(entry.word, entry.count);
      }
    }
    return topK;
  }
}

