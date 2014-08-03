/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A key value table that tracks how often it is opened and closed, read and written.
 */
public class TrackingTable extends AbstractDataset
  implements BatchReadable<byte[], byte[]>, BatchWritable<byte[], byte[]> {

  // some counters that are incremented by the table's operations and verified by the unit test.
  // the following is ugly. but there is no way to share a static counter or similar between the unit test and the
  // flowlets/procedures/etc, because those are loaded in a different class loader. So we use a global thing like the
  // system properties to count.

  public static synchronized int getTracker(String table, String op) {
    String key = table + "-" + op;
    String value = System.getProperty(key, "0");
    return Integer.valueOf(value);
  }
  private static synchronized void track(String table, String op) {
    String key = table + "-" + op;
    String value = System.getProperty(key, "0");
    String newValue = Integer.toString(Integer.valueOf(value) + 1);
    System.setProperty(key, newValue);
  }
  public static synchronized void resetTracker() {
    for (String table : Arrays.asList("foo", "bar")) {
      for (String op : Arrays.asList("open", "close", "read", "write", "split")) {
        String key = table + "-" + op;
        System.clearProperty(key);
      }
    }
  }

  private final KeyValueTable t;

  public TrackingTable(DatasetSpecification spec, @EmbeddedDataset("") KeyValueTable t) {
    super(spec.getName(), t);
    this.t = t;
    track(spec.getName(), "open");
  }

  @Override
  public void close() {
    track(getName(), "close");
  }

  @Nullable
  public byte[] read(byte[] key) {
    track(getName(), "read");
    return t.read(key);
  }

  @Override
  public void write(byte[] key, byte[] value) {
    track(getName(), "write");
    t.write(key, value);
  }

  @Override
  public SplitReader<byte[], byte[]> createSplitReader(Split split) {
    return t.createSplitReader(split);
  }

  @Override
  public List<Split> getSplits() {
    track(getName(), "split");
    return t.getSplits(1, null, null); // return a single split for testing
  }
}
