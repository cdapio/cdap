package com.continuuity;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.KeyValueTable;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A key value table that tracks how often it is opened and closed, read and written.
 */
public class TrackingTable extends DataSet implements BatchReadable<byte[], byte[]>, BatchWritable<byte[], byte[]> {

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

  KeyValueTable t;

  public TrackingTable(String name) {
    super(name);
    t = new KeyValueTable(name);
  }

  @Override
  public void initialize(DataSetSpecification spec, DataSetContext context) {
    super.initialize(spec, context);
    track(getName(), "open");
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
