package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.data2.dataset2.lib.AbstractDataset;
import com.continuuity.internal.data.dataset.lib.table.Row;
import com.continuuity.internal.data.dataset.lib.table.Table;

import java.util.List;

/**
 * This class implements a key/value map on top of {@link Table}. Supported
 * operations are read and write.
 */
public class KeyValueTable extends AbstractDataset implements BatchReadable<byte[], byte[]> {
  private static final byte[] COL = new byte[0];

  private final Table table;

  public KeyValueTable(String instanceName, Table table) {
    super(instanceName, table);
    this.table = table;
  }

  public void write(byte[] key, byte[] value) throws Exception {
    table.put(key, COL, value);
  }

  public void write(String key, String value) throws Exception {
    table.put(Bytes.toBytes(key), COL, Bytes.toBytes(value));
  }

  public byte[] read(byte[] key) throws Exception {
    return table.get(key, COL);
  }

  public String read(String key) throws Exception {
    byte[] value = table.get(Bytes.toBytes(key), COL);
    return value == null ? null : Bytes.toString(value);
  }

  @Override
  public List<Split> getSplits() {
    return table.getSplits();
  }

  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    return table.getSplits(numSplits, start, stop);
  }

  @Override
  public SplitReader<byte[], byte[]> createSplitReader(Split split) {
    return new KeyValueScanner(table.createSplitReader(split));
  }

  /**
   * The split reader for KeyValueTable.
   */
  public class KeyValueScanner extends SplitReader<byte[], byte[]> {

    // the underlying KeyValueTable's split reader
    private SplitReader<byte[], Row> reader;

    public KeyValueScanner(SplitReader<byte[], Row> reader) {
      this.reader = reader;
    }

    @Override
    public void initialize(Split split) throws InterruptedException {
      this.reader.initialize(split);
    }

    @Override
    public boolean nextKeyValue() throws InterruptedException {
      return this.reader.nextKeyValue();
    }

    @Override
    public byte[] getCurrentKey() throws InterruptedException {
      return this.reader.getCurrentKey();
    }

    @Override
    public byte[] getCurrentValue() throws InterruptedException {
      return this.reader.getCurrentValue().get(COL);
    }

    @Override
    public void close() {
      this.reader.close();
    }
  }
}
