package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 *
 */
public class LevelDBFuzzyRowFilter implements Filter {
  private FuzzyRowFilter filter;

  public LevelDBFuzzyRowFilter(FuzzyRowFilter f) {
    this.filter = f;
  }

  @Override
  public void reset() {
    filter.reset();
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    return filter.filterRowKey(buffer, offset, length);
  }

  @Override
  public boolean filterAllRemaining() {
    return filter.filterAllRemaining();
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    return filter.filterKeyValue(v);
  }

  @Override
  public KeyValue transform(KeyValue v) {
    return filter.transform(v);
  }

  @Override
  public void filterRow(List<KeyValue> kvs) {
    filter.filterRow();
  }

  @Override
  public boolean hasFilterRow() {
    return filter.hasFilterRow();
  }

  @Override
  public boolean filterRow() {
    return filter.filterRow();
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue currentKV) {
    KeyValue next = filter.getNextKeyHint(currentKV);
    KeyValue.Type type = KeyValue.Type.codeToType(next.getType());
    // using the currentKV's family, qualifier, timestamp, and value, otherwise they will
    // be empty byte arrays which will cause level db's KeyValue implementation to represent
    // the actual key it uses with fewer bytes, which will mess with the seeking.
    KeyValue output = new KeyValue(next.getRow(), currentKV.getFamily(), currentKV.getQualifier(),
                                   currentKV.getTimestamp(), type, currentKV.getValue());
    return output;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    filter.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    filter.readFields(in);
  }
}
