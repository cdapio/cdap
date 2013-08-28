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
  private Filter filter;
  private MetricsEntityCodec codec;

  public LevelDBFuzzyRowFilter(Filter f) {
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
    byte[] nextRow = next.getRow();
    //System.out.println("----- propose skip to row -----");
    //printRow(nextRow);
    return next;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    filter.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    filter.readFields(in);
  }

  public void printRow(byte[] row) {
    int offset = 0;
    String context = codec.decode(MetricsEntityType.CONTEXT, row, offset);
    System.out.println("-- context = " + context);
    offset += codec.getEncodedSize(MetricsEntityType.CONTEXT);
    String metric = codec.decode(MetricsEntityType.METRIC, row, offset);
    System.out.println("-- metric = " + metric);
    offset += codec.getEncodedSize(MetricsEntityType.METRIC);
    String tag = codec.decode(MetricsEntityType.TAG, row, offset);
    System.out.println("-- tag = " + tag);
    offset += codec.getEncodedSize(MetricsEntityType.TAG);
    int timeBase = Bytes.toInt(row, offset, 4);
    System.out.println("-- timebase = " + timeBase);
    offset += 4;
    System.out.println();
  }

  public void setCodec(MetricsEntityCodec codec) {
    this.codec = codec;
  }
}
