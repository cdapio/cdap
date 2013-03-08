package com.continuuity.data.engine.memory;

import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 */
public class Row implements Comparable<Row> {
  final byte[] value;

  public Row(final byte[] value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    return (o instanceof Row) && Bytes.equals(this.value, ((Row) o).value);
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(this.value);
  }

  @Override
  public int compareTo(Row r) {
    return Bytes.compareTo(this.value, r.value);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("rowkey", Bytes.toString(this.value)).toString();
  }
}
