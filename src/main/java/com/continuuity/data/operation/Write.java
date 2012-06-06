package com.continuuity.data.operation;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.operation.type.WriteOperation;
import com.google.common.base.Objects;

public class Write implements WriteOperation {

  private byte [] key;
  private byte [] value;
 
  public Write(final byte [] key, final byte [] value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public byte [] getKey() {
    return this.key;
  }
 
  public byte [] getValue() {
    return this.value;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("key", Bytes.toString(key))
        .add("value", Bytes.toString(value))
        .toString();
  }

  @Override
  public int getPriority() {
    return 1;
  }
}
