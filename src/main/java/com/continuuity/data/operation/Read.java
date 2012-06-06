package com.continuuity.data.operation;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.operation.type.ReadOperation;
import com.google.common.base.Objects;

public class Read implements ReadOperation<byte[]> {

  private final byte [] key;

  public Read(final byte [] key) {
    this.key = key;
  }

  public byte [] getKey() {
    return this.key;
  }

  @Override
  public byte [] getResult() {
    return null;
  }

  @Override
  public void setResult(byte[] t) {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("key", Bytes.toString(key))
        .toString();
  }
}
