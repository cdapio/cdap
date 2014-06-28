package com.continuuity.data2.dataset.lib.table;

/**
*
*/
public class PutValue implements Update<byte[]> {
  private final byte[] bytes;

  public PutValue(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public byte[] getValue() {
    return bytes;
  }

  @Override
  public byte[] getBytes() {
    return bytes;
  }
}
