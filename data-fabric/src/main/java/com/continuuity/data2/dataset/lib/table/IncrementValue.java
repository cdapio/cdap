package com.continuuity.data2.dataset.lib.table;

import com.continuuity.api.common.Bytes;

/**
*
*/
public class IncrementValue implements Update<Long> {
  private final Long value;

  public IncrementValue(Long value) {
    this.value = value;
  }

  @Override
  public Long getValue() {
    return value;
  }

  @Override
  public byte[] getBytes() {
    return Bytes.toBytes(value);
  }
}
