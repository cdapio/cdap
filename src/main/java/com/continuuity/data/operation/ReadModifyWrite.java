package com.continuuity.data.operation;

import com.continuuity.data.operation.type.ConditionalWriteOperation;

public class ReadModifyWrite implements ConditionalWriteOperation {

  private final byte [] key;
  private final Modifier<byte[]> modifier;

  public ReadModifyWrite(byte [] key, Modifier<byte[]> modifier) {
    this.key = key;
    this.modifier = modifier;
  }

  public byte [] getKey() {
    return key;
  }

  public Modifier<byte[]> getModifier() {
    return modifier;
  }

}
