package com.continuuity.data.operation;

public class ReadModifyWrite extends ConditionalWriteOperation {

  private final byte [] key;
  private final Modifier<byte[]> modifier;

  public ReadModifyWrite(final byte [] key, Modifier<byte[]> modifier) {
    this.key = key;
    this.modifier = modifier;
  }

  @Override
  public byte [] getKey() {
    return key;
  }

  public Modifier<byte[]> getModifier() {
    return modifier;
  }

  @Override
  public int getPriority() {
    return 1;
  }

  @Override
  public int getSize() {
    return 0;
  }
}
