package com.continuuity.data.operation;

public class OrderedWrite implements WriteOperation {
  /** Unique id for the operation */
  private final long id = OperationBase.getId();

  private byte [] key;
  private byte [] value;
 
  public OrderedWrite(final byte [] key, final byte [] value) {
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
  public int getPriority() {
    return 1;
  }

  @Override
  public long getId() {
    return id;
  }

  @Override
  public int getSize() {
    return 0;
  }
}
