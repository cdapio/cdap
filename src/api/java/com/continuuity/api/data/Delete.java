package com.continuuity.api.data;


public class Delete implements WriteOperation {

  private final byte [] key;

  public Delete(final byte [] key) {
    this.key = key;
  }

  @Override
  public byte [] getKey() {
    return this.key;
  }

  @Override
  public int getPriority() {
    return 1;
  }
}
