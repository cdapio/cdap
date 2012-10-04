package com.continuuity.data.operation;

import com.continuuity.api.data.ConditionalWriteOperation;
import com.continuuity.api.data.OperationBase;

public class ReadModifyWrite implements ConditionalWriteOperation {

  /** Unique id for the operation */
  private final long id = OperationBase.getId();

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
  public long getId() {
    return id;
  }
}
