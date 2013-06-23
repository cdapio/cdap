/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor;

/**
 * Interface that defines a read pointer by exposing a method to determine
 * whether the specified txid should be visible according to this pointer.
 */
public interface ReadPointer {

  public boolean isVisible(long txid);

  public long getMaximum();
}
