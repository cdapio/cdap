/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.memory;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;

/**
 * This is an in-memory implementation of the table handle.
 */
public class MemoryOVCTableHandle extends SimpleOVCTableHandle {
  /**
   * This class is a singleton.
   * All clients should use the same instance so that they work with the the same data.
   */
  private static final MemoryOVCTableHandle INSTANCE = new MemoryOVCTableHandle();

  public static MemoryOVCTableHandle getInstance() {
    return INSTANCE;
  }

  private MemoryOVCTableHandle() {}

  @Override
  protected OrderedVersionedColumnarTable createNewTable(byte[] tableName) {
    return new MemoryOVCTable(tableName);
  }

  @Override
  protected OrderedVersionedColumnarTable openTable(byte[] tableName)
      throws OperationException {
    // the in-memory implementation does not support opening an existing table
    // whoever creates a tables needs to hold on to it as long as needed.
    return null;
  }

  @Override
  public String getName() {
    return "memory";
  }
}
