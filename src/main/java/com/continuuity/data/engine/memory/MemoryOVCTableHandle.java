/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.memory;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;

public class MemoryOVCTableHandle extends SimpleOVCTableHandle {
  
  @Override
  public OrderedVersionedColumnarTable createNewTable(byte[] tableName) {
    return new MemoryOVCTable(tableName);
  }

  @Override
  public OrderedVersionedColumnarTable openTable(byte[] tableName)
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
