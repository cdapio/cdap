/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.table.handles;

import com.continuuity.data.engine.memory.MemoryOVCTable;
import com.continuuity.data.table.OrderedVersionedColumnarTable;

public class MemoryOVCTableHandle extends SimpleOVCTableHandle {
  
  @Override
  public OrderedVersionedColumnarTable createNewTable(byte[] tableName) {
    return new MemoryOVCTable(tableName, timeOracle);
  }
}
