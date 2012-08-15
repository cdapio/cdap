/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.memory;

import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;

public class MemoryOVCTableHandle extends SimpleOVCTableHandle {
  
  @Override
  public OrderedVersionedColumnarTable createNewTable(byte[] tableName) {
    return new MemoryOVCTable(tableName);
  }

  @Override
  public String getName() {
    return "memory";
  }
}
