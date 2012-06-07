/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.memory;

import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.table.ColumnarTable;
import com.continuuity.data.table.SimpleColumnarTableHandle;
import com.continuuity.data.table.converter.ColumnarOnVersionedColumnarTable;

public class MemoryColumnarTableHandle extends SimpleColumnarTableHandle {
  
  @Override
  public ColumnarTable createNewTable(byte[] tableName,
      TimestampOracle timeOracle) {
    return new ColumnarOnVersionedColumnarTable(
        new MemoryOVCTable(tableName, timeOracle), timeOracle);
  }
}
