/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.save;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.executor.OperationExecutor;

/**
 * Handles reading/writing of file metadata.
 */
public final class FileManager {
  private final OperationExecutor opex;
  private final OperationContext operationContext;
  private final String table;

  private static final byte [] ROW_KEY_PREFIX = Bytes.toBytes(200);

  public FileManager(OperationExecutor opex, OperationContext operationContext, String table) {
    this.opex = opex;
    this.operationContext = operationContext;
    this.table = table;
  }

  public void writeMetaData(LoggingContext loggingContext, long startTime, String fileName) throws OperationException {
    Write writeOp = new Write(table,
                              Bytes.add(ROW_KEY_PREFIX, Bytes.toBytes(loggingContext.getLogPartition())),
                              Bytes.toBytes(startTime),
                              Bytes.toBytes(fileName)
                              );
    opex.commit(operationContext, writeOp);
  }
}
