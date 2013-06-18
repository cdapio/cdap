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
public class FileManager {
  private final OperationExecutor opex;
  private final OperationContext operationContext;
  private final String table;

  public FileManager(OperationExecutor opex, OperationContext operationContext, String table) {
    this.opex = opex;
    this.operationContext = operationContext;
    this.table = table;
  }

  public void writeMetaData(LoggingContext loggingContext, long startTime, String fileName) throws OperationException {
    Write writeOp = new Write(table,
                              Bytes.toBytes(loggingContext.getLogPartition()),
                              Bytes.toBytes(startTime),
                              Bytes.toBytes(fileName)
                              );
    opex.commit(operationContext, writeOp);
  }
}
