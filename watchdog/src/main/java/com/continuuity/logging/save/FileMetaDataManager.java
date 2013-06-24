/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.save;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Scan;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.table.Scanner;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Handles reading/writing of file metadata.
 */
public final class FileMetaDataManager {
  private final OperationExecutor opex;
  private final OperationContext operationContext;
  private final String table;

  private static final byte [] ROW_KEY_PREFIX = Bytes.toBytes(200);
  private static final byte [] ROW_KEY_PREFIX_END = Bytes.toBytes(201);

  public FileMetaDataManager(OperationExecutor opex, OperationContext operationContext, String table) {
    this.opex = opex;
    this.operationContext = operationContext;
    this.table = table;
  }

  public void writeMetaData(LoggingContext loggingContext, long startTime, Path path) throws OperationException {
    Write writeOp = new Write(table,
                              Bytes.add(ROW_KEY_PREFIX, Bytes.toBytes(loggingContext.getLogPartition())),
                              Bytes.toBytes(startTime),
                              Bytes.toBytes(path.toUri().toString())
                              );
    opex.commit(operationContext, writeOp);
  }

  public int cleanMetaData(long tillTime, DeleteCallback callback) throws OperationException {
    byte [] tillTimeBytes = Bytes.toBytes(tillTime);

    Scan scan = new Scan(table, ROW_KEY_PREFIX, ROW_KEY_PREFIX_END);
    Scanner scanner = opex.scan(operationContext, null, scan);

    DeleteExecutor deleteExecutor = new DeleteExecutor();
    ImmutablePair<byte[], Map<byte[], byte[]>> row;
    while ((row = scanner.next()) != null) {
      byte [] rowKey = row.getFirst();

      for (Map.Entry<byte[], byte[]> entry : row.getSecond().entrySet()) {
        byte [] colName = entry.getKey();
        // Delete if colName is less than tillTime
        if (Bytes.compareTo(colName, tillTimeBytes) < 1) {
          callback.handle(new Path(URI.create(Bytes.toString(entry.getValue()))));
          deleteExecutor.delete(new Delete(rowKey, colName));
        }
      }
    }

    // Flush any remaining deletes
    deleteExecutor.flush();
    scanner.close();

    return deleteExecutor.getTotalDeletes();
  }

  /**
   * Implement to receive a path before its meta data is removed.
   */
  public interface DeleteCallback {
    public void handle(Path path);
  }


  /**
   * Executes delete operation in batches.
   */
  private final class DeleteExecutor {
    private static final int MAX_OPS = 100;
    private List<WriteOperation> deleteList = Lists.newArrayListWithExpectedSize(MAX_OPS);
    private int totalDeletes = 0;

    /**
     * Buffers delete operations, and executes them in batches.
     * @param delete delete operation.
     * @throws OperationException
     */
    public void delete(Delete delete) throws OperationException {
      deleteList.add(delete);

      if (deleteList.size() >= MAX_OPS) {
        flush();
      }
    }

    /**
     * Executes buffered delete operations immediately.
     * @throws OperationException
     */
    public void flush() throws OperationException {
      if (!deleteList.isEmpty()) {
        opex.commit(operationContext, deleteList);
        totalDeletes += deleteList.size();
        deleteList.clear();
      }
    }

    /**
     * @return total number of deletes executed. Any buffered deletes are not included in the count.
     */
    public int getTotalDeletes() {
      return totalDeletes;
    }
  }
}
