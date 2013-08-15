/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.save;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.Scan;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.table.Scanner;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * Handles reading/writing of file metadata.
 */
public final class FileMetaDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetaDataManager.class);

  private static final byte [] ROW_KEY_PREFIX = Bytes.toBytes(200);
  private static final byte [] ROW_KEY_PREFIX_END = Bytes.toBytes(201);

  private final OperationExecutor opex;
  private final OperationContext operationContext;
  private final String table;

  public FileMetaDataManager(OperationExecutor opex, OperationContext operationContext, String table) {
    this.opex = opex;
    this.operationContext = operationContext;
    this.table = table;
  }

  /**
   * Persistes meta data associated with a log file.
   * @param loggingContext logging context containing the meta data.
   * @param startTimeMs start log time associated with the file.
   * @param path log file.
   * @throws OperationException
   */
  public void writeMetaData(LoggingContext loggingContext, long startTimeMs, Path path) throws OperationException {
    LOG.debug("Writing meta data for logging context {} as startTimeMs {} and path {}",
              loggingContext.getLogPartition(), startTimeMs, path);

    Write writeOp = new Write(table,
                              Bytes.add(ROW_KEY_PREFIX, Bytes.toBytes(loggingContext.getLogPartition())),
                              Bytes.toBytes(startTimeMs),
                              Bytes.toBytes(path.toUri().toString())
                              );
    opex.commit(operationContext, writeOp);
  }

  /**
   * Returns a list of log files for a logging context.
   * @param loggingContext logging context.
   * @return Sorted map containing key as start time, and value as log file.
   * @throws OperationException
   */
  public SortedMap<Long, Path> listFiles(LoggingContext loggingContext)
    throws OperationException {
    OperationResult<Map<byte[], byte[]>> cols =
      opex.execute(operationContext,
                   new ReadColumnRange(table,
                     Bytes.add(ROW_KEY_PREFIX, Bytes.toBytes(loggingContext.getLogPartition())),
                                       null, null));

    if (cols.isEmpty() || cols.getValue() == null) {
      return ImmutableSortedMap.of();
    }

    SortedMap<Long, Path> files = Maps.newTreeMap();
    for (Map.Entry<byte[], byte[]> entry : cols.getValue().entrySet()) {
      files.put(Bytes.toLong(entry.getKey()), new Path(Bytes.toString(entry.getValue())));
    }
    return files;
  }

  /**
   * Deletes meta data until a given time, while keeping the latest meta data even if less than tillTime.
   * @param tillTime time till the meta data will be deleted.
   * @param callback callback called before deleting a meta data column.
   * @return total number of columns deleted.
   * @throws OperationException
   */
  public int cleanMetaData(long tillTime, DeleteCallback callback) throws OperationException {
    byte [] tillTimeBytes = Bytes.toBytes(tillTime);

    Scan scan = new Scan(table, ROW_KEY_PREFIX, ROW_KEY_PREFIX_END);
    Scanner scanner = opex.scan(operationContext, null, scan);

    DeleteExecutor deleteExecutor = new DeleteExecutor();
    ImmutablePair<byte[], Map<byte[], byte[]>> row;
    while ((row = scanner.next()) != null) {
      byte [] rowKey = row.getFirst();
      byte [] maxCol = getMaxKey(row.getSecond());

      for (Map.Entry<byte[], byte[]> entry : row.getSecond().entrySet()) {
        byte [] colName = entry.getKey();
        // Delete if colName is less than tillTime, but don't delete the last one
        if (Bytes.compareTo(colName, tillTimeBytes) < 0 && Bytes.compareTo(colName, maxCol) != 0) {
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

  private byte [] getMaxKey(Map<byte[], byte[]> map) {
    if (map instanceof SortedMap) {
      return ((SortedMap<byte [], byte []>) map).lastKey();
    }

    byte [] max = Bytes.EMPTY_BYTE_ARRAY;
    for (byte [] elem : map.keySet()) {
      if (Bytes.compareTo(max, elem) < 0) {
        max = elem;
      }
    }
    return max;
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
