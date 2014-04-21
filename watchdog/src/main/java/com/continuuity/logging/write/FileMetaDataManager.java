/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.write;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.Callable;

/**
 * Handles reading/writing of file metadata.
 */
public final class FileMetaDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetaDataManager.class);

  private static final byte [] ROW_KEY_PREFIX = Bytes.toBytes(200);
  private static final byte [] ROW_KEY_PREFIX_END = Bytes.toBytes(201);

  private final TransactionExecutor txExecutor;
  private final OrderedColumnarTable metaTable;
  private final LocationFactory locationFactory;

  public FileMetaDataManager(OrderedColumnarTable metaTable, TransactionSystemClient txClient,
                             LocationFactory locationFactory) {
    this.metaTable = metaTable;
    this.txExecutor = new DefaultTransactionExecutor(txClient, ImmutableList.of((TransactionAware) metaTable));
    this.locationFactory = locationFactory;
  }


  /**
   * Persistes meta data associated with a log file.
   * @param loggingContext logging context containing the meta data.
   * @param startTimeMs start log time associated with the file.
   * @param location log file.
   * @throws OperationException
   */
  public void writeMetaData(final LoggingContext loggingContext,
                            final long startTimeMs,
                            final Location location) throws Exception {
    LOG.debug("Writing meta data for logging context {} as startTimeMs {} and location {}",
              loggingContext.getLogPartition(), startTimeMs, location.toURI());

    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        metaTable.put(getRowKey(loggingContext),
                      Bytes.toBytes(startTimeMs),
                      Bytes.toBytes(location.toURI().toString()));
      }
    });
  }

  /**
   * Returns a list of log files for a logging context.
   * @param loggingContext logging context.
   * @return Sorted map containing key as start time, and value as log file.
   * @throws OperationException
   */
  public SortedMap<Long, Location> listFiles(final LoggingContext loggingContext) throws Exception {
    return txExecutor.execute(new Callable<SortedMap<Long, Location>>() {
      @Override
      public SortedMap<Long, Location> call() throws Exception {
        Map<byte[], byte[]> cols = metaTable.get(getRowKey(loggingContext));

        if (cols.isEmpty()) {
          return ImmutableSortedMap.of();
        }

        SortedMap<Long, Location> files = Maps.newTreeMap();
        for (Map.Entry<byte[], byte[]> entry : cols.entrySet()) {
          files.put(Bytes.toLong(entry.getKey()), locationFactory.create(new URI(Bytes.toString(entry.getValue()))));
        }
        return files;
      }
    });
  }

  /**
   * Deletes meta data until a given time, while keeping the latest meta data even if less than tillTime.
   * @param tillTime time till the meta data will be deleted.
   * @param callback callback called before deleting a meta data column.
   * @return total number of columns deleted.
   * @throws OperationException
   */
  public int cleanMetaData(final long tillTime, final DeleteCallback callback) throws Exception {
    return txExecutor.execute(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        byte [] tillTimeBytes = Bytes.toBytes(tillTime);

        int deletedColumns = 0;
        Scanner scanner = metaTable.scan(ROW_KEY_PREFIX, ROW_KEY_PREFIX_END);
        try {
          ImmutablePair<byte[], Map<byte[], byte[]>> row;
          while ((row = scanner.next()) != null) {
            byte [] rowKey = row.getFirst();
            byte [] maxCol = getMaxKey(row.getSecond());

            for (Map.Entry<byte[], byte[]> entry : row.getSecond().entrySet()) {
              byte [] colName = entry.getKey();
              if (LOG.isDebugEnabled()) {
                LOG.debug("Got file {} with start time {}", Bytes.toString(entry.getValue()),
                          Bytes.toLong(colName));
              }
              // Delete if colName is less than tillTime, but don't delete the last one
              if (Bytes.compareTo(colName, tillTimeBytes) < 0 && Bytes.compareTo(colName, maxCol) != 0) {
                callback.handle(locationFactory.create(new URI(Bytes.toString(entry.getValue()))));
                metaTable.delete(rowKey, colName);
                deletedColumns++;
              }
            }
          }
        } finally {
          scanner.close();
        }

        return deletedColumns;
      }
    });
  }

  private byte[] getRowKey(LoggingContext loggingContext) {
    return Bytes.add(ROW_KEY_PREFIX, Bytes.toBytes(loggingContext.getLogPartition()));
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
   * Implement to receive a location before its meta data is removed.
   */
  public interface DeleteCallback {
    public void handle(Location location);
  }
}
