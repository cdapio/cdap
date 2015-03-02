/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.logging.write;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.data2.dataset2.tx.DatasetContext;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.logging.save.LogSaverTableUtil;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.SortedMap;

/**
 * Handles reading/writing of file metadata.
 */
public final class FileMetaDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetaDataManager.class);

  public static final byte [] ROW_KEY_PREFIX = Bytes.toBytes(200);
  private static final byte [] ROW_KEY_PREFIX_END = Bytes.toBytes(201);

  private final LocationFactory locationFactory;

  private final Transactional<DatasetContext<Table>, Table> mds;

  public FileMetaDataManager(final LogSaverTableUtil tableUtil, final TransactionSystemClient txClient,
                             LocationFactory locationFactory) {
    this.mds = Transactional.of(
      new TransactionExecutorFactory() {
        @Override
        public TransactionExecutor createExecutor(Iterable<TransactionAware> txAwares) {
          return new DefaultTransactionExecutor(txClient, txAwares);
        }
      },
      new Supplier<DatasetContext<Table>>() {
        @Override
        public DatasetContext<Table> get() {
          try {
            return DatasetContext.of(tableUtil.getMetaTable());
          } catch (Exception e) {
            // there's nothing much we can do here
            throw Throwables.propagate(e);
          }
        }
      });
    this.locationFactory = locationFactory;
  }

  /**
   * Persistes meta data associated with a log file.
   * @param loggingContext logging context containing the meta data.
   * @param startTimeMs start log time associated with the file.
   * @param location log file.
   */
  public void writeMetaData(final LoggingContext loggingContext,
                            final long startTimeMs,
                            final Location location) throws Exception {
    writeMetaData(loggingContext.getLogPartition(), startTimeMs, location);
  }

  /**
   * Persists meta data associated with a log file.
   * @param logPartition partition name that is used to group log messages
   * @param startTimeMs start log time associated with the file.
   * @param location log file.
   */
  public void writeMetaData(final String logPartition,
                            final long startTimeMs,
                            final Location location) throws Exception {
    LOG.debug("Writing meta data for logging context {} as startTimeMs {} and location {}",
              logPartition, startTimeMs, location.toURI());

    mds.execute(new TransactionExecutor.Function<DatasetContext<Table>, Void>() {
      @Override
      public Void apply(DatasetContext<Table> ctx) throws Exception {
        ctx.get().put(getRowKey(logPartition),
                      Bytes.toBytes(startTimeMs),
                      Bytes.toBytes(location.toURI().toString()));
        return null;
      }
    });
  }

  /**
   * Returns a list of log files for a logging context.
   * @param loggingContext logging context.
   * @return Sorted map containing key as start time, and value as log file.
   */
  public SortedMap<Long, Location> listFiles(final LoggingContext loggingContext) throws Exception {
    return mds.execute(new TransactionExecutor.Function<DatasetContext<Table>, SortedMap<Long, Location>>() {
      @Override
      public SortedMap<Long, Location> apply(DatasetContext<Table> ctx) throws Exception {
        Row cols = ctx.get().get(getRowKey(loggingContext));

        if (cols.isEmpty()) {
          return ImmutableSortedMap.of();
        }

        SortedMap<Long, Location> files = Maps.newTreeMap();
        for (Map.Entry<byte[], byte[]> entry : cols.getColumns().entrySet()) {
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
   */
  public int cleanMetaData(final long tillTime, final DeleteCallback callback) throws Exception {
    return mds.execute(new TransactionExecutor.Function<DatasetContext<Table>, Integer>() {
      @Override
      public Integer apply(DatasetContext<Table> ctx) throws Exception {
        byte [] tillTimeBytes = Bytes.toBytes(tillTime);

        int deletedColumns = 0;
        Scanner scanner = ctx.get().scan(ROW_KEY_PREFIX, ROW_KEY_PREFIX_END);
        try {
          Row row;
          while ((row = scanner.next()) != null) {
            byte [] rowKey = row.getRow();
            byte [] maxCol = getMaxKey(row.getColumns());

            for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
              byte [] colName = entry.getKey();
              if (LOG.isDebugEnabled()) {
                LOG.debug("Got file {} with start time {}", Bytes.toString(entry.getValue()),
                          Bytes.toLong(colName));
              }
              // Delete if colName is less than tillTime, but don't delete the last one
              if (Bytes.compareTo(colName, tillTimeBytes) < 0 && Bytes.compareTo(colName, maxCol) != 0) {
                callback.handle(locationFactory.create(new URI(Bytes.toString(entry.getValue()))));
                ctx.get().delete(rowKey, colName);
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
    return getRowKey(loggingContext.getLogPartition());
  }

  private byte[] getRowKey(String logPartition) {
    return Bytes.add(ROW_KEY_PREFIX, Bytes.toBytes(logPartition));
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
