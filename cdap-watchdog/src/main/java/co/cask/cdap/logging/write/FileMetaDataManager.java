/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Processor;
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.GenericLoggingContext;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.save.LogSaverTableUtil;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Handles reading/writing of file metadata.
 */
public final class FileMetaDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetaDataManager.class);

  private static final byte[] ROW_KEY_PREFIX = Bytes.toBytes(200);
  private static final byte[] ROW_KEY_PREFIX_END = Bytes.toBytes(201);
  private static final NavigableMap<?, ?> EMPTY_MAP = Maps.unmodifiableNavigableMap(new TreeMap());

  private final RootLocationFactory rootLocationFactory;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final String logBaseDir;
  private final LogSaverTableUtil tableUtil;
  private final TransactionExecutorFactory transactionExecutorFactory;
  private final Impersonator impersonator;

  /// Note: The FileMetaDataManager needs to have a RootLocationFactory because for custom mapped namespaces the
  // location mapped to a namespace are from root of the filesystem. The FileMetaDataManager stores a location in
  // bytes to a hbase table and to construct it back to a Location it needs to work with a root based location factory.
  @Inject
  public FileMetaDataManager(final LogSaverTableUtil tableUtil, TransactionExecutorFactory txExecutorFactory,
                             RootLocationFactory rootLocationFactory,
                             NamespacedLocationFactory namespacedLocationFactory, CConfiguration cConf,
                             Impersonator impersonator) {
    this.tableUtil = tableUtil;
    this.transactionExecutorFactory = txExecutorFactory;
    this.rootLocationFactory = rootLocationFactory;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);
    this.impersonator = impersonator;
  }

  /**
   * Persists meta data associated with a log file.
   *
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
   *
   * @param logPartition partition name that is used to group log messages
   * @param startTimeMs start log time associated with the file.
   * @param location log file.
   */
  private void writeMetaData(final String logPartition,
                             final long startTimeMs,
                             final Location location) throws Exception {
    LOG.debug("Writing meta data for logging context {} as startTimeMs {} and location {}",
              logPartition, startTimeMs, location);

    execute(new TransactionExecutor.Procedure<Table>() {
      @Override
      public void apply(Table table) throws Exception {
        table.put(getRowKey(logPartition),
                  Bytes.toBytes(startTimeMs),
                  Bytes.toBytes(location.toURI().toString()));
      }
    });
  }

  /**
   * Returns a list of log files for a logging context.
   *
   * @param loggingContext logging context.
   * @return Sorted map containing key as start time, and value as log file.
   */
  public NavigableMap<Long, Location> listFiles(final LoggingContext loggingContext) throws Exception {
    return execute(new TransactionExecutor.Function<Table, NavigableMap<Long, Location>>() {
      @Override
      public NavigableMap<Long, Location> apply(Table table) throws Exception {
        NamespaceId namespaceId = LoggingContextHelper.getNamespaceId(loggingContext);
        final Row cols = table.get(getRowKey(loggingContext));

        if (cols.isEmpty()) {
          //noinspection unchecked
          return (NavigableMap<Long, Location>) EMPTY_MAP;
        }

        final NavigableMap<Long, Location> files = new TreeMap<>();
        impersonator.doAs(namespaceId, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            for (Map.Entry<byte[], byte[]> entry : cols.getColumns().entrySet()) {
              // the location can be any location from on the filesystem for custom mapped namespaces
              files.put(Bytes.toLong(entry.getKey()),
                        rootLocationFactory.create(new URI(Bytes.toString(entry.getValue()))));
            }
            return null;
          }
        });
        return files;
      }
    });
  }

  /**
   * Scans meta data and gathers all the files up to a limited number of records.
   *
   * @param startTableKey row key for the scan and column from where scan will be started
   * @param limit batch size for number of columns to be read
   * @param processor processor to process files
   * @return next row key + start column for next iteration, returns null if end of table
   */
  @Nullable
  public TableKey scanFiles(final TableKey startTableKey, final int limit,
                            final Processor<URI, Set<URI>> processor) {
    return execute(new TransactionExecutor.Function<Table, TableKey>() {
      @Override
      public TableKey apply(Table table) throws Exception {
        byte[] startKey = getRowKey(startTableKey.getRowKey());
        byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
        byte[] startColumn = startTableKey.getStartColumn();

        try (Scanner scanner = table.scan(startKey, stopKey)) {
          Row row;
          int colCount = 0;

          while ((row = scanner.next()) != null) {
            // Get the next logging context
            byte[] rowKey = row.getRow();
            byte[] stopCol = null;

            // Go through files for a logging context
            for (final Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
              byte[] colName = entry.getKey();
              byte[] colValue = entry.getValue();

              // while processing a row key, if start column is present, then it should be considered only for the
              // first time. Skip any column less/equal to start column
              if (startColumn != null && Bytes.compareTo(startColumn, colName) >= 0) {
                continue;
              }

              // Stop if we exceeded the limit
              if (colCount >= limit) {
                //  return current row key if we exceeded the limit
                return new TableKey(getLogPartition(rowKey), stopCol);
              }

              stopCol = colName;
              colCount++;
              processor.process(new URI(Bytes.toString(colValue)));
            }
            startColumn = null;
          }
        }
        return null;
      }
    });
  }

  /**
   * Class which holds table key (row key + column) for log meta
   */
  public static final class TableKey {
    private final String rowKey;
    private final byte[] startColumn;

    public TableKey(String rowKey, @Nullable byte[] startColumn) {
      this.rowKey = rowKey;
      this.startColumn = startColumn;
    }

    public String getRowKey() {
      return rowKey;
    }

    public byte[] getStartColumn() {
      return startColumn;
    }
  }

  /**
   * Deletes meta data until a given time, while keeping the latest meta data even if less than the given time.
   *
   * @param untilTime time until the meta data will be deleted.
   * @param callback callback called before deleting a meta data column.
   * @return total number of columns deleted.
   */
  public int cleanMetaData(final long untilTime, final DeleteCallback callback) throws Exception {
    return execute(new TransactionExecutor.Function<Table, Integer>() {
      @Override
      public Integer apply(Table table) throws Exception {
        int deletedColumns = 0;
        try (Scanner scanner = table.scan(ROW_KEY_PREFIX, ROW_KEY_PREFIX_END)) {
          Row row;
          while ((row = scanner.next()) != null) {
            byte[] rowKey = row.getRow();
            final NamespaceId namespaceId = getNamespaceId(rowKey);
            String namespacedLogDir = null;
            try {
              namespacedLogDir = impersonator.doAs(namespaceId, new Callable<String>() {
                @Override
                public String call() throws Exception {
                  return LoggingContextHelper.getNamespacedBaseDirLocation(namespacedLocationFactory,
                                                                           logBaseDir, namespaceId,
                                                                           impersonator).toString();
                }
              });
            } catch (Exception e) {
              if (e instanceof NamespaceNotFoundException) {
                LOG.warn("Namespace {} does not exist. Only delete metadata for it", namespaceId.getEntityName(), e);
              } else {
                LOG.warn("Error while accessing namespace {} for log cleanup, skipping it",
                         namespaceId.getEntityName(), e);
                continue;
              }
            }

            for (final Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
              try {
                byte[] colName = entry.getKey();
                URI file = new URI(Bytes.toString(entry.getValue()));
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Got file {} with start time {}", file, Bytes.toLong(colName));
                }

                if (Strings.isNullOrEmpty(namespacedLogDir)) {
                  LOG.warn("File {} is not present and will be deleted from metadata because namespace {} " +
                             "does not exist", Bytes.toString(entry.getValue()), namespaceId.getEntityName());
                  table.delete(rowKey, colName);
                  deletedColumns++;
                  continue;
                }

                Location fileLocation = impersonator.doAs(namespaceId, new Callable<Location>() {
                  @Override
                  public Location call() throws Exception {
                    return rootLocationFactory.create(new URI(Bytes.toString(entry.getValue())));
                  }
                });

                if (!fileLocation.exists()) {
                  LOG.warn("Log file {} does not exist, but metadata is present", file);
                  table.delete(rowKey, colName);
                  deletedColumns++;
                } else if (fileLocation.lastModified() < untilTime) {
                  // Delete if file last modified time is less than untilTime
                  callback.handle(namespaceId, fileLocation, namespacedLogDir);
                  table.delete(rowKey, colName);
                  deletedColumns++;
                }
              } catch (Exception e) {
                if (e instanceof NamespaceNotFoundException) {
                  LOG.warn("File {} is not present because namespace {} does not exist",
                           Bytes.toString(entry.getValue()), namespaceId.getEntityName());
                } else {
                  LOG.error("Got exception deleting file {}", Bytes.toString(entry.getValue()), e);
                }
              }
            }
          }
        }
        return deletedColumns;
      }
    });
  }

  private void execute(TransactionExecutor.Procedure<Table> func) {
    try {
      Table table = tableUtil.getMetaTable();
      if (table instanceof TransactionAware) {
        TransactionExecutor txExecutor = Transactions.createTransactionExecutor(transactionExecutorFactory,
                                                                                (TransactionAware) table);
        txExecutor.execute(func, table);
      } else {
        throw new RuntimeException(String.format("Table %s is not TransactionAware, " +
                                                   "Exception while trying to cast it to TransactionAware. " +
                                                   "Please check why the table is not TransactionAware", table));
      }
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error accessing %s table", tableUtil.getMetaTableName()), e);
    }
  }

  private <T> T execute(TransactionExecutor.Function<Table, T> func) {
    try {
      Table table = tableUtil.getMetaTable();
      if (table instanceof TransactionAware) {
        TransactionExecutor txExecutor = Transactions.createTransactionExecutor(transactionExecutorFactory,
                                                                                (TransactionAware) table);
        return txExecutor.execute(func, table);
      } else {
        throw new RuntimeException(String.format("Table %s is not TransactionAware, " +
                                                   "Exception while trying to cast it to TransactionAware. " +
                                                   "Please check why the table is not TransactionAware", table));
      }
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error accessing %s table", tableUtil.getMetaTableName()), e);
    }
  }

  private String getLogPartition(byte[] rowKey) {
    int offset = ROW_KEY_PREFIX_END.length;
    int length = rowKey.length - offset;
    return Bytes.toString(rowKey, offset, length);
  }

  private NamespaceId getNamespaceId(byte[] rowKey) {
    String logPartition = getLogPartition(rowKey);
    Preconditions.checkArgument(logPartition != null, "Log partition cannot be null");
    String [] partitions = logPartition.split(":");
    Preconditions.checkArgument(partitions.length == 3,
                                "Expected log partition to be in the format <ns>:<entity>:<sub-entity>");
    // don't care about the app or the program, only need the namespace
    return LoggingContextHelper.getNamespaceId(new GenericLoggingContext(partitions[0], partitions[1], partitions[2]));
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
    void handle(NamespaceId namespaceId, Location location, String namespacedLogBaseDir);
  }
}
