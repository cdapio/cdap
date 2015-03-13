/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.tx.DatasetContext;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.save.LogSaverTableUtil;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;

/**
 * Handles reading/writing of file metadata.
 */
public final class FileMetaDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetaDataManager.class);

  private static final byte[] ROW_KEY_PREFIX = Bytes.toBytes(200);
  private static final byte[] ROW_KEY_PREFIX_END = Bytes.toBytes(201);
  private static final String DEVELOPER_STRING = "developer";

  private final TransactionExecutorFactory txExecutorFactory;

  private final LocationFactory locationFactory;

  private final DatasetFramework dsFramework;

  private final Transactional<DatasetContext<Table>, Table> mds;
  private final String logBaseDir;

  @Inject
  public FileMetaDataManager(final LogSaverTableUtil tableUtil, TransactionExecutorFactory txExecutorFactory,
                             LocationFactory locationFactory, DatasetFramework dsFramework,
                             CConfiguration cConf) {
    this.dsFramework = dsFramework;
    this.txExecutorFactory = txExecutorFactory;
    this.mds = Transactional.of(txExecutorFactory, new Supplier<DatasetContext<Table>>() {
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
    this.logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);
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
            String namespacedLogDir = LoggingContextHelper.getNamespacedBaseDir(logBaseDir, getLogPartition(rowKey));
            byte [] maxCol = getMaxKey(row.getColumns());

            for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
              byte [] colName = entry.getKey();
              if (LOG.isDebugEnabled()) {
                LOG.debug("Got file {} with start time {}", Bytes.toString(entry.getValue()),
                          Bytes.toLong(colName));
              }
              // Delete if colName is less than tillTime, but don't delete the last one
              if (Bytes.compareTo(colName, tillTimeBytes) < 0 && Bytes.compareTo(colName, maxCol) != 0) {
                callback.handle(locationFactory.create(new URI(Bytes.toString(entry.getValue()))), namespacedLogDir);
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

  private String getLogPartition(byte[] rowKey) {
    int offset = ROW_KEY_PREFIX_END.length;
    int length = rowKey.length - offset;
    return Bytes.toString(rowKey, offset, length);
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
    public void handle(Location location, String namespacedLogBaseDir);
  }

  /**
   * Upgrades the log meta table
   * Note: Currently this supports upgrade from 2.6 to 2.8 and does upgrade for namespaces
   *
   * @throws Exception
   */
  public void upgrade() throws Exception {
    final Transactional<DatasetContext<Table>, Table> oldLogMDS;
    // get the old log meta data store from the default namespace
    oldLogMDS = Transactional.of(txExecutorFactory, new Supplier<DatasetContext<Table>>() {
      @Override
      public DatasetContext<Table> get() {
        try {
          Table table = DatasetsUtil.getOrCreateDataset(dsFramework, Id.DatasetInstance.from
                                                          (Constants.DEFAULT_NAMESPACE_ID, Joiner.on(".").join(
                                                            Constants.SYSTEM_NAMESPACE,
                                                            LoggingConfiguration.LOG_META_DATA_TABLE)),
                                                        "table", DatasetProperties.EMPTY,
                                                        DatasetDefinition.NO_ARGUMENTS, null);
          return DatasetContext.of(table);
        } catch (Exception e) {
          LOG.error("Failed to access {} table", Joiner.on(".").join(Constants.SYSTEM_NAMESPACE,
                                                                     LoggingConfiguration.LOG_META_DATA_TABLE), e);
          throw Throwables.propagate(e);
        }
      }
    });

    oldLogMDS.execute(new TransactionExecutor.Function<DatasetContext<Table>, Void>() {
      @Override
      public Void apply(DatasetContext<Table> ctx) throws Exception {
        Scanner rows = ctx.get().scan(null, null);
        Row row;
        while ((row = rows.next()) != null) {
          String key = Bytes.toString(row.getRow(), ROW_KEY_PREFIX.length,
                                      (row.getRow().length - ROW_KEY_PREFIX.length));
          Map<byte[], byte[]> allColumns = row.getColumns();
          for (Map.Entry<byte[], byte[]> entry : allColumns.entrySet()) {
            long startTimeMs = Bytes.toLong(entry.getKey());
            String oldPath = Bytes.toString(entry.getValue());
            Location newPath;
            String newKey;
            if (key.startsWith(Constants.CDAP_NAMESPACE) || key.startsWith(DEVELOPER_STRING)) {
              newPath = upgradePath(key, oldPath);
              newKey = upgradeKey(key);
              try {
                LOG.info("Upgrading logs for {}", key);
                writeMetaData(newKey, startTimeMs, newPath);
              } catch (Exception e) {
                LOG.warn("Failed to update the log record in log meta table", e);
                throw e;
              }
            }
          }
        }
        return null;
      }
    });
  }

  /**
   * Replaces the accountId with the namespace in the key
   *
   * @param key the key which is the log partition
   * @return the new key with namespace
   */
  private String upgradeKey(String key) {
    if (key.startsWith(Constants.CDAP_NAMESPACE)) {
      return key.replace(Constants.CDAP_NAMESPACE, Constants.SYSTEM_NAMESPACE);
    }
    return key.replace(DEVELOPER_STRING, Constants.DEFAULT_NAMESPACE);
  }

  /**
   * Creates a new path depending on the old log file path
   *
   * @param key the key for this log meta entry
   * @param oldLocation the old log {@link Location}
   * @return the {@link Location}
   * @throws IOException
   * @throws URISyntaxException
   */
  private Location upgradePath(String key, String oldLocation) throws IOException, URISyntaxException {
    Location location = locationFactory.create(new URI(oldLocation));
    Location newLocation = null;
    if (key.startsWith(Constants.CDAP_NAMESPACE)) {
      // Example path of this type: hdfs://blah.blah.net/
      // cdap/logs/avro/cdap/services/service-appfabric/2015-02-26/1424988452088.avro
      // removes the second occurrence of "cdap/" in the path
      newLocation = updateLogLocation(location, Constants.SYSTEM_NAMESPACE);
      // newLocation will be: hdfs://blah.blah.net/cdap/system/logs/avro/services/
      // service-appfabric/2015-02-26/1424988452088.avro
    } else if (key.startsWith(DEVELOPER_STRING)) {
      // Example of this type: hdfs://blah.blah.net/cdap/logs/avro/developer/HelloWorld/
      // flow-WhoFlow/2015-02-26/1424988484082.avro
      newLocation = updateLogLocation(location, Constants.DEFAULT_NAMESPACE);
      // newLocation will be: hdfs://blah.blah.net/cdap/default/logs/avro/HelloWorld/
      // flow-WhoFlow/2015-02-26/1424988484082.avro
    }
    return newLocation;
  }

  /**
   * Strips different parts from the old log location and creates a new one
   *
   * @param location the old log {@link Location}
   * @param namespace the namespace which will be added to the new log location
   * @return the log {@link Location}
   * @throws IOException
   */
  private Location updateLogLocation(Location location, String namespace) throws IOException {
    String logFilename = location.getName();
    Location parentLocation = Locations.getParent(location);  // strip filename
    String date = parentLocation != null ? parentLocation.getName() : null;
    parentLocation = Locations.getParent(parentLocation); // strip date
    String programName = parentLocation != null ? parentLocation.getName() : null;
    parentLocation = Locations.getParent(parentLocation); // strip program name
    String programType = parentLocation != null ? parentLocation.getName() : null;
    parentLocation = Locations.getParent(parentLocation); // strip program type

    parentLocation = Locations.getParent(parentLocation); // strip old namespace
    String avro = parentLocation != null ? parentLocation.getName() : null;

    parentLocation = Locations.getParent(parentLocation); // strip avro

    String logs = parentLocation != null ? parentLocation.getName() : null;

    return locationFactory.create(namespace).append(logs).append(avro).append(programType).append(programName)
      .append(date).append(logFilename);
  }
}
