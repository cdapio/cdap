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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.logging.meta.LoggingStoreTableUtil;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handles reading/writing of file metadata. only used by test cases to test old format meta data reading and cleanup.
 */
public class FileMetaDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetaDataManager.class);

  private static final byte[] ROW_KEY_PREFIX = LoggingStoreTableUtil.OLD_FILE_META_ROW_KEY_PREFIX;

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  // Note: The FileMetaDataManager needs to have a RootLocationFactory because for custom mapped namespaces the
  // location mapped to a namespace are from root of the filesystem. The FileMetaDataManager stores a location in
  // bytes to a hbase table and to construct it back to a Location it needs to work with a root based location factory.
  @Inject
  FileMetaDataManager(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  /**
   * Returns the {@link Table} for storing metadata.
   */
  private Table getMetaTable(DatasetContext context) throws IOException, DatasetManagementException {
    return LoggingStoreTableUtil.getMetadataTable(datasetFramework, context);
  }

  /**
   * Persists meta data associated with a log file.
   *
   * @param loggingContext logging context containing the meta data.
   * @param startTimeMs    start log time associated with the file.
   * @param location       log file.
   */
  public void writeMetaData(final LoggingContext loggingContext,
                            final long startTimeMs,
                            final Location location) throws Exception {
    writeMetaData(loggingContext.getLogPartition(), startTimeMs, location);
  }

  /**
   * Persists meta data associated with a log file.
   * @param logPartition partition name that is used to group log messages
   * @param startTimeMs  start log time associated with the file.
   * @param location     log file.
   */
  private void writeMetaData(final String logPartition,
                             final long startTimeMs,
                             final Location location) throws Exception {
    LOG.debug("Writing meta data for logging context {} as startTimeMs {} and location {}",
              logPartition, startTimeMs, location);

    transactional.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        getMetaTable(context).put(getRowKey(logPartition),
                                  Bytes.toBytes(startTimeMs),
                                  Bytes.toBytes(location.toURI().getPath()));
      }
    });
  }

  private byte[] getRowKey(String logPartition) {
    return Bytes.add(ROW_KEY_PREFIX, Bytes.toBytes(logPartition));
  }

}
