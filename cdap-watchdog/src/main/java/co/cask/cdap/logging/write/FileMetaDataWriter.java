/*
 * Copyright © 2017 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.framework.LogPathIdentifier;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Writes meta data about log files to log meta table.
 */
public class FileMetaDataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetaDataManager.class);
  private static final byte[] COLUMN_PREFIX_VERSION = new byte[] {1};
  private static final byte[] ROW_KEY_PREFIX = Bytes.toBytes(200);
  private static final String META_TABLE_NAME = LoggingConfiguration.LOG_META_DATA_TABLE;

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;


  @Inject
  public FileMetaDataWriter(final DatasetFramework framework,
                            final TransactionSystemClient txClient) {
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(framework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.datasetFramework = framework;
  }

  /**
   * Persists meta data associated with a log file.
   *
   * @param identifier logging context identifier.
   * @param eventTimeMs start log time associated with the file.
   * @param currentTimeMs current time during file creation.
   * @param location log file location.
   */
  public void writeMetaData(final LogPathIdentifier identifier,
                            final long eventTimeMs,
                            final long currentTimeMs,
                            final Location location) throws Exception {
    LOG.debug("Writing meta data for logging context {} with eventTime {} currentTime {} and location {}",
              identifier.getRowKey(), eventTimeMs, currentTimeMs, location);

    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        Table table = getLogMetaTable(context);
        // add column version prefix for new format
        byte[] columnKey = Bytes.add(COLUMN_PREFIX_VERSION, Bytes.toBytes(eventTimeMs), Bytes.toBytes(currentTimeMs));
        table.put(getRowKey(identifier), columnKey, Bytes.toBytes(location.toURI().toString()));
      }
    });
  }

  private Table getLogMetaTable(DatasetContext datasetContext) throws IOException, DatasetManagementException {
    DatasetId metaTableInstanceId = NamespaceId.SYSTEM.dataset(META_TABLE_NAME);
    return DatasetsUtil.getOrCreateDataset(datasetContext,
                                           datasetFramework, metaTableInstanceId, Table.class.getName(),
                                           DatasetProperties.EMPTY);
  }

  private byte[] getRowKey(LogPathIdentifier logPathIdentifier) {
    return Bytes.add(ROW_KEY_PREFIX, logPathIdentifier.getRowKey().getBytes());
  }

  /**
   * Executes the given runnable with a transaction. Any exception will result in {@link RuntimeException}.
   */
  private void txExecute(Transactional transactional, TxRunnable runnable) {
    try {
      transactional.execute(runnable);
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }
}
