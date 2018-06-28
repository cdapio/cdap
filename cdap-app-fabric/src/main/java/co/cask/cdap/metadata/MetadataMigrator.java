/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.MetadataDatasetDefinition;
import co.cask.cdap.data2.metadata.dataset.MetadataEntry;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Metadata Migrator to migrate metadata from V1 metadata tables to V2 metadata tables.
 * Migration happens on business table as well as system table. It only migrates value and history rows.
 * Indexes will be automatically generated for every write on V2 metadata table.
 */
class MetadataMigrator extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataMigrator.class);
  // For outage, only log once per 60 seconds per message.
  private static final Logger OUTAGE_LOG = Loggers.sampling(LOG, LogSamplers.perMessage(
    () -> LogSamplers.limitRate(60000)));
  private static final List<DatasetId> DATASET_IDS = ImmutableList.of(NamespaceId.SYSTEM.dataset("system.metadata"),
                                                                      NamespaceId.SYSTEM.dataset("v2.system.metadata"),
                                                                      NamespaceId.SYSTEM.dataset("business.metadata"),
                                                                      NamespaceId.SYSTEM.dataset("v2.business.metadata")
  );

  private final DatasetFramework dsFramework;
  private final Transactional transactional;
  private final int batchSize;
  private volatile Thread runThread;
  private volatile boolean stopped;
  private volatile boolean hasV1Instance;

  MetadataMigrator(CConfiguration cConf, DatasetFramework dsFramework, TransactionSystemClient txClient) {
    this.dsFramework = dsFramework;
    this.batchSize = cConf.getInt(Constants.Metadata.MIGRATOR_BATCH_SIZE);
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(dsFramework),
                                                                   txClient, NamespaceId.SYSTEM,
                                                                   Collections.emptyMap(), null, null)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Metadata Migrator Service.");
  }

  @Override
  public void run() {
    runThread = Thread.currentThread();

    try {
      for (int i = 0; i < DATASET_IDS.size(); i = i + 2) {
        // assume we already have v1 metadata table instance
        hasV1Instance = true;

        while (!stopped && hasV1Instance) {
          try {
            final int index = i;
            // This thread migrates metadata in batches. It does following operations in a single transaction:
            // 1.) Scan V1 metadata table for value and history rows in batch of batchSize.
            // 2.) Write scanned MetadataEntries to V2 table.
            // 3.) Delete successfully written metadata entries from V1 table.
            Transactionals.execute(transactional, context -> {
              if (!dsFramework.hasInstance(DATASET_IDS.get(index))) {
                hasV1Instance = false;
                return;
              }

              MetadataDataset v1 = getMetadataDataset(context, DATASET_IDS.get(index));
              MetadataDataset v2 = getMetadataDataset(context, DATASET_IDS.get(index + 1));

              // All the metadata entries are written using setProperty because it does not modify the MetadataEntry
              // keys
              List<KeyValue<Long, MetadataEntry>> entries = v1.scanOrDeleteFromV1Table(batchSize, false);

              if (entries.isEmpty()) {
                // All the value and history rows have been migrated so stop this thread and drop V1 MetadataDataset
                dropV1MetadataDataset(index);
                LOG.debug("Migration for dataset {} is complete. This dataset is dropped.", DATASET_IDS.get(index));
                return;
              }

              v2.writeUpgradedRow(entries);
              // We do not need to keep checkpoints. Instead, we will just delete scanned rows from metadata dataset
              v1.scanOrDeleteFromV1Table(entries.size(), true);
            });
          } catch (Exception e) {
            OUTAGE_LOG.error("Exception while migrating metadata from {} to {}, will be retried. ", DATASET_IDS.get(i),
                             DATASET_IDS.get(i + 1), e);
            Thread.sleep(1000);
          }
        }
      }
    } catch (InterruptedException e) {
      // Interruption means stopping the service
    }

    // Clear the interrupt flag
    Thread.interrupted();
  }

  @Override
  protected String getServiceName() {
    return "metadata-migrator-service";
  }

  @Override
  protected void triggerShutdown() {
    stopped = true;
    if (runThread != null) {
      runThread.interrupt();
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Metadata Migrator Service.");
  }

  private void dropV1MetadataDataset(int index) throws DatasetManagementException, IOException {
    DatasetAdmin admin = dsFramework.getAdmin(DATASET_IDS.get(index), null);
    if (admin != null) {
      admin.drop();
      hasV1Instance = false;
    }
  }

  private MetadataDataset getMetadataDataset(DatasetContext context, DatasetId datasetId)
    throws IOException, DatasetManagementException {
    MetadataScope scope = datasetId.getDataset().contains("business") ? MetadataScope.USER : MetadataScope.SYSTEM;
    return DatasetsUtil.getOrCreateDataset(context, dsFramework, datasetId, MetadataDataset.class.getName(),
                                           DatasetProperties.builder().add(MetadataDatasetDefinition.SCOPE_KEY,
                                                                           scope.name()).build());
  }
}
