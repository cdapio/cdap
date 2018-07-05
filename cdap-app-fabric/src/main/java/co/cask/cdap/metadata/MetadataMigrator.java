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
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.AbstractRetryableScheduledService;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.MetadataDatasetDefinition;
import co.cask.cdap.data2.metadata.dataset.MetadataEntries;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * Metadata Migrator to migrate metadata from V1 metadata tables to V2 metadata tables.
 * Migration happens on business table as well as system table. It only migrates value and history rows.
 * Indexes will be automatically generated for every write on V2 metadata table.
 */
class MetadataMigrator extends AbstractRetryableScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataMigrator.class);
  private static final Queue<KeyValue<DatasetId, DatasetId>> DATASETS = new LinkedList<>();

  private final DatasetFramework dsFramework;
  private final Transactional transactional;
  private final int batchSize;

  MetadataMigrator(CConfiguration cConf, DatasetFramework dsFramework, TransactionSystemClient txClient) {
    // exponentially retry in case of failure to migrate
    super(RetryStrategies.exponentialDelay(1, 60, TimeUnit.SECONDS));
    this.dsFramework = dsFramework;
    this.batchSize = cConf.getInt(Constants.Metadata.MIGRATOR_BATCH_SIZE);
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(dsFramework),
                                                                   txClient, NamespaceId.SYSTEM,
                                                                   Collections.emptyMap(), null, null)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );
    DATASETS.add(new KeyValue<>(NamespaceId.SYSTEM.dataset("system.metadata"),
                                NamespaceId.SYSTEM.dataset("v2.system.metadata")));

    DATASETS.add(new KeyValue<>(NamespaceId.SYSTEM.dataset("business.metadata"),
                                NamespaceId.SYSTEM.dataset("v2.business.metadata")));
  }

  @Override
  protected void doStartUp() throws Exception {
    LOG.info("Starting Metadata Migrator Service.");
  }

  @Override
  public long runTask() throws Exception {
    if (DATASETS.isEmpty()) {
      stop();
      return 0;
    }

    try {
      KeyValue<DatasetId, DatasetId> datasetIdEntry = DATASETS.peek();
      if (!dsFramework.hasInstance(datasetIdEntry.getKey())) {
        DATASETS.poll();
        return 0;
      }

      // This thread migrates metadata in batches. It does following operations in a single transaction:
      // 1.) Scan V1 metadata table for value and history rows in batch of batchSize.
      // 2.) Write scanned MetadataEntries to V2 table.
      // 3.) Delete successfully written metadata entries from V1 table.
      Boolean dropV1Table = Transactionals.execute(transactional, context -> {
        MetadataDataset v1 = context.getDataset(datasetIdEntry.getKey().getDataset());
        MetadataDataset v2 = getMetadataDataset(context, datasetIdEntry.getValue());

        // All the metadata entries are written using setProperty because it does not modify the MetadataEntry
        // keys
        MetadataEntries entries = v1.scanFromV1Table(batchSize);

        if (entries.getEntries().isEmpty()) {
          // All the value and history rows have been migrated so stop this thread and drop V1 MetadataDataset
          return true;
        }

        v2.writeUpgradedRows(entries.getEntries());

        // We do not need to keep checkpoints. Instead, we will just delete scanned rows from metadata dataset
        v1.deleteRows(entries.getRows());
        return false;
      });

      if (dropV1Table) {
        dsFramework.deleteInstance(datasetIdEntry.getKey());
        LOG.info("Migration for dataset {} is complete. This dataset is dropped.", datasetIdEntry.getKey());
      }
    } catch (Exception e) {
      LOG.error("Error occurred while migrating metadata. ", e);
      throw e;
    }

    // Immediately migrate next batch to finish migration faster.
    return 0;
  }

  @Override
  protected String getServiceName() {
    return "metadata-migrator-service";
  }

  @Override
  protected void doShutdown() throws Exception {
    LOG.info("Stopping Metadata Migrator Service.");
  }

  private MetadataDataset getMetadataDataset(DatasetContext context, DatasetId datasetId)
    throws IOException, DatasetManagementException {
    MetadataScope scope = datasetId.getDataset().contains("business") ? MetadataScope.USER : MetadataScope.SYSTEM;
    return DatasetsUtil.getOrCreateDataset(context, dsFramework, datasetId, MetadataDataset.class.getName(),
                                           DatasetProperties.builder().add(MetadataDatasetDefinition.SCOPE_KEY,
                                                                           scope.name()).build());
  }
}
