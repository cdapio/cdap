/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.LineageDataset;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Basic implementation of {@link LineageWriter} that writes to {@link LineageDataset} directly.
 */
public class BasicLineageWriter implements LineageWriter {

  private static final Logger LOG = LoggerFactory.getLogger(BasicLineageWriter.class);

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @VisibleForTesting
  @Inject
  public BasicLineageWriter(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
        NamespaceId.SYSTEM, ImmutableMap.of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  @Override
  public void addAccess(ProgramRunId run, DatasetId datasetId, AccessType accessType,
                        @Nullable NamespacedEntityId component) {
    long accessTime = System.currentTimeMillis();
    LOG.debug("Writing access for run {}, dataset {}, accessType {}, component {}, accessTime = {}",
              run, datasetId, accessType, component, accessTime);

    Transactionals.execute(transactional, context -> {
      LineageDataset lineageDataset = getLineageDataset(context, datasetFramework);
      lineageDataset.addAccess(run, datasetId, accessType, accessTime, component);
    });
  }

  @Override
  public void addAccess(ProgramRunId run, StreamId streamId, AccessType accessType,
                        @Nullable NamespacedEntityId component) {
    long accessTime = System.currentTimeMillis();
    LOG.debug("Writing access for run {}, stream {}, accessType {}, component {}, accessTime = {}",
              run, streamId, accessType, component, accessTime);

    Transactionals.execute(transactional, context -> {
      LineageDataset lineageDataset = getLineageDataset(context, datasetFramework);
      lineageDataset.addAccess(run, streamId, accessType, accessTime, component);
    });
  }

  /**
   * Returns an instance {@link LineageDataset}.
   */
  protected LineageDataset getLineageDataset(DatasetContext datasetContext, DatasetFramework datasetFramework) {
    return LineageDataset.getLineageDataset(datasetContext, datasetFramework);
  }
}
