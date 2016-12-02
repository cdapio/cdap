/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.logging.gateway.handlers.store;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

/**
 * This is to for log handler to access run records. Log handler cannot use Store directly because watchdog module
 * doesn't have dependency on app-fabric.
 */
public class ProgramStore {

  private static final DatasetId APP_META_INSTANCE_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @Inject
  public ProgramStore(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  /**
   * Returns run record for a given run.
   *
   * @param id program id
   * @param runId run id
   * @return run record for runid
   */
  public RunRecordMeta getRun(final ProgramId id, final String runId) {
    try {
      return Transactions.execute(transactional, new TxCallable<RunRecordMeta>() {
        @Override
        public RunRecordMeta call(DatasetContext context) throws Exception {
          Table table = DatasetsUtil.getOrCreateDataset(context, datasetFramework, APP_META_INSTANCE_ID,
                                                        Table.class.getName(), DatasetProperties.EMPTY);
          AppMetadataStore metaStore = new AppMetadataStore(table);
          return metaStore.getRun(id, runId);

        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }
}
