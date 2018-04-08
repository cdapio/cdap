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

package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;

/**
 * Implementation of {@link WorkflowStateWriter} that writes to {@link AppMetadataStore} directly.
 */
public class BasicWorkflowStateWriter implements WorkflowStateWriter {

  private final CConfiguration cConf;
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @Inject
  BasicWorkflowStateWriter(CConfiguration cConf, DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.cConf = cConf;
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
        NamespaceId.SYSTEM, ImmutableMap.of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }


  @Override
  public void setWorkflowToken(ProgramRunId workflowRunId, WorkflowToken token) {
    Transactionals.execute(transactional, context -> {
      AppMetadataStore.create(cConf, context, datasetFramework).setWorkflowToken(workflowRunId, token);
    });
  }

  @Override
  public void addWorkflowNodeState(ProgramRunId workflowRunId, WorkflowNodeStateDetail nodeStateDetail) {
    Transactionals.execute(transactional, context -> {
      AppMetadataStore.create(cConf, context, datasetFramework).addWorkflowNodeState(workflowRunId, nodeStateDetail);
    });
  }
}
