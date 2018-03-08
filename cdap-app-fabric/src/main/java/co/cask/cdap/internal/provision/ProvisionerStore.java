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

package co.cask.cdap.internal.provision;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Wrapper around {@link ProvisionerDataset} that runs every operation within a transaction.
 */
public class ProvisionerStore {
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @Inject
  ProvisionerStore(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
                                                                   new TransactionSystemClientAdapter(txClient),
                                                                   NamespaceId.SYSTEM,
                                                                   Collections.emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  @Nullable
  public String getSubscriberState(String clientId) throws Exception {
    return Transactionals.execute(transactional, datasetContext -> {
      ProvisionerDataset table = getTable(datasetContext);
      return table.getSubscriberState(clientId);
    });
  }

  public void persistSubscriberState(String clientId, String lastFetchedId) throws Exception {
    Transactionals.execute(transactional, datasetContext -> {
      ProvisionerDataset table = getTable(datasetContext);
      table.persistSubscriberState(clientId, lastFetchedId);
    });
  }

  public List<ClusterInfo> listClusterInfo() {
    return Transactionals.execute(transactional, datasetContext -> {
      ProvisionerDataset table = getTable(datasetContext);
      return table.listClusterInfo();
    });
  }

  @Nullable
  public ClusterInfo getClusterInfo(ProgramRunId programRunId) {
    return Transactionals.execute(transactional, datasetContext -> {
      ProvisionerDataset table = getTable(datasetContext);
      return table.getClusterInfo(programRunId);
    });
  }

  public void putClusterInfo(ClusterInfo clusterInfo) {
    Transactionals.execute(transactional, datasetContext -> {
      ProvisionerDataset table = getTable(datasetContext);
      table.putClusterInfo(clusterInfo);
    });
  }

  public void deleteClusterInfo(ProgramRunId programRunId) {
    Transactionals.execute(transactional, datasetContext -> {
      ProvisionerDataset table = getTable(datasetContext);
      table.deleteClusterInfo(programRunId);
    });
  }

  private ProvisionerDataset getTable(DatasetContext context) throws IOException, DatasetManagementException {
    ProvisionerDataset.createIfNotExists(datasetFramework);
    return ProvisionerDataset.get(context);
  }
}
