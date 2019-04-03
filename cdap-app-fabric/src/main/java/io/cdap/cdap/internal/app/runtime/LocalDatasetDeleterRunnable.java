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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Class responsible for deleting the local datasets associated with the completed, failed, or killed workflow runs.
 */
public class LocalDatasetDeleterRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(LocalDatasetDeleterRunnable.class);
  private static final Map<String, String> PROPERTIES =
    Collections.singletonMap(Constants.AppFabric.WORKFLOW_LOCAL_DATASET_PROPERTY, Boolean.toString(true));
  private final NamespaceAdmin namespaceAdmin;
  private final Store store;
  private final DatasetFramework datasetFramework;

  public LocalDatasetDeleterRunnable(NamespaceAdmin namespaceAdmin, Store store, DatasetFramework datasetFramework) {
    this.namespaceAdmin = namespaceAdmin;
    this.store = store;
    this.datasetFramework = datasetFramework;
  }

  @Override
  public void run() {
    try {
      List<NamespaceMeta> list = namespaceAdmin.list();
      for (NamespaceMeta namespaceMeta : list) {
        Collection<DatasetSpecificationSummary> specs
          = datasetFramework.getInstances(namespaceMeta.getNamespaceId(), PROPERTIES);

        if (specs.isEmpty()) {
          // avoid fetching run records
          continue;
        }
        Set<String> activeRuns = getActiveRuns(namespaceMeta.getNamespaceId());
        for (DatasetSpecificationSummary spec : specs) {
          deleteLocalDataset(namespaceMeta.getName(), spec.getName(), activeRuns, spec.getProperties());
        }
      }
    } catch (Throwable t) {
      LOG.warn("Failed to delete the local datasets.", t);
    }
  }

  private Set<String> getActiveRuns(NamespaceId namespaceId) {
    Map<ProgramRunId, RunRecordMeta> activeRuns = store.getActiveRuns(namespaceId);
    Set<String> runs = new HashSet<>();
    for (Map.Entry<ProgramRunId, RunRecordMeta> entry : activeRuns.entrySet()) {
      runs.add(entry.getValue().getPid());
    }
    return runs;
  }

  private void deleteLocalDataset(final String namespaceName, final String datasetName, Set<String> activeRuns,
                                  Map<String, String> properties)
    throws Exception {
    String[] split = datasetName.split("\\.");
    String runId = split[split.length - 1];

    if (activeRuns.contains(runId)
      || Boolean.parseBoolean(properties.get(Constants.AppFabric.WORKFLOW_KEEP_LOCAL))) {
      return;
    }

    final DatasetId datasetId = new DatasetId(namespaceName, datasetName);
    try {
      Retries.callWithRetries(new Retries.Callable<Void, Exception>() {
        @Override
        public Void call() throws Exception {
          datasetFramework.deleteInstance(datasetId);
          LOG.info("Deleted local dataset instance {}", datasetId);
          return null;
        }
      }, RetryStrategies.fixDelay(Constants.Retry.LOCAL_DATASET_OPERATION_RETRY_DELAY_SECONDS, TimeUnit.SECONDS));
    } catch (Exception e) {
      LOG.warn("Failed to delete the Workflow local dataset instance {}", datasetId, e);
    }
  }
}
