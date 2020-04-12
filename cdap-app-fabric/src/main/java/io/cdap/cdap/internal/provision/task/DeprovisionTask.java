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
 *
 */

package io.cdap.cdap.internal.provision.task;

import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.internal.provision.ProvisionerNotifier;
import io.cdap.cdap.internal.provision.ProvisioningOp;
import io.cdap.cdap.internal.provision.ProvisioningTaskInfo;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Performs steps to deprovision a cluster for a program run. Before any operation is performed, state is persisted
 * to the ProvisionerStore to record what we are doing. This is done in case we crash in the middle of the task
 * and the task is later restarted. The operation state transition looks like:
 *
 *                                                        |-- (state == NOT_FOUND) --> Deleted
 * RequestingDelete -- request delete --> PollingDelete --|
 *                                                        |-- (state == FAILED || state == ORPHANED) --> Orphaned
 *
 * Some cluster statuses are not expected when polling for delete state. They are handled as follows:
 *
 * RequestingDelete -- (state == RUNNING) --> RequestingDelete
 *
 * PollingDelete -- (state == RUNNING) --> RequestingDelete
 *
 * PollingDelete -- (state == CREATING) --> Orphaned
 *
 */
public class DeprovisionTask extends ProvisioningTask {
  private static final Logger LOG = LoggerFactory.getLogger(DeprovisionTask.class);

  private final ProvisionerNotifier provisionerNotifier;
  private final Location keysDir;

  public DeprovisionTask(ProvisioningTaskInfo initialTaskInfo, TransactionRunner transactionRunner,
                         int retryTimeLimitSecs, Provisioner provisioner,
                         ProvisionerContext provisionerContext, ProvisionerNotifier provisionerNotifier,
                         LocationFactory locationFactory) {
    super(provisioner, provisionerContext, initialTaskInfo, transactionRunner, retryTimeLimitSecs);
    this.provisionerNotifier = provisionerNotifier;
    this.keysDir = locationFactory.create(initialTaskInfo.getSecureKeysDir());
  }

  @Override
  protected Map<ProvisioningOp.Status, ProvisioningSubtask> createSubTasks(ProvisioningTaskInfo initialTaskInfo) {
    Map<ProvisioningOp.Status, ProvisioningSubtask> subtasks = new HashMap<>();

    // RequestingDelete
    ProvisioningSubtask subtask =
      new ClusterDeleteSubtask(provisioner, provisionerContext,
                               cluster -> Optional.of(cluster.getStatus() == ClusterStatus.RUNNING
                                                        ? ProvisioningOp.Status.REQUESTING_DELETE
                                                        : ProvisioningOp.Status.POLLING_DELETE));
    subtasks.put(ProvisioningOp.Status.REQUESTING_DELETE, subtask);

    // PollingDelete
    subtask = new ClusterPollSubtask(provisioner, provisionerContext, cluster -> {
      switch (cluster.getStatus()) {
        case DELETING:
          return Optional.of(ProvisioningOp.Status.POLLING_DELETE);
        case NOT_EXISTS:
          try {
            provisionerNotifier.deprovisioned(programRunId);
          } finally {
            // Delete the keys. We only delete when the cluster is gone.
            Locations.deleteQuietly(keysDir, true);
          }
          return Optional.of(ProvisioningOp.Status.DELETED);
        case RUNNING:
          return Optional.of(ProvisioningOp.Status.REQUESTING_DELETE);
        case CREATING:
        case FAILED:
        case ORPHANED:
          LOG.warn("Got unexpected cluster state {} while trying to delete the cluster. "
                     + "The cluster will be marked as orphaned.", cluster.getStatus());
          provisionerNotifier.orphaned(programRunId);
          return Optional.of(ProvisioningOp.Status.ORPHANED);
      }
      // should never get here
      throw new IllegalStateException(String.format("Unexpected cluster state %s while polling for cluster state.",
                                                    cluster.getStatus()));
    });
    subtasks.put(ProvisioningOp.Status.POLLING_DELETE, subtask);

    // end states
    subtasks.put(ProvisioningOp.Status.ORPHANED, EndSubtask.INSTANCE);
    subtasks.put(ProvisioningOp.Status.DELETED, EndSubtask.INSTANCE);
    subtasks.put(ProvisioningOp.Status.CANCELLED, EndSubtask.INSTANCE);

    return subtasks;
  }

  @Override
  protected void handleSubtaskFailure(ProvisioningTaskInfo taskInfo, Exception e) {
    provisionerNotifier.orphaned(programRunId);
  }

  @Override
  protected void handleStateSaveFailure(ProvisioningTaskInfo taskInfo, Exception e) {
    provisionerNotifier.orphaned(programRunId);
  }
}
