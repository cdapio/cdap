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

package co.cask.cdap.internal.provision.task;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.provision.ProvisionerNotifier;
import co.cask.cdap.internal.provision.ProvisioningOp;
import co.cask.cdap.internal.provision.ProvisioningTaskInfo;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Performs steps to provision a cluster for a program run. Before any operation is performed, state is persisted
 * to the ProvisionerDataset to record what we are doing. This is done in case we crash in the middle of the task
 * and the task is later restarted. The operation state transition looks like:
 *
 *         --------------------------------- (state == NOT_FOUND) -------------------------------------|
 *         |                                                                                           |
 *         v                            |-- (state == FAILED) --> RequestingDelete --> PollingDelete --|
 * RequestingCreate --> PollingCreate --|
 *                                      |-- (state == RUNNING) --> Initializing --> Created
 *
 *
 * PollingCreate -- (state == NOT_EXISTS) --> RequestCreate
 *
 * PollingCreate -- (state == DELETING) --> PollingDelete
 *
 * PollingCreate -- (state == ORPHANED) --> Failed
 *
 *
 * PollingDelete -- (state == RUNNING) --> Initializing --> Created
 *
 * PollingDelete -- (state == FAILED || state == ORPHANED) --> Orphaned
 *
 * PollingDelete -- (state == CREATING) --> PollingCreate
 *
 * PollingDelete -- (state == NOT_FOUND and timeout reached) --> Failed
 */
public class ProvisionTask extends ProvisioningTask {
  private static final Logger LOG = LoggerFactory.getLogger(ProvisionTask.class);
  private final Provisioner provisioner;
  private final ProvisionerContext provisionerContext;
  private final ProvisionerNotifier provisionerNotifier;

  public ProvisionTask(ProvisioningTaskInfo initialTaskInfo, Transactional transactional,
                       DatasetFramework datasetFramework,
                       Provisioner provisioner, ProvisionerContext provisionerContext,
                       ProvisionerNotifier provisionerNotifier, int retryTimeLimitSecs) {
    super(initialTaskInfo, transactional, datasetFramework, retryTimeLimitSecs);
    this.provisioner = provisioner;
    this.provisionerContext = provisionerContext;
    this.provisionerNotifier = provisionerNotifier;
  }

  @Override
  protected Map<ProvisioningOp.Status, ProvisioningSubtask> getSubtasks() {
    Map<ProvisioningOp.Status, ProvisioningSubtask> subtasks = new HashMap<>();

    subtasks.put(ProvisioningOp.Status.REQUESTING_CREATE, createClusterCreateSubtask());
    subtasks.put(ProvisioningOp.Status.POLLING_CREATE, createPollingCreateSubtask());
    subtasks.put(ProvisioningOp.Status.REQUESTING_DELETE,
                 new ClusterDeleteSubtask(provisioner, provisionerContext,
                                          cluster -> Optional.of(ProvisioningOp.Status.POLLING_DELETE)));
    subtasks.put(ProvisioningOp.Status.POLLING_DELETE, createPollingDeleteSubtask());
    subtasks.put(ProvisioningOp.Status.INITIALIZING, createInitializeSubtask());
    subtasks.put(ProvisioningOp.Status.FAILED, EndSubtask.INSTANCE);
    subtasks.put(ProvisioningOp.Status.CREATED, EndSubtask.INSTANCE);

    return subtasks;
  }

  @Override
  protected void handleSubtaskFailure(ProvisioningTaskInfo taskInfo, Exception e) {
    provisionerNotifier.deprovisioning(programRunId);
  }

  @Override
  protected void handleStateSaveFailure(ProvisioningTaskInfo taskInfo, TransactionFailureException e) {
    if (taskInfo.getProvisioningOp().getStatus() == ProvisioningOp.Status.REQUESTING_CREATE) {
      // if we failed to write that we're requesting a cluster create, it means no cluster was created yet,
      // so we can transition directly to deprovisioned
      provisionerNotifier.deprovisioned(programRunId);
    } else {
      // otherwise, we need to try deprovisioning the cluster
      provisionerNotifier.deprovisioning(programRunId);
    }
  }

  private ProvisioningSubtask createClusterCreateSubtask() {
    return new ClusterCreateSubtask(provisioner, provisionerContext, cluster -> {
      if (cluster == null) {
        // this is in violation of the provisioner contract, but in case somebody writes a provisioner that
        // returns a null cluster.
        LOG.warn("Provisioner {} returned an invalid null cluster. " +
                    "Sending notification to de-provision it.", provisioner.getSpec().getName());
        provisionerNotifier.deprovisioning(programRunId);
        // RequestingCreate --> Failed
        return Optional.of(ProvisioningOp.Status.FAILED);
      }

      return Optional.of(ProvisioningOp.Status.POLLING_CREATE);
    });
  }

  private ProvisioningSubtask createPollingCreateSubtask() {
    return new ClusterPollSubtask(provisioner, provisionerContext, ClusterStatus.CREATING, cluster -> {
      switch (cluster.getStatus()) {
        case RUNNING:
          return Optional.of(ProvisioningOp.Status.INITIALIZING);
        case NOT_EXISTS:
          // this might happen if the cluster is manually deleted during the provision task
          // in this scenario, we try creating the cluster again
          return Optional.of(ProvisioningOp.Status.REQUESTING_CREATE);
        case FAILED:
          // create failed, issue a request to delete the cluster
          return Optional.of(ProvisioningOp.Status.REQUESTING_DELETE);
        case DELETING:
          // create failed and it is somehow in deleting. This is just like the failed scenario,
          // except we don't need to issue the delete request. Transition to polling delete.
          return Optional.of(ProvisioningOp.Status.POLLING_DELETE);
        case ORPHANED:
          // something went wrong, try to deprovision
          provisionerNotifier.deprovisioning(programRunId);
          return Optional.of(ProvisioningOp.Status.FAILED);
      }
      // should never get here
      throw new IllegalStateException(String.format("Unexpected cluster state %s while polling for cluster state.",
                                                    cluster.getStatus()));
    });
  }

  private ProvisioningSubtask createPollingDeleteSubtask() {
    long taskStartTime = System.currentTimeMillis();
    return new ClusterPollSubtask(provisioner, provisionerContext, ClusterStatus.DELETING, cluster -> {
      switch (cluster.getStatus()) {
        case CREATING:
          // this would be really weird, but provisioners can do whatever they want
          return Optional.of(ProvisioningOp.Status.POLLING_CREATE);
        case RUNNING:
          // this would be really weird, but provisioners can do whatever they want
          return Optional.of(ProvisioningOp.Status.INITIALIZING);
        case NOT_EXISTS:
          // delete succeeded, try to re-create cluster unless we've timed out
          if (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - taskStartTime) > retryTimeLimitSecs) {
            // over the time out. Give up and transition to deprovisioned
            provisionerNotifier.deprovisioned(programRunId);
            return Optional.of(ProvisioningOp.Status.FAILED);
          } else {
            return Optional.of(ProvisioningOp.Status.REQUESTING_CREATE);
          }
        case FAILED:
          // create failed, issue a request to delete the cluster
          return Optional.of(ProvisioningOp.Status.REQUESTING_DELETE);
        case DELETING:
        case ORPHANED:
          // delete failed or something went wrong, try to deprovision
          provisionerNotifier.deprovisioning(programRunId);
          return Optional.of(ProvisioningOp.Status.FAILED);
      }
      // should never get here
      throw new IllegalStateException(String.format("Unexpected cluster state %s while polling for cluster state.",
                                                    cluster.getStatus()));
    });
  }

  private ProvisioningSubtask createInitializeSubtask() {
    return new ClusterInitializeSubtask(provisioner, provisionerContext, cluster -> {
      provisionerNotifier.provisioned(programRunId, initialTaskInfo.getProgramOptions(),
                                      initialTaskInfo.getProgramDescriptor(), initialTaskInfo.getUser(),
                                      cluster, initialTaskInfo.getSecureKeyInfo());
      return Optional.of(ProvisioningOp.Status.CREATED);
    });
  }

}
