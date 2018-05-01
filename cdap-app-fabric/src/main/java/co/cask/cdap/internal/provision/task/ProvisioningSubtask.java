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

import co.cask.cdap.internal.provision.ProvisioningOp;
import co.cask.cdap.internal.provision.ProvisioningTaskInfo;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.provisioner.RetryableProvisionException;

import java.util.Optional;
import java.util.function.Function;

/**
 * A subtask that is performed as part of a provisioning operation.
 */
public abstract class ProvisioningSubtask {
  private final Function<Cluster, Optional<ProvisioningOp.Status>> transition;
  protected final Provisioner provisioner;
  protected final ProvisionerContext provisionerContext;

  protected ProvisioningSubtask(Provisioner provisioner, ProvisionerContext provisionerContext,
                                Function<Cluster, Optional<ProvisioningOp.Status>> transition) {
    this.provisioner = provisioner;
    this.provisionerContext = provisionerContext;
    this.transition = transition;
  }

  /**
   * Executes the subtask and returns the next subtask that should be executed if there is one.
   *
   * @param taskInfo information about the task being executed, including the current cluster state
   * @return task info to be sent to the next subtask if there is one
   * @throws Exception if there was an error executing the subtask
   */
  public Optional<ProvisioningTaskInfo> execute(ProvisioningTaskInfo taskInfo) throws Exception {
    Cluster cluster = taskInfo.getCluster();
    Cluster nextCluster = execute(cluster);
    return transition.apply(nextCluster).map(nextState -> {
      ProvisioningOp nextOp = new ProvisioningOp(taskInfo.getProvisioningOp().getType(), nextState);
      return new ProvisioningTaskInfo(taskInfo, nextOp, nextCluster);
    });
  }

  /**
   * Execute the subtask and return the cluster that results from the subtask.
   * This method must be implemented in an idempotent fashion as it may be retried.
   *
   * @param cluster the cluster before executing the subtask
   * @return the cluster after successful completion of the subtask
   * @throws InterruptedException if the subtask was interrupted
   * @throws RetryableProvisionException if the subtask failed in a retryable fashion
   * @throws Exception if there was some other type of error executing the subtask
   */
  protected abstract Cluster execute(Cluster cluster) throws Exception;
}
