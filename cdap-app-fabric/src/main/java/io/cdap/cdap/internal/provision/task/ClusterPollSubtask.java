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

import io.cdap.cdap.internal.provision.ProvisioningOp;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategy;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Provisioning subtask that polls for cluster status until it is different than a specified status.
 */
public class ClusterPollSubtask extends ProvisioningSubtask {
  private final ClusterStatus status;

  public ClusterPollSubtask(Provisioner provisioner, ProvisionerContext provisionerContext, ClusterStatus status,
                            Function<Cluster, Optional<ProvisioningOp.Status>> transition) {
    super(provisioner, provisionerContext, transition);
    this.status = status;
  }

  @Override
  protected Cluster execute(Cluster cluster) throws Exception {
    PollingStrategy pollingStrategy = provisioner.getPollingStrategy(provisionerContext, cluster);
    long startTime = System.currentTimeMillis();
    int numPolls = 0;
    ClusterStatus currentStatus;
    do {
      currentStatus = provisioner.getClusterStatus(provisionerContext, cluster);
      if (currentStatus == status) {
        TimeUnit.MILLISECONDS.sleep(pollingStrategy.nextPoll(numPolls++, startTime));
      }
    } while (currentStatus == status);
    return new Cluster(cluster, currentStatus);
  }
}
