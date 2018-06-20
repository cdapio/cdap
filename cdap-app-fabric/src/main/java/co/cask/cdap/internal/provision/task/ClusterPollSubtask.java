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
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;

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
    while (cluster.getStatus() == status) {
      cluster = provisioner.getClusterDetail(provisionerContext, cluster);
      // TODO: CDAP-13346 use provisioner specified polling strategy instead of hardcoded sleep
      TimeUnit.SECONDS.sleep(2);
    }
    return cluster;
  }
}
