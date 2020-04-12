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
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

/**
 * Provisioning subtask that deletes a cluster.
 */
public class ClusterDeleteSubtask extends ProvisioningSubtask {

  public ClusterDeleteSubtask(Provisioner provisioner, ProvisionerContext provisionerContext,
                              Function<Cluster, Optional<ProvisioningOp.Status>> transition) {
    super(provisioner, provisionerContext, transition);
  }

  @Override
  public Cluster execute(Cluster cluster) throws Exception {
    ClusterStatus clusterStatus = provisioner.deleteClusterWithStatus(provisionerContext, cluster);
    return new Cluster(cluster == null ? null : cluster.getName(), clusterStatus,
                       cluster == null ? Collections.emptyList() : cluster.getNodes(),
                       cluster == null ? Collections.emptyMap() : cluster.getProperties());
  }
}
