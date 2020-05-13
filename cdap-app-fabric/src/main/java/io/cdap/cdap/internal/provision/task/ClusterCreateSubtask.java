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

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.provision.ProvisioningOp;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.ssh.SSHContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Provisioning subtask that creates a cluster.
 */
public class ClusterCreateSubtask extends ProvisioningSubtask {

  public ClusterCreateSubtask(Provisioner provisioner, ProvisionerContext provisionerContext,
                              Function<Cluster, Optional<ProvisioningOp.Status>> transition) {
    super(provisioner, provisionerContext, transition);
  }

  @Override
  public Cluster execute(Cluster cluster) throws Exception {
    Cluster nextCluster = provisioner.createCluster(provisionerContext);
    SSHContext sshContext = provisionerContext.getSSHContext();
    // ssh context can be null if ssh is not being used to submit job
    if (sshContext == null) {
      return new Cluster(nextCluster.getName(), nextCluster.getStatus(), nextCluster.getNodes(),
                         nextCluster.getProperties());
    }

    Map<String, String> properties = new HashMap<>(nextCluster.getProperties());
    // Set the SSH user if the provisioner sets it.
    provisionerContext.getSSHContext().getSSHKeyPair().ifPresent(sshKeyPair -> {
      properties.put(Constants.RuntimeMonitor.SSH_USER, sshKeyPair.getPublicKey().getUser());
    });
    return new Cluster(nextCluster.getName(), nextCluster.getStatus(), nextCluster.getNodes(), properties);
  }
}
