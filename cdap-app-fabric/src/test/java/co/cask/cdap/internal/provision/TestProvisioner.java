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

import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import co.cask.cdap.runtime.spi.provisioner.RetryableProvisionException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Provisioner for unit tests. Has a spec similar to the default yarn provisioner.
 */
public class TestProvisioner implements Provisioner {
  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
    "yarn", "Default YARN Provisioner",
    "Runs programs on the CDAP master cluster. Does not provision any resources.",
    new HashMap<>());

  @Override
  public ProvisionerSpecification getSpec() {
    return SPEC;
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    // no-op
  }

  @Override
  public Cluster createCluster(ProvisionerContext context) throws RetryableProvisionException {
    return new Cluster(context.getProgramRun().getRun(), ClusterStatus.RUNNING,
                       Collections.emptyList(), Collections.emptyMap());
  }

  @Override
  public ClusterStatus getClusterStatus(ProvisionerContext context,
                                        Cluster cluster) throws RetryableProvisionException {
    ClusterStatus status = cluster.getStatus();
    return status == ClusterStatus.DELETING ? ClusterStatus.NOT_EXISTS : status;
  }

  @Override
  public void deleteCluster(ProvisionerContext context, Cluster cluster) throws RetryableProvisionException {
    // no-op
  }
}
