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

import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import co.cask.cdap.runtime.spi.provisioner.PollingStrategies;
import co.cask.cdap.runtime.spi.provisioner.PollingStrategy;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Native provisioner that doesn't provision a cluster
 */
public class NativeProvisioner implements Provisioner {

  public static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
    Profile.NATIVE_NAME, "Native",
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
  public Cluster createCluster(ProvisionerContext context) {
    return new Cluster(context.getProgramRun().getRun(), ClusterStatus.RUNNING,
                       Collections.emptyList(), Collections.emptyMap());
  }

  @Override
  public ClusterStatus getClusterStatus(ProvisionerContext context, Cluster cluster) {
    ClusterStatus status = cluster.getStatus();
    return status == ClusterStatus.DELETING ? ClusterStatus.NOT_EXISTS : status;
  }

  @Override
  public Cluster getClusterDetail(ProvisionerContext context, Cluster cluster) {
    return new Cluster(cluster, getClusterStatus(context, cluster));
  }

  @Override
  public void deleteCluster(ProvisionerContext context, Cluster cluster) {
    // no-op
  }

  @Override
  public PollingStrategy getPollingStrategy(ProvisionerContext context, Cluster cluster) {
    // shouldn't matter, as we won't ever poll
    return PollingStrategies.fixedInterval(2, TimeUnit.SECONDS);
  }
}
