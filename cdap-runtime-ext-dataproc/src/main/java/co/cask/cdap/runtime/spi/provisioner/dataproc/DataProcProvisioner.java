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

package co.cask.cdap.runtime.spi.provisioner.dataproc;

import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import co.cask.cdap.runtime.spi.provisioner.ProgramRun;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import com.google.common.annotations.VisibleForTesting;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Provisions a cluster using GCE DataProc.
 */
public class DataProcProvisioner implements Provisioner {
  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
    "gce-dataproc", "GCE DataProc Provisioner",
    "Runs programs on the GCE DataProc clusters.",
    new HashMap<>());

  @Override
  public ProvisionerSpecification getSpec() {
    return SPEC;
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    DataProcConf.fromProperties(properties);
  }

  @Override
  public Cluster createCluster(ProvisionerContext context) throws Exception {
    DataProcConf conf = DataProcConf.fromProperties(context.getProperties());
    String clusterName = getClusterName(context.getProgramRun());

    try (DataProcClient client = DataProcClient.fromConf(conf)) {
      // if it already exists, it means this is a retry. We can skip actually making the request
      Optional<Cluster> existing = client.getCluster(clusterName);

      if (!existing.isPresent()) {
        client.createCluster(clusterName);
        return new Cluster(clusterName, ClusterStatus.CREATING, Collections.emptyList(), Collections.emptyMap());
      } else {
        return existing.get();
      }
    }
  }

  @Override
  public Cluster getClusterDetail(ProvisionerContext context,
                                  Cluster cluster) throws Exception {
    DataProcConf conf = DataProcConf.fromProperties(context.getProperties());
    String clusterName = getClusterName(context.getProgramRun());

    try (DataProcClient client = DataProcClient.fromConf(conf)) {
      Optional<Cluster> existing = client.getCluster(clusterName);
      return existing.orElseGet(() -> new Cluster(cluster, ClusterStatus.NOT_EXISTS));
    }
  }

  @Override
  public void deleteCluster(ProvisionerContext context, Cluster cluster) throws Exception {
    DataProcConf conf = DataProcConf.fromProperties(context.getProperties());
    String clusterName = getClusterName(context.getProgramRun());

    try (DataProcClient client = DataProcClient.fromConf(conf)) {
      client.deleteCluster(clusterName);
    }
  }

  // Name must start with a lowercase letter followed by up to 54 lowercase letters,
  // numbers, or hyphens, and cannot end with a hyphen
  // We'll use app-runid, where app is truncated to fit, lowercased, and stripped of invalid characters
  @VisibleForTesting
  static String getClusterName(ProgramRun programRun) {
    String cleanedAppName = programRun.getApplication().replaceAll("[^A-Za-z0-9\\-]", "").toLowerCase();
    int maxAppLength = 53 - programRun.getRun().length();
    if (cleanedAppName.length() > maxAppLength) {
      cleanedAppName = cleanedAppName.substring(0, maxAppLength);
    }
    return cleanedAppName + "-" + programRun.getRun();
  }
}
