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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Performs steps to deprovision a cluster for a program run.
 */
public class DeprovisionTask extends ProvisioningTask {
  private static final Logger LOG = LoggerFactory.getLogger(DeprovisionTask.class);
  private final Provisioner provisioner;
  private final ProvisionerContext provisionerContext;
  private final ProvisionerNotifier provisionerNotifier;
  private final Transactional transactional;
  private final DatasetFramework datasetFramework;

  public DeprovisionTask(ProgramRunId programRunId, Provisioner provisioner,
                         ProvisionerContext provisionerContext, ProvisionerNotifier provisionerNotifier,
                         Transactional transactional, DatasetFramework datasetFramework) {
    super(programRunId);
    this.provisioner = provisioner;
    this.provisionerContext = provisionerContext;
    this.provisionerNotifier = provisionerNotifier;
    this.transactional = transactional;
    this.datasetFramework = datasetFramework;
  }

  @Override
  public void run() {
    ClusterInfo clusterInfo = Transactionals.execute(transactional, datasetContext -> {
      ProvisionerDataset provisionerDataset = ProvisionerDataset.get(datasetContext, datasetFramework);
      return provisionerDataset.getClusterInfo(programRunId);
    });

    try {
      Cluster cluster = clusterInfo.getCluster();
      provisioner.deleteCluster(provisionerContext, cluster);
      ClusterOp op = new ClusterOp(ClusterOp.Type.DEPROVISION, ClusterOp.Status.POLLING_DELETE);
      cluster = new Cluster(cluster == null ? null : cluster.getName(), ClusterStatus.DELETING,
                            cluster == null ? Collections.emptyList() : cluster.getNodes(),
                            cluster == null ? Collections.emptyMap() : cluster.getProperties());
      ClusterInfo pollingInfo = new ClusterInfo(clusterInfo, op, cluster);
      Transactionals.execute(transactional, dsContext -> {
        ProvisionerDataset dataset = ProvisionerDataset.get(dsContext, datasetFramework);
        dataset.putClusterInfo(pollingInfo);
      });

      ClusterStatus status = ClusterStatus.DELETING;
      while (status == ClusterStatus.DELETING) {
        status = provisioner.getClusterDetail(provisionerContext, cluster).getStatus();
        TimeUnit.SECONDS.sleep(10);
      }

      // TODO: CDAP-13246 handle unexpected states and retry
      switch (status) {
        case NOT_EXISTS:
          provisionerNotifier.deprovisioned(programRunId);
          Transactionals.execute(transactional, dsContext -> {
            ProvisionerDataset dataset = ProvisionerDataset.get(dsContext, datasetFramework);
            dataset.deleteClusterInfo(programRunId);
          });
          break;
      }
    } catch (Throwable t) {
      LOG.warn("Error deprovisioning cluster for program run {}", programRunId, t);
      // TODO: CDAP-13246 handle retries
    }
  }
}
