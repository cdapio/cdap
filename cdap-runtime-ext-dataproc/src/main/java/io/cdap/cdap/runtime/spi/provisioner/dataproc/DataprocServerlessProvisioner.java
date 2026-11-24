/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategies;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategy;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocClusterInfo;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocServerlessRuntimeJobManager;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provisioner for executing jobs on Dataproc Serverless.
 */
public class DataprocServerlessProvisioner extends AbstractDataprocProvisioner {

  private static final Logger LOG = LoggerFactory.getLogger(DataprocServerlessProvisioner.class);

  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
      "gcp-dataproc-serverless", "Dataproc Serverless",
      "Execute Spark jobs as serverless workloads on GCP Dataproc Serverless.");

  private static final String CLUSTER_NAME = "dataproc-serverless-mock";

  public DataprocServerlessProvisioner() {
    super(SPEC);
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    // Validates the properties (re-uses existing DataprocConf checks)
    DataprocConf.create(properties);
  }

  @Override
  protected String getClusterName(ProvisionerContext context) {
    return CLUSTER_NAME;
  }

  @Override
  public Cluster createCluster(ProvisionerContext context) throws Exception {
    LOG.warn("TEST_LOG: Entering DataprocServerlessProvisioner.createCluster. Returning mock RUNNING cluster.");
    // No-op for cluster creation. Return a mock cluster that is already in RUNNING state.
    Map<String, String> properties = createContextProperties(context);
    return new Cluster(CLUSTER_NAME, ClusterStatus.RUNNING, Collections.emptyList(), properties);
  }

  @Override
  protected void doDeleteCluster(ProvisionerContext context, Cluster cluster, DataprocConf conf) {
    LOG.warn("TEST_LOG: Entering DataprocServerlessProvisioner.doDeleteCluster. This is a mock no-op deletion.");
    // No-op for cluster deletion.
  }

  @Override
  public ClusterStatus getClusterStatus(ProvisionerContext context, Cluster cluster) {
    ClusterStatus status = cluster.getStatus();
    LOG.warn("TEST_LOG: DataprocServerlessProvisioner.getClusterStatus. Current status: {}", status);
    // If the cluster status is DELETING, report that it is deleted (NOT_EXISTS).
    return status == ClusterStatus.DELETING ? ClusterStatus.NOT_EXISTS : status;
  }

  @Override
  public Cluster getClusterDetail(ProvisionerContext context, Cluster cluster) {
    return new Cluster(cluster, getClusterStatus(context, cluster));
  }

  @Override
  public PollingStrategy getPollingStrategy(ProvisionerContext context, Cluster cluster) {
    // Fixed polling strategy since there is no cluster creation time.
    return PollingStrategies.fixedInterval(0, TimeUnit.SECONDS);
  }

  @Override
  public Optional<RuntimeJobManager> getRuntimeJobManager(ProvisionerContext context) {
    LOG.warn("TEST_LOG: Entering DataprocServerlessProvisioner.getRuntimeJobManager.");
    Map<String, String> properties = createContextProperties(context);
    DataprocConf conf = DataprocConf.create(properties);

    try {
      String clusterName = getClusterName(context);
      String projectId = conf.getProjectId();
      String region = conf.getRegion();
      String bucket = conf.getGcsBucket() != null ? conf.getGcsBucket() : properties.get("bucket");
      
      LOG.warn("TEST_LOG: Creating DataprocServerlessRuntimeJobManager for project: {}, region: {}, bucket: {}",
               projectId, region, bucket);
      return Optional.of(new DataprocServerlessRuntimeJobManager(
          new DataprocClusterInfo(context, clusterName, conf.getDataprocCredentials(),
              getRootUrl(conf), projectId, region, bucket, getCommonDataprocLabels(context)),
          Collections.unmodifiableMap(properties), context.getCDAPVersionInfo()));
    } catch (Exception e) {
      LOG.warn("TEST_LOG: Exception while initializing DataprocServerlessRuntimeJobManager: ", e);
      throw new RuntimeException("Error while getting credentials for Dataproc Serverless. ", e);
    }
  }
}
