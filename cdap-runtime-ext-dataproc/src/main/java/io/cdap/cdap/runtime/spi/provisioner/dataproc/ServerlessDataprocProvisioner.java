/*
 * Copyright © 2025 Cask Data, Inc.
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

import com.google.common.base.Strings;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.runtime.spi.common.DataprocImageVersion;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategies;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategy;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocClusterInfo;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocRuntimeJobManager;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobDetail;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import io.cdap.cdap.runtime.spi.runtimejob.ServerlessDataprocRuntimeJobManager;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Provisioner to submit a job to Dataproc's Serverless (Dataproc Batch)
 */
public class ServerlessDataprocProvisioner extends AbstractDataprocProvisioner {

  private static final Logger LOG = LoggerFactory.getLogger(ServerlessDataprocProvisioner.class);

  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
    "gcp-serverless-dataproc", "Serverless Dataproc",
    "Connect and Execute jobs on Serverless Dataproc (Batches).");
  // Keys for looking up system properties

  private static final String CLUSTER_NAME = "SERVERLESS_DATAPROC";
  private static final DataprocClientFactory CLIENT_FACTORY = new DefaultDataprocClientFactory();

  public ServerlessDataprocProvisioner() {
    super(SPEC);
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    // Creates the DataprocConf for validation
    DataprocConf.create(properties);
  }

  @Override
  protected String getClusterName(ProvisionerContext context) {
    return context.getProperties().get(CLUSTER_NAME);
  }

  @Override
  public Cluster createCluster(ProvisionerContext context) throws Exception {

    // Responsibilities during existing dp cluster :
    //TODO 1: Ensure labels are added while submitting a job. from AbstractDataprocProvisioner#getCommonDataprocLabels
    //TODO 2: Ensure SparkRuntime Version (image) is compatible while submitting job.
    Map<String, String> contextProperties = createContextProperties(context);
    DataprocConf conf = DataprocConf.create(contextProperties);

    // Return a FAKE CLUSTER for now
    return new Cluster(
      CLUSTER_NAME,
      ClusterStatus.RUNNING,
      Collections.emptyList(), Collections.emptyMap());
  }

  @Override
  protected void doDeleteCluster(ProvisionerContext context, Cluster cluster, DataprocConf conf) {
    // no-op
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
  public PollingStrategy getPollingStrategy(ProvisionerContext context, Cluster cluster) {
    if (cluster.getStatus() == ClusterStatus.CREATING) {
      return PollingStrategies.fixedInterval(0, TimeUnit.SECONDS);
    }
    DataprocConf conf = DataprocConf.create(createContextProperties(context));
    return PollingStrategies.fixedInterval(conf.getPollInterval(), TimeUnit.SECONDS);
  }

  /**
   * Provides implementation of {@link RuntimeJobManager}.
   */
  @Override
  public Optional<RuntimeJobManager> getRuntimeJobManager(ProvisionerContext context) {
    Map<String, String> properties = createContextProperties(context);
    DataprocConf conf = DataprocConf.create(properties);

    // if this system property is not provided, we will assume that ssh should be used instead of
    // runtime job manager for job launch.
//    if (!conf.isRuntimeJobManagerEnabled()) {
//      return Optional.empty();
//    }
    try {
      String clusterName = getClusterName(context);
      String projectId = conf.getProjectId();
      String region = conf.getRegion();
      String bucket =
        conf.getGcsBucket() != null ? conf.getGcsBucket() : properties.get(DataprocUtils.BUCKET);
      return Optional.of(
        new ServerlessDataprocRuntimeJobManager(
          new DataprocClusterInfo(context, clusterName, conf.getDataprocCredentials(),
                                  getRootUrl(conf), projectId,
                                  region, bucket, getCommonDataprocLabels(context)),
          Collections.unmodifiableMap(properties), context.getCDAPVersionInfo(), getImageVersion(conf)));
    } catch (Exception e) {
      throw new RuntimeException("Error while getting credentials for dataproc. ", e);
    }
  }

  @Override
  public ClusterStatus deleteClusterWithStatus(ProvisionerContext context, Cluster cluster) throws Exception {
    LOG.warn("SANKET here in deleteClusterWithStatus");
    RuntimeJobManager jobManager = getRuntimeJobManager(context).orElse(null);

    if (jobManager != null) {
      LOG.warn("SANKET here in deleteClusterWithStatus : jobManager");
      try {
        RuntimeJobDetail jobDetail = jobManager.getDetail(context.getProgramRunInfo()).orElse(null);
        if (jobDetail != null && !jobDetail.getStatus().isTerminated()) {
          LOG.warn("SANKET : trying to cancel for running " );
          jobManager.kill(jobDetail);
        }
      } catch (Exception e) {
        LOG.warn(" Failed to cancel job ");
        return ClusterStatus.RUNNING;
      } finally {
        jobManager.close();
      }

    }
    return ClusterStatus.DELETING;
  }

  String getImageVersion(DataprocConf conf) {
    String imageVersion = conf.getImageVersion();
    if (imageVersion == null) {
      imageVersion = "1.1";
    }
    LOG.warn("Going for Serverless version : " + imageVersion);
    return imageVersion;
  }
}