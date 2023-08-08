/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.cdap.cdap.error.api.ErrorTagProvider.ErrorTag;
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
 * Provisioner for connecting an existing Dataproc cluster.
 */
public class ServerlessDataprocProvisioner extends AbstractDataprocProvisioner {

  private static final Logger LOG = LoggerFactory.getLogger(ServerlessDataprocProvisioner.class);

  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
      "gcp-serverless-dataproc", "Serverless Dataproc",
      "Connect and Execute batches on Serverless Dataproc.");
  // Keys for looking up system properties

  private static final String CLUSTER_NAME = "SERVERLESS_DATAPROC";
//  private static final String SSH_USER = "sshUser";
//  private static final String SSH_KEY = "sshKey";
  private static final DataprocClientFactory CLIENT_FACTORY = new DefaultDataprocClientFactory();

  public ServerlessDataprocProvisioner() {
    super(SPEC);
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    // Creates the DataprocConf for validation
    DataprocConf.create(properties);
    //TODO : create DataprocConf for serverless
  }

  @Override
  protected String getClusterName(ProvisionerContext context) {
    return CLUSTER_NAME;
  }

  @Override
  public Cluster createCluster(ProvisionerContext context) throws Exception {
    Map<String, String> contextProperties = createContextProperties(context);
    DataprocConf conf = DataprocConf.create(contextProperties);

    // TODO : Assuming we don't use SSH ANY MORE
    /*
    if (context.getRuntimeMonitorType() == RuntimeMonitorType.SSH) {
      String sshUser = contextProperties.get(SSH_USER);
      String sshKey = contextProperties.get(SSH_KEY);
      if (Strings.isNullOrEmpty(sshUser) || Strings.isNullOrEmpty(sshKey)) {
        throw new DataprocRuntimeException(
            "SSH User and key are required for monitoring through SSH.",
            ErrorTag.CONFIGURATION);
      }

      SSHKeyPair sshKeyPair = new SSHKeyPair(new SSHPublicKey(sshUser, ""),
          () -> sshKey.getBytes(StandardCharsets.UTF_8));
      // The ssh context shouldn't be null, but protect it in case there is platform bug
      Optional.ofNullable(context.getSSHContext()).ifPresent(c -> c.setSSHKeyPair(sshKeyPair));
    }
    */

    //TODO check if system labels are required at BATCH level
     /*
    String clusterName = contextProperties.get(CLUSTER_NAME);
    try (DataprocClient client = CLIENT_FACTORY.create(conf)) {



      try {
        client.updateClusterLabels(clusterName, getSystemLabels());
      } catch (DataprocRuntimeException e) {
        // It's ok not able to update the labels
        // Only log the stacktrace if trace log level is enabled
        if (LOG.isTraceEnabled()) {
          LOG.trace("Cannot update cluster labels due to {}", e.getMessage(), e);
        } else {
          LOG.debug("Cannot update cluster labels due to {}", e.getMessage());
        }
      }



    }
    */
  /** Return a FAKE CLUSTER for now */
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


// new  OVERRIDES

  /**
   * Provides implementation of {@link RuntimeJobManager}.
   */
  @Override
  public Optional<RuntimeJobManager> getRuntimeJobManager(ProvisionerContext context) {
    Map<String, String> properties = createContextProperties(context);
    DataprocConf conf = DataprocConf.create(properties);

    try {
      String clusterName = getClusterName(context);
      //TODO : HARDCODED : REMOVE
      String projectId = "cdf-test-317207";//conf.getProjectId() ;
      String region = "us-west1";//conf.getRegion();
      String bucket =
        conf.getGcsBucket() != null ? conf.getGcsBucket() : properties.get(DataprocUtils.BUCKET);
      //TODO figure out bucket usage

      Map<String, String> systemLabels = getCommonDataprocLabels(context);
      LOG.warn(" SANKET : in  : getRuntimeJobManager 4: bucket : {} ",bucket );
      bucket = "serverlessdataproc" ; //TODO HARDCODED
      return Optional.of(
        new ServerlessDataprocRuntimeJobManager(
          new DataprocClusterInfo(context, clusterName, conf.getDataprocCredentials(),
                                  getRootUrl(conf), projectId,
                                  region, bucket, systemLabels),
          Collections.unmodifiableMap(properties), context.getCDAPVersionInfo()));
    } catch (Exception e) {
      throw new RuntimeException("Error while getting credentials for dataproc. ", e);
    }
  }


}
