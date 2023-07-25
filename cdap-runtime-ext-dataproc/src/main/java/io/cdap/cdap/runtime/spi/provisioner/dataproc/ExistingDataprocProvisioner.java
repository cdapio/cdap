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
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategies;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategy;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provisioner for connecting an existing Dataproc cluster.
 */
public class ExistingDataprocProvisioner extends AbstractDataprocProvisioner {

  private static final Logger LOG = LoggerFactory.getLogger(ExistingDataprocProvisioner.class);

  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
      "gcp-existing-dataproc", "Existing Dataproc",
      "Connect and Execute jobs on existing Dataproc cluster.");
  // Keys for looking up system properties

  private static final String CLUSTER_NAME = "clusterName";
  private static final String SSH_USER = "sshUser";
  private static final String SSH_KEY = "sshKey";
  private static final DataprocClientFactory CLIENT_FACTORY = new DefaultDataprocClientFactory();

  public ExistingDataprocProvisioner() {
    super(SPEC);
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    // Creates the DataprocConf for validation
    DataprocConf.create(properties);

    String clusterName = properties.get(CLUSTER_NAME);
    if (Strings.isNullOrEmpty(clusterName)) {
      throw new IllegalArgumentException("Dataproc cluster name is missing");
    }
  }

  @Override
  protected String getClusterName(ProvisionerContext context) {
    return context.getProperties().get(CLUSTER_NAME);
  }

  @Override
  public Cluster createCluster(ProvisionerContext context) throws Exception {
    Map<String, String> contextProperties = createContextProperties(context);
    DataprocConf conf = DataprocConf.create(contextProperties);

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

    String clusterName = contextProperties.get(CLUSTER_NAME);
    try (DataprocClient client = CLIENT_FACTORY.create(conf)) {
      try {
        client.updateClusterLabels(clusterName, getCommonDataprocLabels(context));
      } catch (DataprocRuntimeException e) {
        // It's ok not able to update the labels
        // Only log the stacktrace if trace log level is enabled
        if (LOG.isTraceEnabled()) {
          LOG.trace("Cannot update cluster labels due to {}", e.getMessage(), e);
        } else {
          LOG.debug("Cannot update cluster labels due to {}", e.getMessage());
        }
      }
      Cluster cluster = client.getCluster(clusterName)
          .filter(c -> c.getStatus() == ClusterStatus.RUNNING)
          .orElseThrow(() -> new DataprocRuntimeException("Dataproc cluster " + clusterName
              + " does not exist or not in running state.",
              ErrorTag.CONFIGURATION));

      // Determine cluster version and fail if version is smaller than 1.5
      Optional<String> optImageVer = client.getClusterImageVersion(clusterName);
      Optional<DataprocImageVersion> optComparableImageVer = optImageVer.map(this::extractVersion);
      if (!optImageVer.isPresent()) {
        LOG.warn("Unable to determine Dataproc version.");
      } else if (!optComparableImageVer.isPresent()) {
        LOG.warn("Unable to extract Dataproc version from string '{}'.", optImageVer.get());
      } else if (DATAPROC_1_5_VERSION.compareTo(optComparableImageVer.get()) > 0) {
        throw new DataprocRuntimeException(
            "Dataproc cluster must be version 1.5 or greater for pipeline execution.",
            ErrorTag.CONFIGURATION);
      }

      return cluster;
    }
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
}
