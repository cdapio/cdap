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
import co.cask.cdap.runtime.spi.provisioner.Node;
import co.cask.cdap.runtime.spi.provisioner.PollingStrategies;
import co.cask.cdap.runtime.spi.provisioner.PollingStrategy;
import co.cask.cdap.runtime.spi.provisioner.ProgramRun;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import co.cask.cdap.runtime.spi.ssh.SSHKeyPair;
import co.cask.cdap.runtime.spi.ssh.SSHSession;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Provisions a cluster using GCE DataProc.
 */
public class DataProcProvisioner implements Provisioner {

  private static final Logger LOG = LoggerFactory.getLogger(DataProcProvisioner.class);
  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
    "gcp-dataproc", "Google Cloud Dataproc",
    "Google Cloud Dataproc is a fast, easy-to-use, fully-managed cloud service for running Apache Spark and Apache " +
      "Hadoop clusters in a simpler, more cost-efficient way.");
  private static final String CLUSTER_PREFIX = "cdap-";

  @Override
  public ProvisionerSpecification getSpec() {
    return SPEC;
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    DataProcConf conf = DataProcConf.fromProperties(properties);
    try {
      DataProcClient.fromConf(conf);
    } catch (IOException | GeneralSecurityException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  @Override
  public Cluster createCluster(ProvisionerContext context) throws Exception {
    // Generates and set the ssh key
    SSHKeyPair sshKeyPair = context.getSSHContext().generate("cdap");
    context.getSSHContext().setSSHKeyPair(sshKeyPair);

    DataProcConf conf = DataProcConf.fromProvisionerContext(context);
    String clusterName = getClusterName(context.getProgramRun());

    try (DataProcClient client = DataProcClient.fromConf(conf)) {
      // if it already exists, it means this is a retry. We can skip actually making the request
      Optional<Cluster> existing = client.getCluster(clusterName);
      if (existing.isPresent()) {
        return existing.get();
      }

      String imageVersion;
      switch (context.getSparkCompat()) {
        case SPARK1_2_10:
          imageVersion = "1.0";
          break;
        case SPARK2_2_11:
        default:
          imageVersion = "1.2";
          break;
      }

      // dataproc only allows label values to be lowercase letters, numbers, or dashes
      String cdapVersion = context.getCDAPVersion().toLowerCase();
      cdapVersion = cdapVersion.replaceAll("\\.", "_");
      Map<String, String> labels = Collections.singletonMap("cdap-version", cdapVersion);
      client.createCluster(clusterName, imageVersion, labels);
      return new Cluster(clusterName, ClusterStatus.CREATING, Collections.emptyList(), Collections.emptyMap());
    }
  }

  @Override
  public ClusterStatus getClusterStatus(ProvisionerContext context, Cluster cluster) throws Exception {
    DataProcConf conf = DataProcConf.fromProperties(context.getProperties());
    String clusterName = getClusterName(context.getProgramRun());

    try (DataProcClient client = DataProcClient.fromConf(conf)) {
      return client.getClusterStatus(clusterName);
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
  public void initializeCluster(ProvisionerContext context, Cluster cluster) throws Exception {
    // Start the ZK server
    try (SSHSession session = createSSHSession(context, getMasterExternalIp(cluster))) {
      LOG.debug("Starting zookeeper server.");
      String output = session.executeAndWait("sudo zookeeper-server start");
      LOG.debug("Zookeeper server started: {}", output);
    }
  }

  private SSHSession createSSHSession(ProvisionerContext provisionerContext, String host) throws IOException {
    try {
      return provisionerContext.getSSHContext().createSSHSession(host);
    } catch (IOException ioe) {
      if (Throwables.getRootCause(ioe) instanceof ConnectException) {
        throw new IOException(String.format(
                "Failed to connect to host %s. Ensure that GCP Firewall Ingress Rules exist that allow ssh and" +
                        " https (ports 22 and 443).", host),
                ioe);
      }
      throw ioe;
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

  private String getMasterExternalIp(Cluster cluster) {
    Node masterNode = cluster.getNodes().stream()
      .filter(node -> Node.Type.MASTER == node.getType())
      .findFirst().orElseThrow(() -> new IllegalArgumentException("Cluster has no node of master type: " + cluster));

    String ip = masterNode.getIpAddress();
    if (ip == null) {
      throw new IllegalArgumentException(String.format("External IP is not defined for node '%s' in cluster %s",
                                                       masterNode.getId(), cluster));
    }
    return ip;
  }

  @Override
  public PollingStrategy getPollingStrategy(ProvisionerContext context, Cluster cluster) {
    DataProcConf conf = DataProcConf.fromProperties(context.getProperties());
    PollingStrategy strategy = PollingStrategies.fixedInterval(conf.getPollInterval(), TimeUnit.SECONDS);
    switch (cluster.getStatus()) {
      case CREATING:
        return PollingStrategies.initialDelay(strategy, conf.getPollCreateDelay(),
                                              conf.getPollCreateJitter(), TimeUnit.SECONDS);
      case DELETING:
        return PollingStrategies.initialDelay(strategy, conf.getPollDeleteDelay(), TimeUnit.SECONDS);
    }
    LOG.warn("Received a request to get the polling strategy for unexpected cluster status {}", cluster.getStatus());
    return strategy;
  }

  // Name must start with a lowercase letter followed by up to 51 lowercase letters,
  // numbers, or hyphens, and cannot end with a hyphen
  // We'll use app-runid, where app is truncated to fit, lowercased, and stripped of invalid characters
  @VisibleForTesting
  static String getClusterName(ProgramRun programRun) {
    String cleanedAppName = programRun.getApplication().replaceAll("[^A-Za-z0-9\\-]", "").toLowerCase();
    // 51 is max length, need to subtract the prefix and 1 extra for the '-' separating app name and run id
    int maxAppLength = 51 - CLUSTER_PREFIX.length() - 1 - programRun.getRun().length();
    if (cleanedAppName.length() > maxAppLength) {
      cleanedAppName = cleanedAppName.substring(0, maxAppLength);
    }
    return CLUSTER_PREFIX + cleanedAppName + "-" + programRun.getRun();
  }
}
