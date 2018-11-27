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

import co.cask.cdap.runtime.spi.provisioner.Capabilities;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import co.cask.cdap.runtime.spi.provisioner.Node;
import co.cask.cdap.runtime.spi.provisioner.PollingStrategies;
import co.cask.cdap.runtime.spi.provisioner.PollingStrategy;
import co.cask.cdap.runtime.spi.provisioner.ProgramRun;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSystemContext;
import co.cask.cdap.runtime.spi.ssh.SSHKeyPair;
import co.cask.cdap.runtime.spi.ssh.SSHSession;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Provisions a cluster using GCP Dataproc.
 */
public class DataprocProvisioner implements Provisioner {

  private static final Logger LOG = LoggerFactory.getLogger(DataprocProvisioner.class);
  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
    "gcp-dataproc", "Google Cloud Dataproc",
    "Google Cloud Dataproc is a fast, easy-to-use, fully-managed cloud service for running Apache Spark and Apache " +
      "Hadoop clusters in a simpler, more cost-efficient way.");
  private static final String CLUSTER_PREFIX = "cdap-";
  // keys and values cannot be longer than 63 characters
  // keys and values can only contain lowercase letters, numbers, underscores, and dashes
  // keys must start with a lowercase letter
  // keys cannot be empty
  private static final Pattern LABEL_KEY_PATTERN = Pattern.compile("^[a-z][a-z0-9_-]{0,62}$");
  private static final Pattern LABEL_VAL_PATTERN = Pattern.compile("^[a-z0-9_-]{0,63}$");
  private Map<String, String> systemLabels;

  @Override
  public ProvisionerSpecification getSpec() {
    return SPEC;
  }

  @Override
  public void initialize(ProvisionerSystemContext systemContext) {
    Map<String, String> labels = new HashMap<>();
    // dataproc only allows label values to be lowercase letters, numbers, or dashes
    String cdapVersion = systemContext.getCDAPVersion().toLowerCase();
    cdapVersion = cdapVersion.replaceAll("\\.", "_");
    labels.put("cdap-version", cdapVersion);

    String extraLabelsStr = systemContext.getProperties().get("labels");
    // labels are expected to be in format:
    // name1=val1,name2=val2
    if (extraLabelsStr != null) {
      labels.putAll(parseLabels(extraLabelsStr));
    }

    systemLabels = Collections.unmodifiableMap(labels);
  }

  /**
   * Parses labels that are expected to be of the form key1=val1,key2=val2 into a map of key values.
   *
   * If a label key or value is invalid, a message will be logged but the key-value will not be returned in the map.
   * Keys and values cannot be longer than 63 characters.
   * Keys and values can only contain lowercase letters, numeric characters, underscores, and dashes.
   * Keys must start with a lowercase letter and must not be empty.
   *
   * If a label is given without a '=', the label value will be empty.
   * If a label is given as 'key=', the label value will be empty.
   * If a label has multiple '=', it will be ignored. For example, 'key=val1=val2' will be ignored.
   *
   * @param labelsStr the labels string to parse
   * @return valid labels from the parsed string
   */
  @VisibleForTesting
  static Map<String, String> parseLabels(String labelsStr) {
    Splitter labelSplitter = Splitter.on(',').trimResults().omitEmptyStrings();
    Splitter kvSplitter = Splitter.on('=').trimResults().omitEmptyStrings();

    Map<String, String> validLabels = new HashMap<>();
    for (String keyvalue : labelSplitter.split(labelsStr)) {
      Iterator<String> iter = kvSplitter.split(keyvalue).iterator();
      if (!iter.hasNext()) {
        continue;
      }
      String key = iter.next();
      String val = iter.hasNext() ? iter.next() : "";
      if (iter.hasNext()) {
        LOG.info("Ignoring invalid label {}. Labels should be of the form 'key=val' or just 'key'", keyvalue);
        continue;
      }
      if (!LABEL_KEY_PATTERN.matcher(key).matches()) {
        LOG.info("Ignoring invalid label key {}. Label keys cannot be longer than 63 characters, must start with "
                   + "a lowercase letter, and can only contain lowercase letters, numeric characters, underscores,"
                   + " and dashes.", key);
        continue;
      }
      if (!LABEL_VAL_PATTERN.matcher(val).matches()) {
        LOG.info("Ignoring invalid label value {}. Label values cannot be longer than 63 characters, "
                   + "and can only contain lowercase letters, numeric characters, underscores, and dashes.", val);
        continue;
      }
      validLabels.put(key, val);
    }
    return validLabels;
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    DataprocConf.fromProperties(properties);
  }

  @Override
  public Cluster createCluster(ProvisionerContext context) throws Exception {
    // Generates and set the ssh key
    SSHKeyPair sshKeyPair = context.getSSHContext().generate("cdap");
    context.getSSHContext().setSSHKeyPair(sshKeyPair);

    DataprocConf conf = DataprocConf.fromProvisionerContext(context);
    String clusterName = getClusterName(context.getProgramRun());

    try (DataprocClient client = DataprocClient.fromConf(conf)) {
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

      client.createCluster(clusterName, imageVersion, systemLabels);
      return new Cluster(clusterName, ClusterStatus.CREATING, Collections.emptyList(), Collections.emptyMap());
    }
  }

  @Override
  public ClusterStatus getClusterStatus(ProvisionerContext context, Cluster cluster) throws Exception {
    DataprocConf conf = DataprocConf.fromProperties(context.getProperties());
    String clusterName = getClusterName(context.getProgramRun());

    try (DataprocClient client = DataprocClient.fromConf(conf)) {
      return client.getClusterStatus(clusterName);
    }
  }

  @Override
  public Cluster getClusterDetail(ProvisionerContext context,
                                  Cluster cluster) throws Exception {
    DataprocConf conf = DataprocConf.fromProperties(context.getProperties());
    String clusterName = getClusterName(context.getProgramRun());

    try (DataprocClient client = DataprocClient.fromConf(conf)) {
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
      LOG.info("### Trying to create ssh session with {}", host);
      return provisionerContext.getSSHContext().createSSHSession(host);
    } catch (IOException ioe) {
      if (Throwables.getRootCause(ioe) instanceof ConnectException) {
        throw new IOException(String.format(
                "Failed to connect to host %s. Ensure that GCP Firewall Ingress Rules exist that allow ssh " +
                        "on port 22.", host),
                ioe);
      }
      throw ioe;
    }
  }

  @Override
  public void deleteCluster(ProvisionerContext context, Cluster cluster) throws Exception {
    DataprocConf conf = DataprocConf.fromProperties(context.getProperties());
    String clusterName = getClusterName(context.getProgramRun());

    try (DataprocClient client = DataprocClient.fromConf(conf)) {
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
    DataprocConf conf = DataprocConf.fromProperties(context.getProperties());
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

  @Override
  public Capabilities getCapabilities() {
    return new Capabilities(Collections.unmodifiableSet(new HashSet<>(Arrays.asList("fileSet", "externalDataset"))));
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
