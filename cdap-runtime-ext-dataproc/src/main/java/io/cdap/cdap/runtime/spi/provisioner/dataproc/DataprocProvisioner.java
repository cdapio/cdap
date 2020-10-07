/*
 * Copyright © 2018-2020 Cask Data, Inc.
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

import com.google.cloud.dataproc.v1.ClusterOperationMetadata;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategies;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategy;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import io.cdap.cdap.runtime.spi.ssh.SSHContext;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Provisions a cluster using GCP Dataproc.
 */
public class DataprocProvisioner extends AbstractDataprocProvisioner {

  private static final Logger LOG = LoggerFactory.getLogger(DataprocProvisioner.class);
  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
    "gcp-dataproc", "Dataproc",
    "Dataproc is a fast, easy-to-use, fully-managed cloud service for running Apache Spark and Apache " +
      "Hadoop clusters in a simpler, more cost-efficient way on Google Cloud Platform.");
  private static final String CLUSTER_PREFIX = "cdap-";

  // Key which is set to true if the instance only have private ip assigned to it else false
  private static final String PRIVATE_INSTANCE = "privateInstance";

  private static final Pattern NETWORK_TAGS_PATTERN = Pattern.compile(("^[a-z][a-z0-9-]{0,62}$"));

  @SuppressWarnings("WeakerAccess")
  public DataprocProvisioner() {
    super(SPEC);
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    DataprocConf conf = DataprocConf.create(properties);
    boolean privateInstance = Boolean.parseBoolean(getSystemContext().getProperties().get(PRIVATE_INSTANCE));

    if (privateInstance && conf.isPreferExternalIP()) {
      // When prefer external IP is set to true it means only Dataproc external ip can be used to for communication
      // the instance being private instance is incapable of using external ip for communication
      throw new IllegalArgumentException("The instance is incapable of using external ip for communication " +
                                           "with Dataproc cluster. Please correct profile configuration by " +
                                           "deselecting preferExternalIP.");
    }

    // Validate Network Tags as per https://cloud.google.com/vpc/docs/add-remove-network-tags
    // Total of 64 Tags Allowed
    // Each tag length cannot exceed 63 chars
    // Lower case letters and dashes allowed only.
    List<String> networkTags = conf.getNetworkTags();
    if (!networkTags.stream().allMatch(e -> NETWORK_TAGS_PATTERN.matcher(e).matches())) {
      throw new IllegalArgumentException("Invalid Network Tags: Ensure tag length is max 63 chars"
                                           + " and contains  lowercase letters, numbers and dashes only. ");
    }
    if (networkTags.size() > 64) {
      throw new IllegalArgumentException("Exceed Max number of tags. Only Max of 64 allowed. ");
    }
  }

  @Override
  public Cluster createCluster(ProvisionerContext context) throws Exception {
    DataprocConf conf = DataprocConf.create(createContextProperties(context));

    if (context.getRuntimeMonitorType() == RuntimeMonitorType.SSH || !conf.isRuntimeJobManagerEnabled()) {
      // Generates and set the ssh key if it does not have one.
      // Since invocation of this method can come from a retry, we don't need to keep regenerating the keys
      SSHContext sshContext = context.getSSHContext();
      if (sshContext != null) {
        SSHKeyPair sshKeyPair = sshContext.getSSHKeyPair().orElse(null);
        if (sshKeyPair == null) {
          sshKeyPair = sshContext.generate("cdap");
          sshContext.setSSHKeyPair(sshKeyPair);
        }
        conf = DataprocConf.create(createContextProperties(context), sshKeyPair.getPublicKey());
      }
    }

    String clusterName = getClusterName(context);

    try (DataprocClient client = DataprocClient.fromConf(conf)) {
      // if it already exists, it means this is a retry. We can skip actually making the request
      Optional<Cluster> existing = client.getCluster(clusterName);
      if (existing.isPresent()) {
        return existing.get();
      }

      String imageVersion = conf.getImageVersion();
      if (imageVersion == null) {
        switch (context.getSparkCompat()) {
          case SPARK1_2_10:
            imageVersion = "1.0";
            break;
          case SPARK2_2_11:
          default:
            imageVersion = "1.3";
            break;
        }
      }

      // Reload system context properties and get system labels
      Map<String, String> systemLabels = getSystemLabels();
      LOG.info("Creating Dataproc cluster {} in project {}, in region {}, with image {}, with system labels {}",
               clusterName, conf.getProjectId(),
               conf.getRegion(), imageVersion, systemLabels);

      boolean privateInstance = Boolean.parseBoolean(getSystemContext().getProperties().get(PRIVATE_INSTANCE));
      ClusterOperationMetadata createOperationMeta = client.createCluster(clusterName, imageVersion,
                                                                          systemLabels, privateInstance);
      int numWarnings = createOperationMeta.getWarningsCount();
      if (numWarnings > 0) {
        LOG.warn("Encountered {} warning{} while creating Dataproc cluster:\n{}",
                 numWarnings, numWarnings > 1 ? "s" : "",
                 String.join("\n", createOperationMeta.getWarningsList()));
      }
      return new Cluster(clusterName, ClusterStatus.CREATING, Collections.emptyList(), Collections.emptyMap());
    }
  }

  @Override
  public ClusterStatus getClusterStatus(ProvisionerContext context, Cluster cluster) throws Exception {
    DataprocConf conf = DataprocConf.create(createContextProperties(context));
    String clusterName = getClusterName(context);
    try (DataprocClient client = DataprocClient.fromConf(conf)) {
      return client.getClusterStatus(clusterName);
    }
  }

  @Override
  public Cluster getClusterDetail(ProvisionerContext context, Cluster cluster) throws Exception {
    DataprocConf conf = DataprocConf.create(createContextProperties(context));
    String clusterName = getClusterName(context);
    try (DataprocClient client = DataprocClient.fromConf(conf)) {
      Optional<Cluster> existing = client.getCluster(clusterName);
      return existing.orElseGet(() -> new Cluster(cluster, ClusterStatus.NOT_EXISTS));
    }
  }

  @Override
  protected void doDeleteCluster(ProvisionerContext context, Cluster cluster, DataprocConf conf) throws Exception {
    String clusterName = getClusterName(context);
    try (DataprocClient client = DataprocClient.fromConf(conf)) {
      client.deleteCluster(clusterName);
    }
  }

  @Override
  public PollingStrategy getPollingStrategy(ProvisionerContext context, Cluster cluster) {
    DataprocConf conf = DataprocConf.create(createContextProperties(context));
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
  protected boolean isDefaultContextProperty(String property) {
    if (super.isDefaultContextProperty(property)) {
      return true;
    }
    return ImmutableSet.of(
      DataprocConf.PREFER_EXTERNAL_IP,
      DataprocConf.NETWORK,
      DataprocConf.NETWORK_HOST_PROJECT_ID,
      DataprocConf.STACKDRIVER_LOGGING_ENABLED,
      DataprocConf.STACKDRIVER_MONITORING_ENABLED,
      DataprocConf.IMAGE_VERSION,
      DataprocConf.SERVICE_ACCOUNT
    ).contains(property);
  }

  @Override
  protected Map<String, String> getDefaultContextProperties() {
    Map<String, String> properties = new HashMap<>(super.getDefaultContextProperties());

    // set some dataproc properties that we know will make program execution more stable
    properties.put("dataproc:dataproc.conscrypt.provider.enable", "false");
    properties.put("yarn:yarn.nodemanager.pmem-check-enabled", "false");
    properties.put("yarn:yarn.nodemanager.vmem-check-enabled", "false");

    // The additional property is needed to be able to provision a singlenode cluster on
    // dataproc. Dataproc has an issue that it will treat 0 number of worker
    // nodes as the default number, which means it will always provision a
    // cluster with 2 worker nodes if this property is not set. Refer to
    // https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/single-node-clusters
    // for more information.
    properties.put("dataproc:dataproc.allow.zero.workers", "true");

    // Retain logs
    properties.put("yarn:yarn.nodemanager.delete.debug-delay-sec", "86400");

    return properties;
  }

  /**
   * Gets the cluster name for the given context.
   * Name must start with a lowercase letter followed by up to 51 lowercase letters,
   * numbers, or hyphens, and cannot end with a hyphen
   * We'll use app-runid, where app is truncated to fit, lowercased, and stripped of invalid characters.
   *
   * @param context the provisioner context
   * @return a string that is a valid cluster name
   */
  @Override
  protected String getClusterName(ProvisionerContext context) {
    ProgramRunInfo programRunInfo = context.getProgramRunInfo();
    String cleanedAppName = programRunInfo.getApplication().replaceAll("[^A-Za-z0-9\\-]", "").toLowerCase();
    // 51 is max length, need to subtract the prefix and 1 extra for the '-' separating app name and run id
    int maxAppLength = 51 - CLUSTER_PREFIX.length() - 1 - programRunInfo.getRun().length();
    if (cleanedAppName.length() > maxAppLength) {
      cleanedAppName = cleanedAppName.substring(0, maxAppLength);
    }
    return CLUSTER_PREFIX + cleanedAppName + "-" + programRunInfo.getRun();
  }
}
