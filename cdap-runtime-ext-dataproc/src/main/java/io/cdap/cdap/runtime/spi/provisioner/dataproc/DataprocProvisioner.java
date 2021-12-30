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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategies;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategy;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
import io.cdap.cdap.runtime.spi.ssh.SSHContext;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

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

  //First version spark 3 is default one
  private static final String SPARK3_CDAP_DEFAULT = "6.5";

  //A lock to use for cluster reuse
  private static final String REUSE_LOCK = "reuse";

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

    try (DataprocClient client = getClient(conf)) {
      Cluster reused = tryReuseCluster(client, context, conf);
      if (reused != null) {
        DataprocUtils.emitMetric(context, conf.getRegion(),
                                 "provisioner.createCluster.reuse.count");
        return reused;
      }

      String clusterName = getRunKey(context);

      // if it already exists, it means this is a retry. We can skip actually making the request
      Optional<Cluster> existing = client.getCluster(clusterName);
      if (existing.isPresent()) {
        return existing.get();
      }

      String imageVersion = getImageVersion(context, conf);
      String imageDescription = conf.getCustomImageUri();
      if (imageDescription == null || imageDescription.isEmpty()) {
        imageDescription = imageVersion;
      }

      // Reload system context properties and get system labels
      Map<String, String> labels = new HashMap<>();
      labels.putAll(getSystemLabels());
      labels.putAll(getReuseLabels(context, conf));
      labels.putAll(conf.getClusterLabels());
      LOG.info("Creating Dataproc cluster {} in project {}, in region {}, with image {}, with labels {}",
               clusterName, conf.getProjectId(), conf.getRegion(), imageDescription, labels);

      boolean privateInstance = Boolean.parseBoolean(getSystemContext().getProperties().get(PRIVATE_INSTANCE));
      ClusterOperationMetadata createOperationMeta = client.createCluster(clusterName, imageVersion,
                                                                          labels, privateInstance);
      int numWarnings = createOperationMeta.getWarningsCount();
      if (numWarnings > 0) {
        LOG.warn("Encountered {} warning{} while creating Dataproc cluster:\n{}",
                 numWarnings, numWarnings > 1 ? "s" : "",
                 String.join("\n", createOperationMeta.getWarningsList()));
      }
      DataprocUtils.emitMetric(context, conf.getRegion(),
                               "provisioner.createCluster.response.count");
      return new Cluster(clusterName, ClusterStatus.CREATING, Collections.emptyList(), Collections.emptyMap());
    } catch (Exception e) {
      DataprocUtils.emitMetric(context, conf.getRegion(),
                               "provisioner.createCluster.response.count", e);
      throw e;
    }
  }

  private Map<? extends String, ? extends String> getReuseLabels(ProvisionerContext context, DataprocConf conf) {
    if (!isReuseSupported(conf)) {
      return Collections.emptyMap();
    }
    Map<String, String> reuseLabels = new HashMap<>();
    String normalProfileName = getNormalizedProfileName(context);
    if (normalProfileName != null) {
      reuseLabels.put(LABEL_PROFILE, normalProfileName);
    }
    reuseLabels.put(LABEL_REUSE_KEY, conf.getClusterReuseKey());
    reuseLabels.put(LABEL_RUN_KEY, getRunKey(context));
    return reuseLabels;
  }

  /**
   * If cluster reuse is enabled & possible tries to find a cluster to reuse.
   * @param client data proc client
   * @param context provisioner contex
   * @param conf dataproc configuration
   * @return a cluster ready to reuse or null if none available.
   */
  @Nullable
  private Cluster tryReuseCluster(DataprocClient client, ProvisionerContext context, DataprocConf conf)
    throws RetryableProvisionException, IOException {
    if (!isReuseSupported(conf)) {
      LOG.debug("Not checking cluster reuse, enabled: {}, skip delete: {}, idle ttl: {}, reuse threshold: {}",
                conf.isClusterReuseEnabled(), conf.isSkipDelete(), conf.getIdleTTLMinutes(),
                conf.getClusterReuseThresholdMinutes());
      return null;
    }

    String clusterKey = getRunKey(context);

    //For idempotency, check if we already have the cluster allocated
    Optional<Cluster> clusterOptional = findCluster(clusterKey, client);
    if (clusterOptional.isPresent()) {
      Cluster cluster = clusterOptional.get();
      if (cluster.getStatus() == ClusterStatus.CREATING ||
        cluster.getStatus() == ClusterStatus.RUNNING) {
        LOG.debug("Found allocated cluster {}", cluster.getName());
        return cluster;
      } else {
        LOG.debug("Preallocated cluster {} has expired, will find a new one");
        //Let's remove the reuse label to ensure new cluster will be picked up by findCluster
        try {
          client.updateClusterLabels(cluster.getName(), Collections.emptyMap(), Collections.singleton(LABEL_RUN_KEY));
        } catch (Exception e) {
          LOG.trace("Unable to remove reuse label, cluster may have died already", e);
          if (!LOG.isTraceEnabled()) {
            LOG.debug("Unable to remove reuse label, cluster may have died already");
          }
        }
      }
    }

    Lock reuseLock = getSystemContext().getLock(REUSE_LOCK);
    reuseLock.lock();
    try {
      Map<String, String> filter = new HashMap<>();
      String normalizedProfileName = getNormalizedProfileName(context);
      if (normalizedProfileName != null) {
        filter.put(LABEL_PROFILE, normalizedProfileName);
      }
      filter.put(LABEL_VERSON, getVersionLabel());
      filter.put(LABEL_REUSE_KEY, conf.getClusterReuseKey());
      filter.put(LABEL_REUSE_UNTIL, "*");

      Optional<Cluster> cluster = client.getClusters(ClusterStatus.RUNNING, filter, clientCluster -> {
        //Verify reuse label
        long reuseUntil = Long.valueOf(clientCluster.getLabelsOrDefault(LABEL_REUSE_UNTIL, "0"));
        long now = System.currentTimeMillis();
        if (reuseUntil < now) {
          LOG.debug("Skipping expired cluster {}, reuse until {} is before now {}",
                    clientCluster.getClusterName(), reuseUntil, now);
          return false;
        }
        return true;
      }).findAny();

      if (cluster.isPresent()) {
        String clusterName = cluster.get().getName();
        LOG.info("Found cluster to reuse: {}", clusterName);
        // Add cdap-reuse-for to find cluster later if needed
        // And remove reuseUntil to indicate the cluster is taken
        client.updateClusterLabels(clusterName,
                                   Collections.singletonMap(LABEL_RUN_KEY, clusterKey),
                                   Collections.singleton(LABEL_REUSE_UNTIL)
        );
      } else {
        LOG.debug("Could not find any available cluster to reuse.");
      }
      return cluster.orElse(null);
    } catch (Exception e) {
      LOG.warn("Error retrieving clusters to reuse, will create a new one", e);
      return null;
    } finally {
      reuseLock.unlock();
    }
  }

  private boolean isReuseSupported(DataprocConf conf) {
    return conf.isClusterReuseEnabled() && conf.isSkipDelete() &&
      conf.getIdleTTLMinutes() > conf.getClusterReuseThresholdMinutes();
  }

  @Nullable
  private String getNormalizedProfileName(ProvisionerContext context) {
    if (context.getProfileName() == null) {
      return null;
    }
    String normalName = context.getProfileName().replaceAll("[^a-zA-Z0-9]", "");
    return normalName.isEmpty() ? null : normalName;
  }

  @VisibleForTesting
  String getImageVersion(ProvisionerContext context, DataprocConf conf) {
    String imageVersion = conf.getImageVersion();
    if (imageVersion == null) {
      switch (context.getSparkCompat()) {
        case SPARK3_2_12:
          imageVersion = "2.0";
          break;
        case SPARK2_2_11:
        default:
          if (context.getAppCDAPVersionInfo() == null ||
            context.getAppCDAPVersionInfo().compareTo(SPARK3_CDAP_DEFAULT) < 0) {
            imageVersion = "1.3";
          } else {
            imageVersion = "2.0";
          }
          break;
      }
    }
    return imageVersion;
  }

  @Override
  public ClusterStatus getClusterStatus(ProvisionerContext context, Cluster cluster) throws Exception {
    DataprocConf conf = DataprocConf.create(createContextProperties(context));
    String clusterName = cluster.getName();

    ClusterStatus status = cluster.getStatus();
    // if we are skipping the delete, need to avoid checking the real cluster status and pretend like it is deleted.
    if (conf.isSkipDelete() && status == ClusterStatus.DELETING) {
      return ClusterStatus.NOT_EXISTS;
    }

    try (DataprocClient client = getClient(conf)) {
      status = client.getClusterStatus(clusterName);
      DataprocUtils.emitMetric(context, conf.getRegion(),
                               "provisioner.clusterStatus.response.count");
      return status;
    } catch (Exception e) {
      DataprocUtils.emitMetric(context, conf.getRegion(),
                               "provisioner.clusterStatus.response.count", e);
      throw e;
    }
  }

  @Override
  public Cluster getClusterDetail(ProvisionerContext context, Cluster cluster) throws Exception {
    DataprocConf conf = DataprocConf.create(createContextProperties(context));
    String clusterName = cluster.getName();
    try (DataprocClient client = getClient(conf)) {
      Optional<Cluster> existing = client.getCluster(clusterName);
      DataprocUtils.emitMetric(context, conf.getRegion(),
                               "provisioner.clusterDetail.response.count");
      return existing.orElseGet(() -> new Cluster(cluster, ClusterStatus.NOT_EXISTS));
    } catch (Exception e) {
      DataprocUtils.emitMetric(context, conf.getRegion(),
                               "provisioner.clusterDetail.response.count", e);
      throw e;
    }
  }

  @Override
  protected void doDeleteCluster(ProvisionerContext context, Cluster cluster, DataprocConf conf) throws Exception {
    if (!isReuseSupported(conf) && conf.isSkipDelete()) {
      return;
    }
    String clusterName = cluster.getName();
    try (DataprocClient client = getClient(conf)) {
      if (isReuseSupported(conf)) {
        long reuseUntil = System.currentTimeMillis() +
          TimeUnit.MINUTES.toMillis(conf.getIdleTTLMinutes() - conf.getClusterReuseThresholdMinutes());
        LOG.debug("Marking cluster {} reusable for {} minutes",
                  clusterName,
                  conf.getIdleTTLMinutes() - conf.getClusterReuseThresholdMinutes());
        client.updateClusterLabels(clusterName,
                                   //Add reuse until
                                   Collections.singletonMap(LABEL_REUSE_UNTIL, Long.toString(reuseUntil)),
                                   //Drop allocation
                                   Collections.singleton(LABEL_RUN_KEY));
      } else {
        client.deleteCluster(clusterName);
      }
      DataprocUtils.emitMetric(context, conf.getRegion(),
                               "provisioner.deleteCluster.response.count");
    } catch (Exception e) {
      DataprocUtils.emitMetric(context, conf.getRegion(),
                               "provisioner.deleteCluster.response.count", e);
      throw e;
    }
  }

  @Override
  protected String getClusterName(ProvisionerContext context) throws Exception {
    DataprocConf conf = DataprocConf.create(createContextProperties(context));
    String clusterKey = getRunKey(context);
    if (isReuseSupported(conf)) {
      try (DataprocClient client = getClient(conf)) {
        Optional<Cluster> allocatedCluster = findCluster(clusterKey, client);
        return allocatedCluster.map(Cluster::getName).orElse(clusterKey);
      }
    }
    return clusterKey;
  }

  @VisibleForTesting
  protected DataprocClient getClient(DataprocConf conf) throws IOException, GeneralSecurityException,
    RetryableProvisionException {
    return DataprocClient.fromConf(conf);
  }

  private Optional<Cluster> findCluster(String clusterKey, DataprocClient client)
    throws RetryableProvisionException {
    return client.getClusters(null, Collections.singletonMap(LABEL_RUN_KEY, clusterKey)).findAny();
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
      DataprocConf.COMPONENT_GATEWAY_ENABLED,
      DataprocConf.IMAGE_VERSION,
      DataprocConf.CLUSTER_META_DATA,
      DataprocConf.SERVICE_ACCOUNT,
      DataprocConf.CLUSTER_IDLE_TTL_MINUTES
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
   * Gets the identifier to locate cluster for the given context.
   * It's used as a cluster name when reuse is disabled and put into {@link #LABEL_RUN_KEY} when reuse is enabled.
   * Name must start with a lowercase letter followed by up to 51 lowercase letters,
   * numbers, or hyphens, and cannot end with a hyphen
   * We'll use app-runid, where app is truncated to fit, lowercased, and stripped of invalid characters.
   *
   * @param context the provisioner context
   * @return a string that is a valid cluster name
   */
  @VisibleForTesting
  String getRunKey(ProvisionerContext context) {
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
