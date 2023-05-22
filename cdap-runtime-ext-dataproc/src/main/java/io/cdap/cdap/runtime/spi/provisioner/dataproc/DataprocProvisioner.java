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
import com.google.cloud.dataproc.v1.ClusterStatus.State;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.error.api.ErrorTagProvider.ErrorTag;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.runtime.spi.common.DataprocImageVersion;
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
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provisions a cluster using GCP Dataproc.
 */
public class DataprocProvisioner extends AbstractDataprocProvisioner {

  private static final Logger LOG = LoggerFactory.getLogger(DataprocProvisioner.class);
  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
      "gcp-dataproc", "Dataproc",
      "Dataproc is a fast, easy-to-use, fully-managed cloud service for running Apache Spark and Apache "

          + "Hadoop clusters in a simpler, more cost-efficient way on Google Cloud Platform.");
  private static final String CLUSTER_PREFIX = "cdap-";

  // Key which is set to true if the instance only have private ip assigned to it else false
  private static final String PRIVATE_INSTANCE = "privateInstance";

  private static final Pattern NETWORK_TAGS_PATTERN = Pattern.compile(("^[a-z][a-z0-9-]{0,62}$"));

  //A lock to use for cluster reuse
  private static final String REUSE_LOCK = "reuse";

  private final DataprocClientFactory clientFactory;

  @SuppressWarnings("WeakerAccess")
  public DataprocProvisioner() {
    this(new DefaultDataprocClientFactory(new GoogleComputeFactory()));
  }

  @VisibleForTesting
  DataprocProvisioner(DataprocClientFactory clientFactory) {
    super(SPEC);
    this.clientFactory = clientFactory;
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    DataprocConf conf = DataprocConf.create(properties);
    boolean privateInstance = Boolean.parseBoolean(
        getSystemContext().getProperties().get(PRIVATE_INSTANCE));

    if (privateInstance && conf.isPreferExternalIp()) {
      // When prefer external IP is set to true it means only Dataproc external ip can be used to for communication
      // the instance being private instance is incapable of using external ip for communication
      throw new DataprocRuntimeException(
          "The instance is incapable of using external ip for communication with Dataproc cluster. "

              + "Please correct profile configuration by deselecting preferExternalIP.",
          ErrorTag.CONFIGURATION);
    }

    // Validate Network Tags as per https://cloud.google.com/vpc/docs/add-remove-network-tags
    // Total of 64 Tags Allowed
    // Each tag length cannot exceed 63 chars
    // Lower case letters and dashes allowed only.
    List<String> networkTags = conf.getNetworkTags();
    if (!networkTags.stream().allMatch(e -> NETWORK_TAGS_PATTERN.matcher(e).matches())) {
      throw new DataprocRuntimeException("Invalid Network Tags: Ensure tag length is max 63 chars"
          + " and contains lowercase letters, numbers and dashes only. ",
          ErrorTag.CONFIGURATION);
    }

    if (networkTags.size() > 64) {
      throw new DataprocRuntimeException("Exceed Max number of tags. Only Max of 64 allowed. ",
          ErrorTag.CONFIGURATION);
    }

    if (!isAutoscalingFieldsValid(conf, properties)) {
      throw new DataprocRuntimeException(
          String.format("Invalid configs : %s, %s, %s. These are not allowed when %s is enabled ",
              DataprocConf.WORKER_NUM_NODES, DataprocConf.SECONDARY_WORKER_NUM_NODES,
              DataprocConf.AUTOSCALING_POLICY, DataprocConf.PREDEFINED_AUTOSCALE_ENABLED),
          ErrorTag.CONFIGURATION);
    }
  }

  @Override
  public Optional<String> getTotalProcessingCpusLabel(Map<String, String> properties) {
    DataprocConf conf = DataprocConf.create(properties);
    StringBuilder label = new StringBuilder();

    if (conf.isPredefinedAutoScaleEnabled()) {
      label.append(DataprocUtils.WORKER_CPU_PREFIX).append(" ");
    } else if (conf.getAutoScalingPolicy() != null && !conf.getAutoScalingPolicy().isEmpty()) {
      //Return default implementation, as this is an user auto-scaling policy for which we do not have details
      return super.getTotalProcessingCpusLabel(properties);
    }

    int totalCpus =
        conf.getTotalWorkerCpus() > 0 ? conf.getTotalWorkerCpus() : conf.getTotalMasterCpus();
    label.append(totalCpus);
    return Optional.of(label.toString());
  }

  @Override
  public Cluster createCluster(ProvisionerContext context) throws Exception {
    DataprocConf conf = DataprocConf.create(createContextProperties(context));
    if (!isAutoscalingFieldsValid(conf, createContextProperties(context))) {
      LOG.warn("The configs : {}, {}, {} will not be considered when {} is enabled ",
          DataprocConf.WORKER_NUM_NODES,
          DataprocConf.SECONDARY_WORKER_NUM_NODES, DataprocConf.AUTOSCALING_POLICY,
          DataprocConf.PREDEFINED_AUTOSCALE_ENABLED);
    }

    SSHPublicKey sshPublicKey = null;
    if (shouldUseSsh(context, conf)) {
      // Generates and set the ssh key if it does not have one.
      // Since invocation of this method can come from a retry, we don't need to keep regenerating the keys
      SSHContext sshContext = context.getSSHContext();
      if (sshContext != null) {
        SSHKeyPair sshKeyPair = sshContext.getSSHKeyPair().orElse(null);
        if (sshKeyPair == null) {
          sshKeyPair = sshContext.generate("cdap");
          sshContext.setSSHKeyPair(sshKeyPair);
        }
        sshPublicKey = sshKeyPair.getPublicKey();
      }
    }

    try (DataprocClient client = clientFactory.create(conf, sshPublicKey != null)) {
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

      // Check dataproc cluster version if a custom image is not being used
      if (conf.getCustomImageUri() == null) {
        // Determine cluster version and fail if version is smaller than 1.5
        DataprocImageVersion comparableImageVersion = extractVersion(imageVersion);
        if (comparableImageVersion == null) {
          LOG.warn("Unable to extract Dataproc version from string '{}'.", imageVersion);
        } else if (DATAPROC_1_5_VERSION.compareTo(comparableImageVersion) > 0) {
          throw new DataprocRuntimeException(
              "Dataproc cluster must be version 1.5 or greater for pipeline execution.",
              ErrorTag.CONFIGURATION);
        }
      }

      // Reload system context properties and get system labels
      Map<String, String> labels = new HashMap<>();
      labels.putAll(getSystemLabels());
      labels.putAll(getReuseLabels(context, conf));
      labels.putAll(conf.getClusterLabels());
      LOG.info(
          "Creating Dataproc cluster {} in project {}, in region {}, with image {}, with labels {}, endpoint {}",
          clusterName, conf.getProjectId(), conf.getRegion(), imageDescription, labels,
          getRootUrl(conf));

      boolean privateInstance = Boolean.parseBoolean(
          getSystemContext().getProperties().get(PRIVATE_INSTANCE));
      ClusterOperationMetadata createOperationMeta = client.createCluster(clusterName, imageVersion,
          labels, privateInstance, sshPublicKey);
      int numWarnings = createOperationMeta.getWarningsCount();
      if (numWarnings > 0) {
        LOG.warn("Encountered {} warning{} while creating Dataproc cluster:\n{}",
            numWarnings, numWarnings > 1 ? "s" : "",
            String.join("\n", createOperationMeta.getWarningsList()));
      }
      DataprocUtils.emitMetric(context, conf.getRegion(),
          "provisioner.createCluster.response.count");
      return new Cluster(clusterName, ClusterStatus.CREATING, Collections.emptyList(),
          Collections.emptyMap());
    } catch (Exception e) {
      DataprocUtils.emitMetric(context, conf.getRegion(),
          "provisioner.createCluster.response.count", e);
      throw e;
    }
  }

  private Map<? extends String, ? extends String> getReuseLabels(ProvisionerContext context,
      DataprocConf conf) {
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
   *
   * @param client data proc client
   * @param context provisioner contex
   * @param conf dataproc configuration
   * @return a cluster ready to reuse or null if none available.
   */
  @Nullable
  private Cluster tryReuseCluster(DataprocClient client, ProvisionerContext context,
      DataprocConf conf)
      throws RetryableProvisionException, InterruptedException {
    if (!isReuseSupported(conf)) {
      LOG.debug(
          "Not checking cluster reuse, enabled: {}, skip delete: {}, idle ttl: {}, reuse threshold: {}",
          conf.isClusterReuseEnabled(), conf.isSkipDelete(), conf.getIdleTtlMinutes(),
          conf.getClusterReuseThresholdMinutes());
      return null;
    }

    String clusterKey = getRunKey(context);

    for (Stopwatch stopwatch = Stopwatch.createStarted();;) {
      //For idempotency, check if we already have the cluster allocated
      Optional<Cluster> clusterOptional = findCluster(clusterKey, client);
      if (clusterOptional.isPresent()) {
        Cluster cluster = clusterOptional.get();
        if (cluster.getStatus() == ClusterStatus.CREATING
            || cluster.getStatus() == ClusterStatus.RUNNING) {
          LOG.debug("Found allocated cluster {}", cluster.getName());
          return cluster;
        } else {
          LOG.debug("Preallocated cluster {} has expired, will find a new one", cluster.getName());
          //Let's remove the reuse label to ensure new cluster will be picked up by findCluster
          try {
            client.updateClusterLabels(cluster.getName(), Collections.emptyMap(),
                Collections.singleton(LABEL_RUN_KEY));
          } catch (Exception e) {
            LOG.trace("Unable to remove reuse label, cluster may have died already", e);
            if (!LOG.isTraceEnabled()) {
              LOG.debug("Unable to remove reuse label, cluster may have died already");
            }
          }
        }
      }

      AtomicBoolean foundUsedCluster = new AtomicBoolean(false);
      AtomicBoolean foundUpdatingCluster = new AtomicBoolean(false);
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

        Optional<Cluster> cluster = client.getClusters(filter,
            clientCluster -> {
              //Check if cluster is used. We query those only to see if retries make sense
              if (!clientCluster.containsLabels(LABEL_REUSE_UNTIL)) {
                foundUsedCluster.set(true);
                return false;
              }
              if (clientCluster.getStatus().getState() == State.UPDATING) {
                //If there are updating clusters, we retry for longer to allow update to finish
                foundUpdatingCluster.set(true);
                return false;
              }
              if (clientCluster.getStatus().getState() != State.RUNNING) {
                return false;
              }
              //Verify reuse label
              long reuseUntil = Long.parseLong(
                  clientCluster.getLabelsOrDefault(LABEL_REUSE_UNTIL, "0"));
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
          return cluster.get();
        }
      } catch (Exception e) {
        LOG.warn("Error retrieving clusters to reuse, will create a new one", e);
        return null;
      } finally {
        reuseLock.unlock();
      }
      if (foundUpdatingCluster.get()
          && stopwatch.elapsed(TimeUnit.MILLISECONDS) < conf.getClusterReuseUpdateMaxMs()) {
        LOG.trace("A suitable cluster is updating, will retry the poll.");
        Thread.sleep(conf.getClusterReuseRetryDelayMs());
      } else if (foundUsedCluster.get()
          && stopwatch.elapsed(TimeUnit.MILLISECONDS) < conf.getClusterReuseRetryMaxMs()) {
        // With pipelines chained with triggers it's possible that next pipeline starts before
        // previous pipeline cluster was released. To ensure we don't create an extra cluster
        // in such scenario, we retry reuse lookup up to 3 seconds with 1s delay (default).
        LOG.trace("Could not find any available cluster to reuse, will retry.");
        Thread.sleep(conf.getClusterReuseRetryDelayMs());
      } else {
        LOG.debug("Could not find any available cluster to reuse.");
        return null;
      }
    }
  }

  private boolean isReuseSupported(DataprocConf conf) {
    return conf.isClusterReuseEnabled() && conf.isSkipDelete()
        && (conf.getIdleTtlMinutes() <= 0
        || conf.getIdleTtlMinutes() > conf.getClusterReuseThresholdMinutes());
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
      // Dataproc 2.0 is the default version until 6.9.x
      imageVersion = "2.0";
    }
    return imageVersion;
  }

  @Override
  public ClusterStatus getClusterStatus(ProvisionerContext context, Cluster cluster)
      throws Exception {
    DataprocConf conf = DataprocConf.create(createContextProperties(context));
    String clusterName = cluster.getName();

    ClusterStatus status = cluster.getStatus();
    // if we are skipping the delete, need to avoid checking the real cluster status and pretend like it is deleted.
    if (conf.isSkipDelete() && status == ClusterStatus.DELETING) {
      return ClusterStatus.NOT_EXISTS;
    }

    try (DataprocClient client = clientFactory.create(conf)) {
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
  public String getClusterFailureMsg(ProvisionerContext context, Cluster cluster) throws Exception {
    DataprocConf conf = DataprocConf.create(createContextProperties(context));
    String clusterName = cluster.getName();

    try (DataprocClient client = clientFactory.create(conf)) {
      return client.getClusterFailureMsg(clusterName);
    }
  }

  @Override
  public Cluster getClusterDetail(ProvisionerContext context, Cluster cluster) throws Exception {
    DataprocConf conf = DataprocConf.create(createContextProperties(context));
    String clusterName = cluster.getName();
    try (DataprocClient client = clientFactory.create(conf, shouldUseSsh(context, conf))) {
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
  protected void doDeleteCluster(ProvisionerContext context, Cluster cluster, DataprocConf conf)
      throws Exception {
    if (!isReuseSupported(conf) && conf.isSkipDelete()) {
      return;
    }
    String clusterName = cluster.getName();
    try (DataprocClient client = clientFactory.create(conf)) {
      if (isReuseSupported(conf)) {
        long reuseUntil = System.currentTimeMillis()
            + TimeUnit.MINUTES.toMillis(
            conf.getIdleTtlMinutes() - conf.getClusterReuseThresholdMinutes());
        LOG.debug("Marking cluster {} reusable for {} minutes",
            clusterName,
            conf.getIdleTtlMinutes() - conf.getClusterReuseThresholdMinutes());
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
      try (DataprocClient client = clientFactory.create(conf)) {
        Optional<Cluster> allocatedCluster = findCluster(clusterKey, client);
        return allocatedCluster.map(Cluster::getName).orElse(clusterKey);
      }
    }
    return clusterKey;
  }

  private Optional<Cluster> findCluster(String clusterKey, DataprocClient client)
      throws RetryableProvisionException {
    return client.getClusters(Collections.singletonMap(LABEL_RUN_KEY, clusterKey)).findAny();
  }

  @Override
  public PollingStrategy getPollingStrategy(ProvisionerContext context, Cluster cluster) {
    DataprocConf conf = DataprocConf.create(createContextProperties(context));
    PollingStrategy strategy = PollingStrategies.fixedInterval(conf.getPollInterval(),
        TimeUnit.SECONDS);
    switch (cluster.getStatus()) {
      case CREATING:
        return PollingStrategies.initialDelay(strategy, conf.getPollCreateDelay(),
            conf.getPollCreateJitter(), TimeUnit.SECONDS);
      case DELETING:
        return PollingStrategies.initialDelay(strategy, conf.getPollDeleteDelay(),
            TimeUnit.SECONDS);
      default:
        LOG.warn("Received a request to get the polling strategy for unexpected cluster status {}",
            cluster.getStatus());
        return strategy;
    }
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
        DataprocConf.CLUSTER_IDLE_TTL_MINUTES,
        DataprocConf.CLUSTER_REUSE_THRESHOLD_MINUTES
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
   * Gets the identifier to locate cluster for the given context. It's used as a cluster name when
   * reuse is disabled and put into {@link #LABEL_RUN_KEY} when reuse is enabled. Name must start
   * with a lowercase letter followed by up to 51 lowercase letters, numbers, or hyphens, and cannot
   * end with a hyphen We'll use app-runid, where app is truncated to fit, lowercased, and stripped
   * of invalid characters.
   *
   * @param context the provisioner context
   * @return a string that is a valid cluster name
   */
  @VisibleForTesting
  String getRunKey(ProvisionerContext context) {
    ProgramRunInfo programRunInfo = context.getProgramRunInfo();

    String cleanedAppName = programRunInfo.getApplication().replaceAll("[^A-Za-z0-9\\-]", "")
        .toLowerCase();
    // 51 is max length, need to subtract the prefix and 1 extra for the '-' separating app name and run id
    int maxAppLength = 51 - CLUSTER_PREFIX.length() - 1 - programRunInfo.getRun().length();
    if (cleanedAppName.length() > maxAppLength) {
      cleanedAppName = cleanedAppName.substring(0, maxAppLength);
    }
    return CLUSTER_PREFIX + cleanedAppName + "-" + programRunInfo.getRun();
  }

  private boolean shouldUseSsh(ProvisionerContext context, DataprocConf conf) {
    return context.getRuntimeMonitorType() == RuntimeMonitorType.SSH
        || !conf.isRuntimeJobManagerEnabled();
  }

  private boolean isAutoscalingFieldsValid(DataprocConf conf, Map<String, String> properties) {
    if (conf.isPredefinedAutoScaleEnabled()) {
      //If predefined auto-scaling is enabled, then user should not send values for the following
      if (properties.containsKey(DataprocConf.WORKER_NUM_NODES)
          || properties.containsKey(DataprocConf.SECONDARY_WORKER_NUM_NODES)
          || properties.containsKey(DataprocConf.AUTOSCALING_POLICY)) {
        return false;
      }
    }
    return true;
  }

}
