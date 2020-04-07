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
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.provisioner.Capabilities;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategies;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategy;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSystemContext;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocClusterInfo;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocRuntimeJobManager;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobDetail;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import io.cdap.cdap.runtime.spi.ssh.SSHContext;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
    "gcp-dataproc", "Dataproc",
    "Dataproc is a fast, easy-to-use, fully-managed cloud service for running Apache Spark and Apache " +
      "Hadoop clusters in a simpler, more cost-efficient way on Google Cloud Platform.");
  private static final String CLUSTER_PREFIX = "cdap-";

  // Keys for looking up system properties
  private static final String LABELS_PROPERTY = "labels";

  // Key which is set to true if the instance only have private ip assigned to it else false
  private static final String PRIVATE_INSTANCE = "privateInstance";
  // The GCS bucket used by the runtime job manager for launching jobs via the job API
  // It can be overridden by profile runtime arguments (system.profile.properties.bucket)
  private static final String BUCKET = "bucket";
  private static final String RUNTIME_JOB_MANAGER = "runtime.job.manager";

  // keys and values cannot be longer than 63 characters
  // keys and values can only contain lowercase letters, numbers, underscores, and dashes
  // keys must start with a lowercase letter
  // keys cannot be empty
  private static final Pattern LABEL_KEY_PATTERN = Pattern.compile("^[a-z][a-z0-9_-]{0,62}$");
  private static final Pattern LABEL_VAL_PATTERN = Pattern.compile("^[a-z0-9_-]{0,63}$");
  private static final Pattern NETWORK_TAGS_PATTERN = Pattern.compile(("^[a-z][a-z0-9-]{0,62}$"));

  private ProvisionerSystemContext systemContext;

  @Override
  public ProvisionerSpecification getSpec() {
    return SPEC;
  }

  @Override
  public void initialize(ProvisionerSystemContext systemContext) {
    this.systemContext = systemContext;
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
    systemContext.reloadProperties();
    DataprocConf conf = DataprocConf.fromProperties(properties);
    boolean privateInstance = Boolean.parseBoolean(systemContext.getProperties().getOrDefault(PRIVATE_INSTANCE,
                                                                                              "false"));
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
    // Generates and set the ssh key if it does not have one.
    // Since invocation of this method can come from a retry, we don't need to keep regenerating the keys
    SSHContext sshContext = context.getSSHContext();
    SSHPublicKey sshPublicKey = null;
    if (sshContext != null) {
      SSHKeyPair sshKeyPair = sshContext.getSSHKeyPair().orElse(null);
      if (sshKeyPair == null) {
        sshKeyPair = sshContext.generate("cdap");
        sshContext.setSSHKeyPair(sshKeyPair);
      }
      sshPublicKey = sshKeyPair.getPublicKey();
    }

    // Reload system context properties and get system labels
    systemContext.reloadProperties();
    Map<String, String> systemLabels = getSystemLabels(systemContext);

    DataprocConf conf = DataprocConf.create(createContextProperties(context), sshPublicKey);
    String clusterName = getClusterName(context.getProgramRunInfo());

    try (DataprocClient client =
           DataprocClient.fromConf(conf,
                                   Boolean.parseBoolean(systemContext.getProperties().getOrDefault(PRIVATE_INSTANCE,
                                                                                                   "false")))) {
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

      LOG.info("Creating Dataproc cluster {} in project {}, in region {}, with image {}, with system labels {}",
               clusterName, conf.getProjectId(), conf.getRegion(), imageVersion, systemLabels);
      ClusterOperationMetadata createOperationMeta = client.createCluster(clusterName, imageVersion, systemLabels);
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
    DataprocConf conf = DataprocConf.fromProperties(createContextProperties(context));
    String clusterName = getClusterName(context.getProgramRunInfo());
    // Reload system context properties
    systemContext.reloadProperties();
    try (DataprocClient client =
           DataprocClient.fromConf(conf,
                                   Boolean.parseBoolean(systemContext.getProperties().getOrDefault(PRIVATE_INSTANCE,
                                                                                                   "false")))) {
      return client.getClusterStatus(clusterName);
    }
  }

  @Override
  public Cluster getClusterDetail(ProvisionerContext context, Cluster cluster) throws Exception {
    DataprocConf conf = DataprocConf.fromProperties(createContextProperties(context));
    String clusterName = getClusterName(context.getProgramRunInfo());
    // Reload system context properties
    systemContext.reloadProperties();
    try (DataprocClient client =
           DataprocClient.fromConf(conf,
                                   Boolean.parseBoolean(systemContext.getProperties().getOrDefault(PRIVATE_INSTANCE,
                                                                                                   "false")))) {
      Optional<Cluster> existing = client.getCluster(clusterName);
      return existing.orElseGet(() -> new Cluster(cluster, ClusterStatus.NOT_EXISTS));
    }
  }

  @Override
  public void deleteCluster(ProvisionerContext context, Cluster cluster) throws Exception {
    deleteClusterWithStatus(context, cluster);
  }

  @Override
  public ClusterStatus deleteClusterWithStatus(ProvisionerContext context, Cluster cluster) throws Exception {
    // Reload system context properties
    systemContext.reloadProperties();

    Map<String, String> properties = createContextProperties(context);
    DataprocConf conf = DataprocConf.fromProperties(properties);
    RuntimeJobManager jobManager = getRuntimeJobManager(context).orElse(null);

    if (jobManager != null) {
      try {
        // If there is job manager, check to make sure the job is completed.
        // Also cleanup files created by the job run.
        RuntimeJobDetail jobDetail = jobManager.getDetail(context.getProgramRunInfo()).orElse(null);
        if (jobDetail != null && !jobDetail.getStatus().isTerminated()) {
          return ClusterStatus.RUNNING;
        }
      } finally {
        jobManager.close();
      }

      Storage storageClient = StorageOptions.newBuilder().setProjectId(conf.getProjectId())
        .setCredentials(conf.getDataprocCredentials()).build().getService();
      DataprocUtils.deleteGCSPath(storageClient, properties.get(BUCKET),
                                  DataprocUtils.CDAP_GCS_ROOT + "/" + context.getProgramRunInfo().getRun());
    }
    String clusterName = getClusterName(context.getProgramRunInfo());
    try (DataprocClient client =
           DataprocClient.fromConf(conf, Boolean.parseBoolean(systemContext.getProperties().get(PRIVATE_INSTANCE)))) {
      client.deleteCluster(clusterName);
    }
    return ClusterStatus.DELETING;
  }

  @Override
  public PollingStrategy getPollingStrategy(ProvisionerContext context, Cluster cluster) {
    DataprocConf conf = DataprocConf.fromProperties(createContextProperties(context));
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

  private Map<String, String> getSystemLabels(ProvisionerSystemContext systemContext) {
    Map<String, String> labels = new HashMap<>();
    // dataproc only allows label values to be lowercase letters, numbers, or dashes
    String cdapVersion = systemContext.getCDAPVersion().toLowerCase();
    cdapVersion = cdapVersion.replaceAll("\\.", "_");
    labels.put("cdap-version", cdapVersion);

    String extraLabelsStr = systemContext.getProperties().get(LABELS_PROPERTY);
    // labels are expected to be in format:
    // name1=val1,name2=val2
    if (extraLabelsStr != null) {
      labels.putAll(parseLabels(extraLabelsStr));
    }

    return Collections.unmodifiableMap(labels);
  }

  /**
   * Creates properties for the current context. It will default missing values from the system context properties.
   */
  private Map<String, String> createContextProperties(ProvisionerContext context) {
    Map<String, String> contextProperties = new HashMap<>(context.getProperties());

    // Default the project id from system config if missing or if it is auto-detect
    String contextProjectId = contextProperties.get(DataprocConf.PROJECT_ID_KEY);
    if (contextProjectId == null || DataprocConf.AUTO_DETECT.equals(contextProjectId)) {
      contextProjectId = systemContext.getProperties().getOrDefault(DataprocConf.PROJECT_ID_KEY, contextProjectId);
      if (contextProjectId != null) {
        LOG.trace("Setting default Dataproc project ID to {}", contextProjectId);
        contextProperties.put(DataprocConf.PROJECT_ID_KEY, contextProjectId);
      }
    }

    // Default settings from the system context
    List<String> keys = Arrays.asList(DataprocConf.PREFER_EXTERNAL_IP,
                                      DataprocConf.NETWORK,
                                      DataprocConf.NETWORK_HOST_PROJECT_ID,
                                      DataprocConf.STACKDRIVER_LOGGING_ENABLED,
                                      DataprocConf.STACKDRIVER_MONITORING_ENABLED,
                                      DataprocConf.IMAGE_VERSION,
                                      BUCKET);
    for (String key : keys) {
      if (!contextProperties.containsKey(key)) {
        String value = systemContext.getProperties().get(key);
        if (value != null) {
          LOG.trace("Setting default Dataproc property {} to {}", key, value);
          contextProperties.put(key, value.trim());
        }
      }
    }

    // set some dataproc properties that we know will make program execution more stable
    contextProperties.put("dataproc:dataproc.conscrypt.provider.enable", "false");
    contextProperties.put("yarn:yarn.nodemanager.pmem-check-enabled", "false");
    contextProperties.put("yarn:yarn.nodemanager.vmem-check-enabled", "false");

    return contextProperties;
  }

  // Name must start with a lowercase letter followed by up to 51 lowercase letters,
  // numbers, or hyphens, and cannot end with a hyphen
  // We'll use app-runid, where app is truncated to fit, lowercased, and stripped of invalid characters
  @VisibleForTesting
  static String getClusterName(ProgramRunInfo programRunInfo) {
    String cleanedAppName = programRunInfo.getApplication().replaceAll("[^A-Za-z0-9\\-]", "").toLowerCase();
    // 51 is max length, need to subtract the prefix and 1 extra for the '-' separating app name and run id
    int maxAppLength = 51 - CLUSTER_PREFIX.length() - 1 - programRunInfo.getRun().length();
    if (cleanedAppName.length() > maxAppLength) {
      cleanedAppName = cleanedAppName.substring(0, maxAppLength);
    }
    return CLUSTER_PREFIX + cleanedAppName + "-" + programRunInfo.getRun();
  }

  /**
   * Provides implementation of {@link RuntimeJobManager}.
   */
  @Override
  public Optional<RuntimeJobManager> getRuntimeJobManager(ProvisionerContext context) {
    Map<String, String> properties = createContextProperties(context);
    // if this system property is not provided, we will assume that ssh should be used instead of
    // runtime job manager for job launch.
    if (!Boolean.parseBoolean(systemContext.getProperties().get(RUNTIME_JOB_MANAGER))) {
      return Optional.empty();
    }
    DataprocConf conf = DataprocConf.create(properties, null);
    String clusterName = getClusterName(context.getProgramRunInfo());
    String projectId = conf.getProjectId();
    String region = conf.getRegion();
    String bucket = properties.get(BUCKET);

    Map<String, String> systemLabels = getSystemLabels(systemContext);
    try {
      return Optional.of(
        new DataprocRuntimeJobManager(new DataprocClusterInfo(context, clusterName, conf.getDataprocCredentials(),
                                                              DataprocClient.DATAPROC_GOOGLEAPIS_COM_443,
                                                              projectId, region, bucket, systemLabels)));
    } catch (Exception e) {
      throw new RuntimeException("Error while getting credentials for dataproc. ", e);
    }

  }
}
