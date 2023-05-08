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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Strings;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Configuration for Dataproc.
 */
final class DataprocConf {

  static final String CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

  static final String TOKEN_ENDPOINT_KEY = "token.endpoint";
  static final String PROJECT_ID_KEY = "projectId";
  static final String AUTO_DETECT = "auto-detect";
  static final String NETWORK = "network";
  static final String NETWORK_HOST_PROJECT_ID = "networkHostProjectId";
  static final String PREFER_EXTERNAL_IP = "preferExternalIP";
  static final String STACKDRIVER_LOGGING_ENABLED = "stackdriverLoggingEnabled";
  static final String STACKDRIVER_MONITORING_ENABLED = "stackdriverMonitoringEnabled";
  static final String COMPONENT_GATEWAY_ENABLED = "componentGatewayEnabled";
  static final String SKIP_DELETE = "skipDelete";
  static final String IMAGE_VERSION = "imageVersion";
  static final String CUSTOM_IMAGE_URI = "customImageUri";
  static final String ENCRYPTION_KEY_NAME = "encryptionKeyName";
  static final String RUNTIME_JOB_MANAGER = "runtime.job.manager";
  // The property name for the GCE cluster meta data
  // It can be overridden by profile runtime arguments (system.profile.properties.clusterMetaData)
  static final String CLUSTER_META_DATA = "clusterMetaData";
  static final String CLUSTER_LABELS = "clusterLabels";
  // The property name for the serviceAccount that is passed to Dataproc when creating the Dataproc Cluster
  // Dataproc will pass it to GCE when creating the GCE cluster.
  // It can be overridden by profile runtime arguments (system.profile.properties.serviceAccount)
  static final String SERVICE_ACCOUNT = "serviceAccount";
  static final String ROOT_URL = "root.url";

  static final Pattern CLUSTER_PROPERTIES_PATTERN = Pattern.compile("^[a-zA-Z0-9\\-]+:");
  static final int MAX_NETWORK_TAGS = 64;

  static final String SECURE_BOOT_ENABLED = "secureBootEnabled";
  static final String VTPM_ENABLED = "vTpmEnabled";
  static final String INTEGRITY_MONITORING_ENABLED = "integrityMonitoringEnabled";
  // Controls cluster reuse
  static final String CLUSTER_REUSE_ENABLED = "clusterReuseEnabled";
  // If cluster reuse is enabled, defines how much idle time should be left for cluster to be considered reusable
  // E.g. with cluster idle timeout of 5 minutes and threshold of 3 minutes, cluster will be reusable for
  // 2 minutes (5-3) after last job completion
  static final String CLUSTER_REUSE_THRESHOLD_MINUTES = "clusterReuseThresholdMinutes";
  static final int CLUSTER_REUSE_THRESHOLD_MINUTES_DEFAULT = 3;
  /**
   * With pipelines chained with triggers it's possible that next pipeline starts before
   * previous pipeline cluster was released. To ensure we don't create an extra cluster
   * in such scenario, we retry reuse lookup up to 3 seconds with 300ms delay (default).
   */
  static final String CLUSTER_REUSE_RETRY_DELAY_MS = "clusterReuseRetryDelayMs";
  static final long CLUSTER_REUSE_RETRY_DELAY_MS_DEFAULT = 300;
  static final String CLUSTER_REUSE_RETRY_MAX_MS = "clusterReuseRetryMaxMs";
  static final long CLUSTER_REUSE_RETRY_MAX_DEFAULT = 3000;
  static final String CLUSTER_IDLE_TTL_MINUTES = "idleTTL";
  static final int CLUSTER_IDLE_TTL_MINUTES_DEFAULT = 30;
  static final String PREDEFINED_AUTOSCALE_ENABLED = "enablePredefinedAutoScaling";
  static final String WORKER_NUM_NODES = "workerNumNodes";
  static final String SECONDARY_WORKER_NUM_NODES = "secondaryWorkerNumNodes";
  static final String AUTOSCALING_POLICY = "autoScalingPolicy";

  public static final String COMPUTE_HTTP_REQUEST_CONNECTION_TIMEOUT = "compute.request.connection.timeout.millis";
  private static final int COMPUTE_HTTP_REQUEST_CONNECTION_TIMEOUT_DEFAULT = 20000;
  public static final String COMPUTE_HTTP_REQUEST_READ_TIMEOUT = "compute.request.read.timeout.millis";
  private static final int COMPUTE_HTTP_REQUEST_READ_TIMEOUT_DEFAULT = 20000;

  private final String accountKey;
  private final String region;
  private final String zone;
  private final String projectId;
  private final String network;
  private final String networkHostProjectID;
  private final String subnet;
  private final String imageVersion;
  private final String customImageUri;
  private final String rootUrl;

  private final int masterNumNodes;
  private final int masterCPUs;
  private final int masterMemoryMB;
  private final int masterDiskGB;
  private final String masterDiskType;
  private final String masterMachineType;

  private final int workerNumNodes;
  private final int secondaryWorkerNumNodes;
  private final int workerCPUs;
  private final int workerMemoryMB;
  private final int workerDiskGB;
  private final String workerDiskType;
  private final String workerMachineType;

  private final long pollCreateDelay;
  private final long pollCreateJitter;
  private final long pollDeleteDelay;
  private final long pollInterval;

  private final String encryptionKeyName;
  private final String gcsBucket;

  private final String serviceAccount;
  private final boolean preferExternalIP;
  private final boolean stackdriverLoggingEnabled;
  private final boolean stackdriverMonitoringEnabled;
  private final boolean componentGatewayEnabled;
  private final boolean skipDelete;
  private final Map<String, String> clusterProperties;

  private final Map<String, String> clusterMetaData;
  private final Map<String, String> clusterLabels;
  private final List<String> networkTags;
  private final String initActions;
  private final String autoScalingPolicy;
  private final int idleTTLMinutes;

  private final boolean secureBootEnabled;
  private final boolean vTpmEnabled;
  private final boolean integrityMonitoringEnabled;

  private final boolean runtimeJobManagerEnabled;

  private final String tokenEndpoint;

  private final boolean clusterReuseEnabled;
  private final long clusterReuseThresholdMinutes;
  private final long clusterReuseRetryDelayMs;
  private final long clusterReuseRetryMaxMs;
  private final String clusterReuseKey;
  private final boolean enablePredefinedAutoScaling;
  private final boolean disableLocalCaching;

  private final int computeReadTimeout;
  private final int computeConnectionTimeout;

  private final boolean gcsCacheEnabled;
  private final String troubleshootingDocsUrl;

  public String getTroubleshootingDocsUrl() {
    return troubleshootingDocsUrl;
  }

  private DataprocConf(@Nullable String accountKey, String region, String zone, String projectId,
      @Nullable String networkHostProjectId, @Nullable String network, @Nullable String subnet,
      int masterNumNodes, int masterCPUs, int masterMemoryMB,
      int masterDiskGB, String masterDiskType, @Nullable String masterMachineType,
      int workerNumNodes, int secondaryWorkerNumNodes, int workerCPUs, int workerMemoryMB,
      int workerDiskGB, String workerDiskType, @Nullable String workerMachineType,
      long pollCreateDelay, long pollCreateJitter, long pollDeleteDelay, long pollInterval,
      @Nullable String encryptionKeyName, @Nullable String gcsBucket,
      @Nullable String serviceAccount, boolean preferExternalIP, boolean stackdriverLoggingEnabled,
      boolean stackdriverMonitoringEnabled, boolean componentGatewayEnable, boolean skipDelete,
      @Nullable String imageVersion,
      @Nullable String customImageUri,
      @Nullable Map<String, String> clusterMetaData,
      @Nullable Map<String, String> clusterLabels, List<String> networkTags,
      @Nullable String initActions, boolean runtimeJobManagerEnabled,
      Map<String, String> clusterProperties, @Nullable String autoScalingPolicy, int idleTTLMinutes,
      @Nullable String tokenEndpoint, boolean secureBootEnabled, boolean vTpmEnabled,
      boolean integrityMonitoringEnabled, boolean clusterReuseEnabled,
      long clusterReuseThresholdMinutes, long clusterReuseRetryDelayMs, long clusterReuseRetryMaxMs,
      @Nullable String clusterReuseKey,
      boolean enablePredefinedAutoScaling, int computeReadTimeout, int computeConnectionTimeout,
      @Nullable String rootUrl, boolean gcsCacheEnabled, boolean disableLocalCaching,
      String troubleshootingDocsUrl) {
    this.accountKey = accountKey;
    this.region = region;
    this.zone = zone;
    this.projectId = projectId;
    this.clusterReuseEnabled = clusterReuseEnabled;
    this.clusterReuseThresholdMinutes = clusterReuseThresholdMinutes;
    this.clusterReuseRetryDelayMs = clusterReuseRetryDelayMs;
    this.clusterReuseRetryMaxMs = clusterReuseRetryMaxMs;
    this.clusterReuseKey = clusterReuseKey;
    this.networkHostProjectID =
        Strings.isNullOrEmpty(networkHostProjectId) ? projectId : networkHostProjectId;
    this.network = network;
    this.subnet = subnet;
    this.masterNumNodes = masterNumNodes;
    this.masterCPUs = masterCPUs;
    this.masterMemoryMB = masterMemoryMB;
    this.masterDiskGB = masterDiskGB;
    this.masterDiskType = masterDiskType;
    this.masterMachineType = masterMachineType;
    this.workerNumNodes = workerNumNodes;
    this.secondaryWorkerNumNodes = secondaryWorkerNumNodes;
    this.workerCPUs = workerCPUs;
    this.workerMemoryMB = workerMemoryMB;
    this.workerDiskGB = workerDiskGB;
    this.workerDiskType = workerDiskType;
    this.workerMachineType = workerMachineType;
    this.pollCreateDelay = pollCreateDelay;
    this.pollCreateJitter = pollCreateJitter;
    this.pollDeleteDelay = pollDeleteDelay;
    this.pollInterval = pollInterval;
    this.encryptionKeyName = encryptionKeyName;
    this.gcsBucket = gcsBucket;
    this.serviceAccount = serviceAccount;
    this.preferExternalIP = preferExternalIP;
    this.stackdriverLoggingEnabled = stackdriverLoggingEnabled;
    this.stackdriverMonitoringEnabled = stackdriverMonitoringEnabled;
    this.componentGatewayEnabled = componentGatewayEnable;
    this.skipDelete = skipDelete;
    this.imageVersion = imageVersion;
    this.customImageUri = customImageUri;
    this.clusterMetaData = clusterMetaData;
    this.clusterLabels = clusterLabels;
    this.networkTags = networkTags;
    this.initActions = initActions;
    this.runtimeJobManagerEnabled = runtimeJobManagerEnabled;
    this.clusterProperties = clusterProperties;
    this.autoScalingPolicy = autoScalingPolicy;
    this.idleTTLMinutes = idleTTLMinutes;
    this.tokenEndpoint = tokenEndpoint;
    this.secureBootEnabled = secureBootEnabled;
    this.vTpmEnabled = vTpmEnabled;
    this.integrityMonitoringEnabled = integrityMonitoringEnabled;
    this.enablePredefinedAutoScaling = enablePredefinedAutoScaling;
    this.computeReadTimeout = computeReadTimeout;
    this.computeConnectionTimeout = computeConnectionTimeout;
    this.rootUrl = rootUrl;
    this.gcsCacheEnabled = gcsCacheEnabled;
    this.disableLocalCaching = disableLocalCaching;
    this.troubleshootingDocsUrl = troubleshootingDocsUrl;
  }

  String getRegion() {
    return region;
  }

  @Nullable
  String getZone() {
    return zone;
  }

  String getProjectId() {
    return projectId;
  }

  @Nullable
  String getNetwork() {
    return network;
  }

  String getNetworkHostProjectID() {
    return networkHostProjectID;
  }

  @Nullable
  String getSubnet() {
    return subnet;
  }

  int getMasterNumNodes() {
    return masterNumNodes;
  }

  int getMasterDiskGB() {
    return masterDiskGB;
  }

  String getMasterDiskType() {
    return masterDiskType;
  }

  int getTotalMasterCpus() {
    return masterCPUs * masterNumNodes;
  }

  int getWorkerNumNodes() {
    return workerNumNodes;
  }

  int getSecondaryWorkerNumNodes() {
    return secondaryWorkerNumNodes;
  }

  int getWorkerDiskGB() {
    return workerDiskGB;
  }

  String getWorkerDiskType() {
    return workerDiskType;
  }

  String getMasterMachineType() {
    return getMachineType(masterMachineType, masterCPUs, masterMemoryMB);
  }

  String getWorkerMachineType() {
    return getMachineType(workerMachineType, workerCPUs, workerMemoryMB);
  }

  int getTotalWorkerCPUs() {
    if (enablePredefinedAutoScaling) {
      return workerCPUs *
          (PredefinedAutoScaling.getMaxSecondaryWorkerInstances()
              + PredefinedAutoScaling.getPrimaryWorkerInstances());
    }

    return workerCPUs * (workerNumNodes + secondaryWorkerNumNodes);
  }

  @Nullable
  String getImageVersion() {
    return imageVersion;
  }

  @Nullable
  public String getCustomImageUri() {
    return customImageUri;
  }

  long getPollCreateDelay() {
    return pollCreateDelay;
  }

  long getPollCreateJitter() {
    return pollCreateJitter;
  }

  long getPollDeleteDelay() {
    return pollDeleteDelay;
  }

  long getPollInterval() {
    return pollInterval;
  }

  @Nullable
  String getEncryptionKeyName() {
    return encryptionKeyName;
  }

  @Nullable
  String getGcsBucket() {
    return gcsBucket;
  }

  @Nullable
  String getServiceAccount() {
    return serviceAccount;
  }

  boolean isPreferExternalIP() {
    return preferExternalIP;
  }

  boolean isStackdriverLoggingEnabled() {
    return stackdriverLoggingEnabled;
  }

  boolean isStackdriverMonitoringEnabled() {
    return stackdriverMonitoringEnabled;
  }

  boolean isComponentGatewayEnabled() {
    return componentGatewayEnabled;
  }

  public boolean isSkipDelete() {
    return skipDelete;
  }

  Map<String, String> getClusterMetaData() {
    return clusterMetaData;
  }

  Map<String, String> getClusterLabels() {
    return clusterLabels;
  }

  List<String> getNetworkTags() {
    return Collections.unmodifiableList(networkTags);
  }

  List<String> getInitActions() {
    if (Strings.isNullOrEmpty(initActions)) {
      return Collections.emptyList();
    }
    return Arrays.stream(initActions.split(","))
        .map(String::trim).collect(Collectors.toList());
  }

  boolean isRuntimeJobManagerEnabled() {
    return runtimeJobManagerEnabled;
  }

  Map<String, String> getClusterProperties() {
    return clusterProperties;
  }

  @Nullable
  String getAutoScalingPolicy() {
    return autoScalingPolicy;
  }

  public int getIdleTTLMinutes() {
    return idleTTLMinutes;
  }

  @Nullable
  public String getTokenEndpoint() {
    return tokenEndpoint;
  }

  public boolean isSecureBootEnabled() {
    return secureBootEnabled;
  }

  public boolean isvTpmEnabled() {
    return vTpmEnabled;
  }

  public boolean isIntegrityMonitoringEnabled() {
    return integrityMonitoringEnabled;
  }

  public boolean isClusterReuseEnabled() {
    return clusterReuseEnabled;
  }

  public long getClusterReuseThresholdMinutes() {
    return clusterReuseThresholdMinutes;
  }

  public long getClusterReuseRetryDelayMs() {
    return clusterReuseRetryDelayMs;
  }

  public long getClusterReuseRetryMaxMs() {
    return clusterReuseRetryMaxMs;
  }

  public boolean isPredefinedAutoScaleEnabled() {
    return enablePredefinedAutoScaling;
  }

  public boolean isDisableLocalCaching() {
    return disableLocalCaching;
  }

  public int getComputeReadTimeout() {
    return computeReadTimeout;
  }

  public int getComputeConnectionTimeout() {
    return computeConnectionTimeout;
  }

  public String getRootUrl() {
    return rootUrl;
  }

  /**
   * @return a key that should be used along with the profile name to filter clusters with the same
   *     configuration. Always returns a value if cluster reuse is enabled, returns null otherwise.
   */
  @Nullable
  public String getClusterReuseKey() {
    return clusterReuseKey;
  }

  /**
   * @return GoogleCredentials for use with Compute
   * @throws IOException if there was an error reading the account key
   */
  GoogleCredentials getComputeCredential() throws IOException {
    if (accountKey == null) {
      return ComputeEngineCredentials.getOrCreate(tokenEndpoint);
    }

    try (InputStream is = new ByteArrayInputStream(accountKey.getBytes(StandardCharsets.UTF_8))) {
      return GoogleCredentials.fromStream(is)
          .createScoped(Collections.singleton(CLOUD_PLATFORM_SCOPE));
    }
  }

  /**
   * @return GoogleCredentials for use with Dataproc
   * @throws IOException if there was an error reading the account key
   */
  GoogleCredentials getDataprocCredentials() throws IOException {
    if (accountKey == null) {
      return ComputeEngineCredentials.getOrCreate(tokenEndpoint);
    }

    try (InputStream is = new ByteArrayInputStream(accountKey.getBytes(StandardCharsets.UTF_8))) {
      return GoogleCredentials.fromStream(is).createScoped(CLOUD_PLATFORM_SCOPE);
    }
  }

  private String getMachineType(@Nullable String type, int cpus, int memoryMB) {
    // n1 is of format custom-cpu-memory
    // other types are of format type-custom-cpu-memory. For example, n2d-custom-4-16
    String typePrefix = type == null || type.isEmpty() || "n1".equals(type.toLowerCase())
        ? "" : type.toLowerCase() + "-";
    return String.format("%scustom-%d-%d", typePrefix, cpus, memoryMB);
  }

  /**
   * Create the conf from a property map while also performing validation.
   *
   * @throws IllegalArgumentException if it is an invalid config
   */
  static DataprocConf create(Map<String, String> properties) {
    String accountKey = getString(properties, "accountKey");
    if (accountKey == null || AUTO_DETECT.equals(accountKey)) {
      String endPoint = getString(properties, TOKEN_ENDPOINT_KEY);
      try {
        ComputeEngineCredentials.getOrCreate(endPoint);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to get credentials from the environment. "
            + "Please explicitly set the account key.", e);
      }
    }
    String projectId = getString(properties, PROJECT_ID_KEY);
    if (projectId == null || AUTO_DETECT.equals(projectId)) {
      projectId = DataprocUtils.getSystemProjectId();
    }

    String zone = getString(properties, "zone");
    String region = getString(properties, "region");
    if (region == null || AUTO_DETECT.equals(region)) {
      // See if the user specified a zone.
      // If it does, derived region from the provided zone; otherwise, use the system zone.
      if (zone == null || AUTO_DETECT.equals(zone)) {
        region = DataprocUtils.getRegionFromZone(DataprocUtils.getSystemZone());
      } else {
        region = DataprocUtils.getRegionFromZone(zone);
      }
    }

    if (zone == null || AUTO_DETECT.equals(zone)) {
      // Region is always set so that zone can be omitted
      zone = null;
    } else {
      // Make sure the zone provided match with the region
      if (!zone.startsWith(region + "-")) {
        throw new IllegalArgumentException(
            "Provided zone " + zone + " is not in the region " + region);
      }
    }
    String networkHostProjectID = getString(properties, NETWORK_HOST_PROJECT_ID);
    String network = getString(properties, NETWORK);
    if (network == null || AUTO_DETECT.equals(network)) {
      network = null;
    }
    String subnet = getString(properties, "subnet");

    int masterNumNodes = getInt(properties, "masterNumNodes", 1);
    if (masterNumNodes != 1 && masterNumNodes != 3) {
      throw new IllegalArgumentException(
          String.format("Invalid config 'masterNumNodes' = %d. Master nodes must be either 1 or 3.",
              masterNumNodes));
    }

    int workerNumNodes = getInt(properties, WORKER_NUM_NODES, 2);
    int secondaryWorkerNumNodes = getInt(properties, SECONDARY_WORKER_NUM_NODES, 0);
    String autoScalingPolicy = getString(properties, AUTOSCALING_POLICY);

    boolean enablePredefinedAutoScaling =
        Boolean.parseBoolean(properties.getOrDefault(PREDEFINED_AUTOSCALE_ENABLED, "false"));

    if (enablePredefinedAutoScaling) {
      workerNumNodes = PredefinedAutoScaling.getPrimaryWorkerInstances();
      secondaryWorkerNumNodes = PredefinedAutoScaling.getMinSecondaryWorkerInstances();
      autoScalingPolicy = ""; // The policy will be created while cluster provisioning
    }

    if (workerNumNodes == 1) {
      throw new IllegalArgumentException(
          "Invalid config 'workerNumNodes' = 1. Worker nodes must either be zero for a single node cluster, "

              + "or at least 2 for a multi node cluster.");
    }

    if (secondaryWorkerNumNodes < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid config 'secondaryWorkerNumNodes' = %d. The value must be 0 or greater.",
              secondaryWorkerNumNodes));
    }
    // TODO: more extensive validation. Each cpu number has a different allowed memory range
    // for example, 1 cpu requires memory from 3.5gb to 6.5gb in .25gb increments
    // 3 cpu requires memory from 3.6gb to 26gb in .25gb increments
    int masterCPUs = getInt(properties, "masterCPUs", 4);
    int workerCPUs = getInt(properties, "workerCPUs", 4);
    int masterMemoryGB = getInt(properties, "masterMemoryMB", 15 * 1024);
    int workerMemoryGB = getInt(properties, "workerMemoryMB", 15 * 1024);

    int masterDiskGB = getInt(properties, "masterDiskGB", 1000);
    String masterDiskType = getString(properties, "masterDiskType");
    String masterMachineType = getString(properties, "masterMachineType");
    if (masterDiskType == null) {
      masterDiskType = "pd-standard";
    }
    int workerDiskGB = getInt(properties, "workerDiskGB", 1000);
    String workerDiskType = getString(properties, "workerDiskType");
    String workerMachineType = getString(properties, "workerMachineType");
    if (workerDiskType == null) {
      workerDiskType = "pd-standard";
    }

    long pollCreateDelay = getLong(properties, "pollCreateDelay", 60);
    long pollCreateJitter = getLong(properties, "pollCreateJitter", 20);
    long pollDeleteDelay = getLong(properties, "pollDeleteDelay", 30);
    long pollInterval = getLong(properties, "pollInterval", 2);

    String serviceAccount = getString(properties, "serviceAccount");
    boolean preferExternalIP = Boolean.parseBoolean(properties.get(PREFER_EXTERNAL_IP));
    // By default stackdriver is enabled. This is for backward compatibility
    boolean stackdriverLoggingEnabled = Boolean.parseBoolean(
        properties.getOrDefault(STACKDRIVER_LOGGING_ENABLED,
            "true"));
    boolean stackdriverMonitoringEnabled = Boolean.parseBoolean(
        properties.getOrDefault(STACKDRIVER_MONITORING_ENABLED,
            "true"));
    boolean componentGatewayEnabled = Boolean.parseBoolean(
        properties.get(COMPONENT_GATEWAY_ENABLED));
    boolean skipDelete = Boolean.parseBoolean(properties.get(SKIP_DELETE));

    Map<String, String> clusterPropOverrides =
        DataprocUtils.parseKeyValueConfig(getString(properties, "clusterProperties"), ";", "=");

    Map<String, String> clusterProps = properties.entrySet().stream()
        .filter(e -> CLUSTER_PROPERTIES_PATTERN.matcher(e.getKey()).find())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    clusterProps.putAll(clusterPropOverrides);

    String imageVersion = getString(properties, IMAGE_VERSION);
    String customImageUri = getString(properties, CUSTOM_IMAGE_URI);
    String gcpCmekKeyName = getString(properties, ENCRYPTION_KEY_NAME);
    String gcpCmekBucket = getString(properties, "gcsBucket");

    Map<String, String> clusterMetaData = Collections.unmodifiableMap(
        DataprocUtils.parseKeyValueConfig(getString(properties, CLUSTER_META_DATA), ";", "\\|"));

    Map<String, String> clusterLabels = Collections.unmodifiableMap(
        DataprocUtils.parseKeyValueConfig(getString(properties, CLUSTER_LABELS), ";", "\\|"));

    String networkTagsProperty = Optional.ofNullable(getString(properties, "networkTags"))
        .orElse("");
    List<String> networkTags = Collections.unmodifiableList(
        Arrays.stream(networkTagsProperty.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList()));

    if (networkTags.size() > MAX_NETWORK_TAGS) {
      throw new IllegalArgumentException(
          "Number of network tags cannot be more than " + MAX_NETWORK_TAGS);
    }

    String initActions = getString(properties, "initActions");
    boolean runtimeJobManagerEnabled = Boolean.parseBoolean(properties.get(RUNTIME_JOB_MANAGER));
    int idleTTL = getInt(properties, CLUSTER_IDLE_TTL_MINUTES, CLUSTER_IDLE_TTL_MINUTES_DEFAULT);

    String tokenEndpoint = getString(properties, TOKEN_ENDPOINT_KEY);
    boolean secureBootEnabled = Boolean.parseBoolean(
        properties.getOrDefault(SECURE_BOOT_ENABLED, "false"));
    boolean vTpmEnabled = Boolean.parseBoolean(properties.getOrDefault(VTPM_ENABLED, "false"));
    boolean integrityMonitoringEnabled = Boolean.parseBoolean(
        properties.getOrDefault(INTEGRITY_MONITORING_ENABLED, "false"));

    boolean clusterReuseEnabled = Boolean.parseBoolean(
        properties.getOrDefault(CLUSTER_REUSE_ENABLED, "true"));
    int clusterReuseThresholdMinutes = getInt(properties, CLUSTER_REUSE_THRESHOLD_MINUTES,
        CLUSTER_REUSE_THRESHOLD_MINUTES_DEFAULT);
    long clusterReuseRetryDelayMs = getLong(properties, CLUSTER_REUSE_RETRY_DELAY_MS,
        CLUSTER_REUSE_RETRY_DELAY_MS_DEFAULT);
    long clusterReuseRetryMaxMs = getLong(properties, CLUSTER_REUSE_RETRY_MAX_MS,
        CLUSTER_REUSE_RETRY_MAX_DEFAULT);
    String clusterReuseKey = null;
    if (clusterReuseEnabled) {
      try {
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        digest.update(properties.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining(","))
            .getBytes(StandardCharsets.UTF_8)
        );
        clusterReuseKey = String.format("%040x", new BigInteger(1, digest.digest()));
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException("SHA-1 algorithm is not available for cluster reuse", e);
      }
    }
    int computeReadTimeout = getInt(properties, COMPUTE_HTTP_REQUEST_READ_TIMEOUT,
        COMPUTE_HTTP_REQUEST_READ_TIMEOUT_DEFAULT);
    int computeConnectionTimeout = getInt(properties, COMPUTE_HTTP_REQUEST_CONNECTION_TIMEOUT,
        COMPUTE_HTTP_REQUEST_CONNECTION_TIMEOUT_DEFAULT);
    String rootUrl = getString(properties, ROOT_URL);

    // If false, artifacts will not be cached in GCS regardless of cConf setting.
    boolean gcsCacheEnabled = Boolean.parseBoolean(
        properties.getOrDefault(DataprocUtils.GCS_CACHE_ENABLED, "true"));
    // If true, artifacts will not be cached locally.
    boolean disableLocalCaching =
        Boolean.parseBoolean(properties.getOrDefault(DataprocUtils.LOCAL_CACHE_DISABLED, "false"));
    String troubleshootingDocsURL =
        properties.getOrDefault(DataprocUtils.TROUBLESHOOTING_DOCS_URL_KEY,
            DataprocUtils.TROUBLESHOOTING_DOCS_URL_DEFAULT);

    return new DataprocConf(accountKey, region, zone, projectId, networkHostProjectID, network,
        subnet,
        masterNumNodes, masterCPUs, masterMemoryGB, masterDiskGB,
        masterDiskType, masterMachineType,
        workerNumNodes, secondaryWorkerNumNodes, workerCPUs, workerMemoryGB, workerDiskGB,
        workerDiskType, workerMachineType,
        pollCreateDelay, pollCreateJitter, pollDeleteDelay, pollInterval,
        gcpCmekKeyName, gcpCmekBucket, serviceAccount, preferExternalIP,
        stackdriverLoggingEnabled, stackdriverMonitoringEnabled,
        componentGatewayEnabled, skipDelete,
        imageVersion, customImageUri, clusterMetaData, clusterLabels, networkTags,
        initActions, runtimeJobManagerEnabled, clusterProps, autoScalingPolicy, idleTTL,
        tokenEndpoint, secureBootEnabled, vTpmEnabled, integrityMonitoringEnabled,
        clusterReuseEnabled, clusterReuseThresholdMinutes, clusterReuseRetryDelayMs,
        clusterReuseRetryMaxMs, clusterReuseKey,
        enablePredefinedAutoScaling, computeReadTimeout, computeConnectionTimeout, rootUrl,
        gcsCacheEnabled, disableLocalCaching, troubleshootingDocsURL);
  }

  // the UI never sends nulls, it only sends empty strings.
  @Nullable
  private static String getString(Map<String, String> properties, String key) {
    String val = properties.get(key);
    if (val != null && val.isEmpty()) {
      return null;
    }
    return val;
  }

  private static int getInt(Map<String, String> properties, String key, int defaultVal) {
    String valStr = properties.get(key);
    if (valStr == null || valStr.isEmpty()) {
      return defaultVal;
    }
    try {
      int val = Integer.parseInt(valStr);
      if (val < 0) {
        throw new IllegalArgumentException(
            String.format("Invalid config '%s' = '%s'. Must be a positive integer.", key, valStr));
      }
      return val;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Invalid config '%s' = '%s'. Must be a valid, positive integer.", key,
              valStr));
    }
  }

  private static long getLong(Map<String, String> properties, String key, long defaultVal) {
    String valStr = properties.get(key);
    if (valStr == null || valStr.isEmpty()) {
      return defaultVal;
    }
    try {
      long val = Long.parseLong(valStr);
      if (val < 0) {
        throw new IllegalArgumentException(
            String.format("Invalid config '%s' = '%s'. Must be a positive long.", key, valStr));
      }
      return val;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Invalid config '%s' = '%s'. Must be a valid, positive long.", key,
              valStr));
    }
  }
}
