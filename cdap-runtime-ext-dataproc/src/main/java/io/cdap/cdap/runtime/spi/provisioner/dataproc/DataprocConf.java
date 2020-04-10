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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Configuration for Dataproc.
 */
final class DataprocConf {

  static final String CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

  static final String PROJECT_ID_KEY = "projectId";
  static final String AUTO_DETECT = "auto-detect";
  static final String NETWORK = "network";
  static final String NETWORK_HOST_PROJECT_ID = "networkHostProjectId";
  static final String PREFER_EXTERNAL_IP = "preferExternalIP";
  static final String STACKDRIVER_LOGGING_ENABLED = "stackdriverLoggingEnabled";
  static final String STACKDRIVER_MONITORING_ENABLED = "stackdriverMonitoringEnabled";
  static final String IMAGE_VERSION = "imageVersion";

  private static final Pattern CLUSTER_PROPERTIES_PATTERN = Pattern.compile("^[a-zA-Z0-9\\-]+:");

  private final String accountKey;
  private final String region;
  private final String zone;
  private final String projectId;
  private final String network;
  private final String networkHostProjectID;
  private final String subnet;
  private final String imageVersion;

  private final int masterNumNodes;
  private final int masterCPUs;
  private final int masterMemoryMB;
  private final int masterDiskGB;

  private final int workerNumNodes;
  private final int workerCPUs;
  private final int workerMemoryMB;
  private final int workerDiskGB;

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
  private final SSHPublicKey publicKey;
  private final Map<String, String> dataprocProperties;

  private final Map<String, String> clusterMetaData;
  private final String networkTags;
  private final String initActions;

  DataprocConf(DataprocConf conf, String network, String subnet) {
    this(conf.accountKey, conf.region, conf.zone, conf.projectId, conf.networkHostProjectID, network, subnet,
         conf.masterNumNodes, conf.masterCPUs, conf.masterMemoryMB, conf.masterDiskGB,
         conf.workerNumNodes, conf.workerCPUs, conf.workerMemoryMB, conf.workerDiskGB,
         conf.pollCreateDelay, conf.pollCreateJitter, conf.pollDeleteDelay, conf.pollInterval,
         conf.encryptionKeyName, conf.gcsBucket, conf.serviceAccount,
         conf.preferExternalIP, conf.stackdriverLoggingEnabled, conf.stackdriverMonitoringEnabled,
         conf.publicKey, conf.imageVersion, conf.clusterMetaData, conf.networkTags, conf.initActions,
         conf.dataprocProperties);
  }

  private DataprocConf(@Nullable String accountKey, String region, String zone, String projectId,
                       @Nullable String networkHostProjectId, @Nullable String network, @Nullable String subnet,
                       int masterNumNodes, int masterCPUs, int masterMemoryMB,
                       int masterDiskGB, int workerNumNodes, int workerCPUs, int workerMemoryMB, int workerDiskGB,
                       long pollCreateDelay, long pollCreateJitter, long pollDeleteDelay, long pollInterval,
                       @Nullable String encryptionKeyName, @Nullable String gcsBucket,
                       @Nullable String serviceAccount, boolean preferExternalIP, boolean stackdriverLoggingEnabled,
                       boolean stackdriverMonitoringEnabled, @Nullable SSHPublicKey publicKey,
                       @Nullable String imageVersion, @Nullable Map<String, String> clusterMetaData,
                       @Nullable String networkTags, @Nullable String initActions,
                       Map<String, String> dataprocProperties) {
    this.accountKey = accountKey;
    this.region = region;
    this.zone = zone;
    this.projectId = projectId;
    this.networkHostProjectID = networkHostProjectId;
    this.network = network;
    this.subnet = subnet;
    this.masterNumNodes = masterNumNodes;
    this.masterCPUs = masterCPUs;
    this.masterMemoryMB = masterMemoryMB;
    this.masterDiskGB = masterDiskGB;
    this.workerNumNodes = workerNumNodes;
    this.workerCPUs = workerCPUs;
    this.workerMemoryMB = workerMemoryMB;
    this.workerDiskGB = workerDiskGB;
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
    this.publicKey = publicKey;
    this.imageVersion = imageVersion;
    this.clusterMetaData = clusterMetaData;
    this.networkTags = networkTags;
    this.initActions = initActions;
    this.dataprocProperties = dataprocProperties;
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

  @Nullable
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

  int getWorkerNumNodes() {
    return workerNumNodes;
  }

  int getWorkerDiskGB() {
    return workerDiskGB;
  }

  String getMasterMachineType() {
    return getMachineType(masterCPUs, masterMemoryMB);
  }

  String getWorkerMachineType() {
    return getMachineType(workerCPUs, workerMemoryMB);
  }

  @Nullable
  String getImageVersion() {
    return imageVersion;
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

  @Nullable
  SSHPublicKey getPublicKey() {
    return publicKey;
  }

  Map<String, String> getClusterMetaData() {
    return clusterMetaData;
  }

  List<String> getNetworkTags() {
    if (Strings.isNullOrEmpty(networkTags)) {
      return Collections.emptyList();
    }
    return Arrays.stream(networkTags.split(","))
      .map(String::trim)
      .collect(Collectors.toList());
  }

  List<String> getInitActions() {
    if (Strings.isNullOrEmpty(initActions)) {
      return Collections.emptyList();
    }
    return Arrays.stream(initActions.split(","))
      .map(String::trim).collect(Collectors.toList());
  }

  Map<String, String> getDataprocProperties() {
    return dataprocProperties;
  }

  /**
   * @return GoogleCredential for use with Compute
   * @throws IOException if there was an error reading the account key
   */
  GoogleCredential getComputeCredential() throws IOException {
    if (accountKey == null) {
      return GoogleCredential.getApplicationDefault();
    }

    try (InputStream is = new ByteArrayInputStream(accountKey.getBytes(StandardCharsets.UTF_8))) {
      return GoogleCredential.fromStream(is)
        .createScoped(Collections.singleton(CLOUD_PLATFORM_SCOPE));
    }
  }

  /**
   * @return GoogleCredentials for use with Dataproc
   * @throws IOException if there was an error reading the account key
   */
  GoogleCredentials getDataprocCredentials() throws IOException {
    if (accountKey == null) {
      return getComputeEngineCredentials();
    }

    try (InputStream is = new ByteArrayInputStream(accountKey.getBytes(StandardCharsets.UTF_8))) {
      return GoogleCredentials.fromStream(is).createScoped(CLOUD_PLATFORM_SCOPE);
    }
  }

  private static GoogleCredentials getComputeEngineCredentials() throws IOException {
    try {
      GoogleCredentials credentials = ComputeEngineCredentials.create();
      credentials.refreshAccessToken();
      return credentials;
    } catch (IOException e) {
      throw new IOException("Unable to get credentials from the environment. "
                              + "Please explicitly set the account key.", e);
    }
  }

  private String getMachineType(int cpus, int memoryGB) {
    // TODO: there are special names for pre-defined cpu and memory
    // for example, 4cpu 3.6gb memory is 'n1-highcpu-4', 4cpu 15gb memory is 'n1-standard-4'
    return String.format("custom-%d-%d", cpus, memoryGB);
  }

  /**
   * Create the conf from a property map while also performing validation.
   *
   * @throws IllegalArgumentException if it is an invalid config
   */
  static DataprocConf fromProperties(Map<String, String> properties) {
    return create(properties, null);
  }

  static DataprocConf create(Map<String, String> properties, @Nullable SSHPublicKey publicKey) {
    String accountKey = getString(properties, "accountKey");
    if (accountKey == null || AUTO_DETECT.equals(accountKey)) {
      try {
        getComputeEngineCredentials();
      } catch (IOException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }
    String projectId = getString(properties, PROJECT_ID_KEY);
    if (projectId == null || AUTO_DETECT.equals(projectId)) {
      projectId = getSystemProjectId();
    }

    String zone = getString(properties, "zone");
    String region = getString(properties, "region");
    if (region == null || AUTO_DETECT.equals(region)) {
      // See if the user specified a zone.
      // If it does, derived region from the provided zone; otherwise, use the system zone.
      if (zone == null || AUTO_DETECT.equals(zone)) {
        region = getRegionFromZone(getSystemZone());
      } else {
        region = getRegionFromZone(zone);
      }
    }

    if (zone == null || AUTO_DETECT.equals(zone)) {
      // Region is always set so that zone can be omitted
      zone = null;
    } else {
      // Make sure the zone provided match with the region
      if (!zone.startsWith(region + "-")) {
        throw new IllegalArgumentException("Provided zone " + zone + " is not in the region " + region);
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
        String.format("Invalid config 'masterNumNodes' = %d. Master nodes must be either 1 or 3.", masterNumNodes));
    }
    int workerNumNodes = getInt(properties, "workerNumNodes", 2);
    if (workerNumNodes == 1) {
      throw new IllegalArgumentException(
        "Invalid config 'workerNumNodes' = 1. Worker nodes must either be zero for a single node cluster, " +
          "or at least 2 for a multi node cluster.");
    }
    // TODO: more extensive validation. Each cpu number has a different allowed memory range
    // for example, 1 cpu requires memory from 3.5gb to 6.5gb in .25gb increments
    // 3 cpu requires memory from 3.6gb to 26gb in .25gb increments
    int masterCPUs = getInt(properties, "masterCPUs", 4);
    int workerCPUs = getInt(properties, "workerCPUs", 4);
    int masterMemoryGB = getInt(properties, "masterMemoryMB", 15 * 1024);
    int workerMemoryGB = getInt(properties, "workerMemoryMB", 15 * 1024);

    int masterDiskGB = getInt(properties, "masterDiskGB", 1000);
    int workerDiskGB = getInt(properties, "workerDiskGB", 1000);

    long pollCreateDelay = getLong(properties, "pollCreateDelay", 60);
    long pollCreateJitter = getLong(properties, "pollCreateJitter", 20);
    long pollDeleteDelay = getLong(properties, "pollDeleteDelay", 30);
    long pollInterval = getLong(properties, "pollInterval", 2);

    String serviceAccount = getString(properties, "serviceAccount");
    boolean preferExternalIP = Boolean.parseBoolean(properties.get(PREFER_EXTERNAL_IP));
    // By default stackdriver is enabled. This is for backward compatibility
    boolean stackdriverLoggingEnabled = Boolean.parseBoolean(properties.getOrDefault(STACKDRIVER_LOGGING_ENABLED,
                                                                                     "true"));
    boolean stackdriverMonitoringEnabled = Boolean.parseBoolean(properties.getOrDefault(STACKDRIVER_MONITORING_ENABLED,
                                                                                        "true"));

    Map<String, String> dataprocProps = Collections.unmodifiableMap(
      properties.entrySet().stream()
        .filter(e -> CLUSTER_PROPERTIES_PATTERN.matcher(e.getKey()).find())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
    );

    String imageVersion = getString(properties, IMAGE_VERSION);
    String gcpCmekKeyName = getString(properties, "encryptionKeyName");
    String gcpCmekBucket = getString(properties, "gcsBucket");

    Map<String, String> clusterMetaData = Collections.unmodifiableMap(
      DataprocUtils.parseKeyValueConfig(getString(properties, "clusterMetaData"),
                                        ";", "\\|"));
    String networkTags = getString(properties, "networkTags");
    String initActions = getString(properties, "initActions");

    return new DataprocConf(accountKey, region, zone, projectId, networkHostProjectID, network, subnet,
                            masterNumNodes, masterCPUs, masterMemoryGB, masterDiskGB,
                            workerNumNodes, workerCPUs, workerMemoryGB, workerDiskGB,
                            pollCreateDelay, pollCreateJitter, pollDeleteDelay, pollInterval,
                            gcpCmekKeyName, gcpCmekBucket, serviceAccount, preferExternalIP,
                            stackdriverLoggingEnabled, stackdriverMonitoringEnabled, publicKey,
                            imageVersion, clusterMetaData, networkTags, initActions, dataprocProps);
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
        String.format("Invalid config '%s' = '%s'. Must be a valid, positive integer.", key, valStr));
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
        String.format("Invalid config '%s' = '%s'. Must be a valid, positive long.", key, valStr));
    }
  }

  /**
   * Get network from the metadata server.
   */
  static String getSystemNetwork() {
    try {
      String network = getMetadata("instance/network-interfaces/0/network");
      // will be something like projects/<project-number>/networks/default
      return network.substring(network.lastIndexOf('/') + 1);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to get the network from the environment. "
                                           + "Please explicitly set the network.", e);
    }
  }

  /**
   * Get zone from the metadata server.
   */
  private static String getSystemZone() {
    try {
      String zone = getMetadata("instance/zone");
      // will be something like projects/<project-number>/zones/us-east1-b
      return zone.substring(zone.lastIndexOf('/') + 1);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to get the zone from the environment. "
                                           + "Please explicitly set the zone.", e);
    }
  }

  /**
   * Returns the region of the given zone.
   */
  private static String getRegionFromZone(String zone) {
    int idx = zone.lastIndexOf("-");
    if (idx <= 0) {
      throw new IllegalArgumentException("Invalid zone. Zone must be in the format of <region>-<zone-name>");
    }
    return zone.substring(0, idx);
  }


  /**
   * Get project id from the metadata server.
   */
  static String getSystemProjectId() {
    try {
      return getMetadata("project/project-id");
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to get project id from the environment. "
                                           + "Please explicitly set the project id and account key.", e);
    }
  }

  /**
   * Makes a request to the metadata server that lives on the VM, as described at
   * https://cloud.google.com/compute/docs/storing-retrieving-metadata.
   */
  private static String getMetadata(String resource) throws IOException {
    URL url = new URL("http://metadata.google.internal/computeMetadata/v1/" + resource);
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestProperty("Metadata-Flavor", "Google");
      connection.connect();
      try (Reader reader = new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8)) {
        return CharStreams.toString(reader);
      }
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }
}
