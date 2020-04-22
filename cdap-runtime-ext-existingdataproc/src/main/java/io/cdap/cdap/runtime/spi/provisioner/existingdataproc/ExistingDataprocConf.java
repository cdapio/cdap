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

package io.cdap.cdap.runtime.spi.provisioner.existingdataproc;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.util.Strings;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.io.CharStreams;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Configuration for Dataproc.
 */
final class ExistingDataprocConf {

  static final String CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
  static final String PROJECT_ID_KEY = "projectId";
  static final String AUTO_DETECT = "auto-detect";
  private final String projectId;
  private final String clusterName;
  private final String region;
  private final String accountKey;
  private final SSHKeyPair sshKeyPair;
  private final Map<String, String> labels;
  private static final Pattern LABEL_KEY_PATTERN = Pattern.compile("^[a-z][a-z0-9_-]{0,62}$");
  private static final Pattern LABEL_VAL_PATTERN = Pattern.compile("^[a-z0-9_-]{0,63}$");



  ExistingDataprocConf(ExistingDataprocConf conf) {
    this(conf.projectId, conf.clusterName, conf.region, conf.accountKey, conf.sshKeyPair, conf.labels);
  }

  private ExistingDataprocConf(String projectId, String clusterName, String region, @Nullable String accountKey,
                               @Nullable SSHKeyPair sshKeyPair, Map<String, String> labels) {
    this.projectId = projectId;
    this.clusterName = clusterName;
    this.region = region;
    this.accountKey = accountKey;
    this.sshKeyPair = sshKeyPair;
    this.labels = labels;

  }

  String getProjectId() {
    return projectId;
  }

  String getClusterName() {
    return clusterName;
  }

  String getRegion() {
    return region;
  }

  @Nullable
  String getAccountKey() {
    return accountKey;
  }

  @Nullable
  public SSHKeyPair getKeyPair() {
    return sshKeyPair;
  }

  Map<String, String> getLabels() {
    return labels;

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
  static ExistingDataprocConf fromProperties(Map<String, String> properties) {
    return create(properties);
  }

  static ExistingDataprocConf create(Map<String, String> properties) {
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
    String region = getString(properties, "region");
    if (region == null || AUTO_DETECT.equals(region)) {
      region = getRegionFromZone(getSystemZone());
    }
    String clustername = getString(properties, "clusterName");
    if (clustername == null) {
      throw new IllegalArgumentException("Dataproc ClusterName is mandatory.");
    }

    boolean sshMonitoringEnabled = Boolean.parseBoolean(properties.get("sshMonitoringEnabled"));
    String user = getString(properties, "user");
    if (sshMonitoringEnabled && Strings.isNullOrEmpty(user)) {
      throw new IllegalArgumentException("SSH monitoring is enabled but SSH user not provided for monitoring");

    }

    String privateKey = getString(properties, "sshKey");
    if (sshMonitoringEnabled && Strings.isNullOrEmpty(privateKey)) {
      throw new IllegalArgumentException("SSH monitoring is enabled but SSH key not provided for monitoring");

    }

    SSHKeyPair keyPair = null;
    if (sshMonitoringEnabled) {
      keyPair = new SSHKeyPair(new SSHPublicKey(user, ""), () -> privateKey.getBytes(StandardCharsets.UTF_8));
    }
    Map<String, String> labels;
    String labelsStr = getString(properties, "labels");
    if (Strings.isNullOrEmpty(labelsStr)) {
      labels = Collections.emptyMap();
    } else {
      labels = DataprocUtils.parseKeyValueConfig(labelsStr, ";", "\\|");
      //Validate Label key/values to conform to Requirements
      // https://cloud.google.com/dataproc/docs/guides/creating-managing-labels
      if (labels.size() > 64) {
        throw new IllegalArgumentException("Exceed Max number of labels. Only Max of 64 allowed. ");
      }
      labels.forEach((k, v) -> {
        if (!LABEL_KEY_PATTERN.matcher(k).matches()) {
          throw new IllegalArgumentException("Invalid label key " + k + ". Label key cannot be longer than 63"
                                               + "characters and can only contain lowercase letters, numeric"
                                               + " characters, underscores, and dashes.");
        }
        if (!LABEL_VAL_PATTERN.matcher(v).matches()) {
          throw new IllegalArgumentException("Invalid label value " + v + ". Label values cannot be longer than 63"
                                               + "characters and can only contain lowercase letters, numeric"
                                               + " characters, underscores, and dashes.");
        }
      });
    }
    return new ExistingDataprocConf(projectId, clustername, region, accountKey, keyPair, labels);
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
