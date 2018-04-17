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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.services.compute.Compute;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataproc.v1.ClusterControllerSettings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Map;

/**
 * Configuratino for DataProc.
 */
public class DataProcConf {
  private final String accountKey;
  private final String region;
  private final String zone;
  private final String projectId;
  private final String network;

  private final int masterNumNodes;
  private final int masterCPUs;
  private final int masterMemoryMB;
  private final int masterDiskGB;

  private final int workerNumNodes;
  private final int workerCPUs;
  private final int workerMemoryMB;
  private final int workerDiskGB;

  private DataProcConf(String accountKey, String region, String zone, String projectId, String network,
                       int masterNumNodes, int masterCPUs, int masterMemoryMB, int masterDiskGB,
                       int workerNumNodes, int workerCPUs, int workerMemoryMB, int workerDiskGB) {
    this.accountKey = accountKey;
    this.region = region;
    this.zone = zone;
    this.projectId = projectId;
    this.network = network;
    this.masterNumNodes = masterNumNodes;
    this.masterCPUs = masterCPUs;
    this.masterMemoryMB = masterMemoryMB;
    this.masterDiskGB = masterDiskGB;
    this.workerNumNodes = workerNumNodes;
    this.workerCPUs = workerCPUs;
    this.workerMemoryMB = workerMemoryMB;
    this.workerDiskGB = workerDiskGB;
  }

  public String getRegion() {
    return region;
  }

  public String getZone() {
    return zone;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getNetwork() {
    return network;
  }

  public int getMasterNumNodes() {
    return masterNumNodes;
  }

  public int getMasterDiskGB() {
    return masterDiskGB;
  }

  public int getWorkerNumNodes() {
    return workerNumNodes;
  }

  public int getWorkerDiskGB() {
    return workerDiskGB;
  }

  public String getMasterMachineType() {
    return getMachineType(masterCPUs, masterMemoryMB);
  }

  public String getWorkerMachineType() {
    return getMachineType(workerCPUs, workerMemoryMB);
  }

  private String getMachineType(int cpus, int memoryGB) {
    // TODO: there are special names for pre-defined cpu and memory
    // for example, 4cpu 3.6gb memory is 'n1-highcpu-4', 4cpu 15gb memory is 'n1-standard-4'
    return String.format("custom-%d-%d", cpus, memoryGB);
  }

  public ClusterControllerSettings getControllerSettings() throws IOException {
    CredentialsProvider credentialsProvider;
    try (InputStream is = new ByteArrayInputStream(accountKey.getBytes(StandardCharsets.UTF_8))) {
      credentialsProvider = FixedCredentialsProvider.create(GoogleCredentials.fromStream(is));
    }

    return ClusterControllerSettings.newBuilder()
      .setCredentialsProvider(credentialsProvider)
      .build();
  }

  public Compute getCompute() throws GeneralSecurityException, IOException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

    try (InputStream is = new ByteArrayInputStream(accountKey.getBytes(StandardCharsets.UTF_8))) {
      GoogleCredential credential = GoogleCredential.fromStream(is)
        .createScoped(Collections.singleton("https://www.googleapis.com/auth/cloud-platform"));
      return new Compute.Builder(httpTransport, JacksonFactory.getDefaultInstance(), credential).build();
    }
  }

  /**
   * Create the conf from a property map while also performing validation.
   */
  public static DataProcConf fromProperties(Map<String, String> properties) {
    String accountKey = getString(properties, "accountKey");
    String projectId = getString(properties, "projectId");

    // TODO: validate zone based on the region
    String region = getString(properties, "region", "global");
    String zone = getString(properties, "zone", "us-central1-a");
    String network = getString(properties, "network", "default");

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

    int masterDiskGB = getInt(properties, "masterDiskGB", 500);
    int workerDiskGB = getInt(properties, "workerDiskGB", 500);



    return new DataProcConf(accountKey, region, zone, projectId, network,
                            masterNumNodes, masterCPUs, masterMemoryGB, masterDiskGB,
                            workerNumNodes, workerCPUs, workerMemoryGB, workerDiskGB);
  }

  private static String getString(Map<String, String> properties, String key) {
    String val = properties.get(key);
    if (val == null) {
      throw new IllegalArgumentException(String.format("Invalid config. '%s' must be specified.", key));
    }
    return val;
  }

  private static String getString(Map<String, String> properties, String key, String defaultVal) {
    String val = properties.get(key);
    return val == null ? defaultVal : val;
  }

  private static int getInt(Map<String, String> properties, String key, int defaultVal) {
    String valStr = properties.get(key);
    if (valStr == null) {
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
}
