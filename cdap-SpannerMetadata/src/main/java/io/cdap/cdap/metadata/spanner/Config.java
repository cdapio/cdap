/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.cdap.metadata.spanner;

import io.cdap.cdap.common.conf.CConfiguration;
import java.util.Map;

/**
 * Configuration class for the Spanner metadata storage provider.
 */
public class Config {

  // Configuration keys
  public static final String CONF_SPANNER_PROJECT_ID = "metadata.spanner.project.id";
  public static final String CONF_SPANNER_INSTANCE_ID = "metadata.spanner.instance.id";
  public static final String CONF_SPANNER_DATABASE_ID = "metadata.spanner.database.id";
  public static final String CONF_SPANNER_CONFLICT_NUM_RETRIES = "metadata.spanner.conflict.num.retries";
  public static final String CONF_SPANNER_CONFLICT_RETRY_SLEEP_MS = "metadata.spanner.conflict.retry.sleep.ms";
  public static final String CONF_SPANNER_EMULATOR_HOST = "metadata.spanner.emulator.host";
  public static final String CONF_SPANNER_NUM_CHANNELS = "metadata.spanner.num.channels";
  public static final String CONF_SPANNER_ENDPOINT = "metadata.spanner.endpoint";
  public static final String CONF_SPANNER_READ_TIMEOUT_MS = "metadata.spanner.read.timeout.ms";
  public static final String CONF_SPANNER_WRITE_TIMEOUT_MS = "metadata.spanner.write.timeout.ms";
  public static final String CONF_SPANNER_DDL_TIMEOUT_MS = "metadata.spanner.ddl.timeout.ms";

  // Default values
  public static final int DEFAULT_SPANNER_CONFLICT_NUM_RETRIES = 50;
  public static final int DEFAULT_SPANNER_CONFLICT_RETRY_SLEEP_MS = 100;
  public static final int DEFAULT_SPANNER_READ_TIMEOUT_MS = 60000; // 60 seconds
  public static final int DEFAULT_SPANNER_WRITE_TIMEOUT_MS = 60000; // 60 seconds
  public static final int DEFAULT_SPANNER_DDL_TIMEOUT_MS = 120000; // 120 seconds

  // Instance variables
  private final String projectId;
  private final String instanceId;
  private final String databaseId;
  private final int conflictNumRetries;
  private final int conflictRetrySleepMs;
  private final String emulatorHost;
  private final int numChannels;
  private final String endpoint;
  private final int readTimeoutMs;
  private final int writeTimeoutMs;
  private final int ddlTimeoutMs;

  // Constructors
  public Config(CConfiguration cConfiguration) {
    this.projectId = cConfiguration.get(CONF_SPANNER_PROJECT_ID);
    this.instanceId = cConfiguration.get(CONF_SPANNER_INSTANCE_ID);
    this.databaseId = cConfiguration.get(CONF_SPANNER_DATABASE_ID);
    this.conflictNumRetries = cConfiguration.getInt(CONF_SPANNER_CONFLICT_NUM_RETRIES,
            DEFAULT_SPANNER_CONFLICT_NUM_RETRIES);
    this.conflictRetrySleepMs = cConfiguration.getInt(CONF_SPANNER_CONFLICT_RETRY_SLEEP_MS,
            DEFAULT_SPANNER_CONFLICT_RETRY_SLEEP_MS);
    this.emulatorHost = cConfiguration.get(CONF_SPANNER_EMULATOR_HOST);
    this.numChannels = cConfiguration.getInt(CONF_SPANNER_NUM_CHANNELS);
    this.endpoint = cConfiguration.get(CONF_SPANNER_ENDPOINT);
    this.readTimeoutMs = cConfiguration.getInt(CONF_SPANNER_READ_TIMEOUT_MS, DEFAULT_SPANNER_READ_TIMEOUT_MS);
    this.writeTimeoutMs = cConfiguration.getInt(CONF_SPANNER_WRITE_TIMEOUT_MS, DEFAULT_SPANNER_WRITE_TIMEOUT_MS);
    this.ddlTimeoutMs = cConfiguration.getInt(CONF_SPANNER_DDL_TIMEOUT_MS, DEFAULT_SPANNER_DDL_TIMEOUT_MS);
  }

  public Config(Map<String, String> properties) {
    this.projectId = properties.get(CONF_SPANNER_PROJECT_ID);
    this.instanceId = properties.get(CONF_SPANNER_INSTANCE_ID);
    this.databaseId = properties.get(CONF_SPANNER_DATABASE_ID);
    this.conflictNumRetries = Integer.parseInt(properties.getOrDefault(CONF_SPANNER_CONFLICT_NUM_RETRIES,
            String.valueOf(DEFAULT_SPANNER_CONFLICT_NUM_RETRIES)));
    this.conflictRetrySleepMs = Integer.parseInt(properties.getOrDefault(CONF_SPANNER_CONFLICT_RETRY_SLEEP_MS,
            String.valueOf(DEFAULT_SPANNER_CONFLICT_RETRY_SLEEP_MS)));
    this.emulatorHost = properties.get(CONF_SPANNER_EMULATOR_HOST);
    String numChannelsStr = properties.get(CONF_SPANNER_NUM_CHANNELS);
    this.numChannels = numChannelsStr != null ? Integer.parseInt(numChannelsStr) : 0;
    this.endpoint = properties.get(CONF_SPANNER_ENDPOINT);
    this.readTimeoutMs = Integer.parseInt(properties.getOrDefault(CONF_SPANNER_READ_TIMEOUT_MS,
            String.valueOf(DEFAULT_SPANNER_READ_TIMEOUT_MS)));
    this.writeTimeoutMs = Integer.parseInt(properties.getOrDefault(CONF_SPANNER_WRITE_TIMEOUT_MS,
            String.valueOf(DEFAULT_SPANNER_WRITE_TIMEOUT_MS)));
    this.ddlTimeoutMs = Integer.parseInt(properties.getOrDefault(CONF_SPANNER_DDL_TIMEOUT_MS,
            String.valueOf(DEFAULT_SPANNER_DDL_TIMEOUT_MS)));
  }

  // Getters
  public String getProjectId() {
    return projectId;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public String getDatabaseId() {
    return databaseId;
  }

  public int getConflictNumRetries() {
    return conflictNumRetries;
  }

  public int getConflictRetrySleepMs() {
    return conflictRetrySleepMs;
  }

  public String getEmulatorHost() {
    return emulatorHost;
  }

  public int getNumChannels() {
    return numChannels;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public int getReadTimeoutMs() {
    return readTimeoutMs;
  }

  public int getWriteTimeoutMs() {
    return writeTimeoutMs;
  }

  public int getDdlTimeoutMs() {
    return ddlTimeoutMs;
  }
}