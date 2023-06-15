/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import com.google.gson.annotations.JsonAdapter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Represents the configuration of a namespace. This class needs to be GSON serializable.
 */
@JsonAdapter(NamespaceConfigCodec.class)
public class NamespaceConfig {

  public static final String SCHEDULER_QUEUE_NAME = "scheduler.queue.name";
  public static final String ROOT_DIRECTORY = "root.directory";
  public static final String HBASE_NAMESPACE = "hbase.namespace";
  public static final String HIVE_DATABASE = "hive.database";

  public static final String PRINCIPAL = "principal";
  public static final String GROUP_NAME = "groupName";
  public static final String KEYTAB_URI = "keytabURI";

  private final Map<String, String> configs;

  // scheduler queue name is kept non nullable unlike others like root directory, hbase namespace etc for backward
  // compatibility
  public NamespaceConfig(String schedulerQueueName, @Nullable String rootDirectory,
      @Nullable String hbaseNamespace, @Nullable String hiveDatabase,
      @Nullable String principal, @Nullable String groupName, @Nullable String keytabURI) {
    this(schedulerQueueName, rootDirectory, hbaseNamespace, hiveDatabase, principal, groupName,
        keytabURI,
        new HashMap<>());
  }

  public NamespaceConfig(String schedulerQueueName, @Nullable String rootDirectory,
      @Nullable String hbaseNamespace, @Nullable String hiveDatabase,
      @Nullable String principal, @Nullable String groupName, @Nullable String keytabURI,
      Map<String, String> existingConfigs) {
    Map<String, String> configs = new HashMap<>(existingConfigs);
    configs.put(SCHEDULER_QUEUE_NAME, schedulerQueueName);

    if (rootDirectory != null) {
      configs.put(ROOT_DIRECTORY, rootDirectory);
    }

    if (hbaseNamespace != null) {
      configs.put(HBASE_NAMESPACE, hbaseNamespace);
    }

    if (hiveDatabase != null) {
      configs.put(HIVE_DATABASE, hiveDatabase);
    }

    if (principal != null) {
      configs.put(PRINCIPAL, principal);
    }

    if (groupName != null) {
      configs.put(GROUP_NAME, groupName);
    }

    if (keytabURI != null) {
      configs.put(KEYTAB_URI, keytabURI);
    }

    this.configs = Collections.unmodifiableMap(configs);
  }

  /**
   * Creates a new instance with the given set of configurations.
   */
  public NamespaceConfig(Map<String, String> configs) {
    this.configs = Collections.unmodifiableMap(new HashMap<>(configs));
  }

  public String getSchedulerQueueName() {
    return getConfig(SCHEDULER_QUEUE_NAME);
  }

  @Nullable
  public String getRootDirectory() {
    return getConfig(ROOT_DIRECTORY);
  }

  @Nullable
  public String getHbaseNamespace() {
    return getConfig(HBASE_NAMESPACE);
  }

  @Nullable
  public String getHiveDatabase() {
    return getConfig(HIVE_DATABASE);
  }

  @Nullable
  public String getPrincipal() {
    return getConfig(PRINCIPAL);
  }

  public String getGroupName() {
    return getConfig(GROUP_NAME);
  }

  @Nullable
  public String getKeytabURI() {
    return getConfig(KEYTAB_URI);
  }

  /**
   * @return the keytab URI without the fragment containing version.
   */
  @Nullable
  public String getKeytabUriWithoutVersion() {
    // try to get the keytab URI version if the keytab URI is not null
    String keytabURI = getKeytabURI();
    if (keytabURI != null) {
      // find the position of the fragment containing the version in the keytab URI
      int versionIdx = keytabURI.lastIndexOf("#");
      return versionIdx < 0 ? keytabURI : keytabURI.substring(0, versionIdx);
    }
    return keytabURI;
  }

  /**
   * @return the version of the keytab URI or 0 if no version is present in the keytab URI.
   */
  public int getKeytabUriVersion() {
    // try to get the keytab URI version if the keytab URI is not null
    String keytabURI = getKeytabURI();
    if (keytabURI != null) {
      // find the position where the fragment containing the version in the keytab URI
      int versionIdx = keytabURI.lastIndexOf("#");
      // if the version doesn't exist in the keytab URI, initialize it to 0
      return versionIdx < 0 ? 0 : Integer.parseInt(keytabURI.substring(versionIdx + 1));
    }
    return 0;
  }

  public Set<String> getDifference(@Nullable NamespaceConfig other) {
    if (other == null) {
      // nothing to validate
      return Collections.emptySet();
    }

    Set<String> difference = new HashSet<>();

    if (!Objects.equals(this.getRootDirectory(), other.getRootDirectory())) {
      difference.add(ROOT_DIRECTORY);
    }

    if (!Objects.equals(this.getHbaseNamespace(), other.getHbaseNamespace())) {
      difference.add(HBASE_NAMESPACE);
    }

    if (!Objects.equals(this.getHiveDatabase(), other.getHiveDatabase())) {
      difference.add(HIVE_DATABASE);
    }

    if (!Objects.equals(this.getPrincipal(), other.getPrincipal())) {
      difference.add(PRINCIPAL);
    }

    if (!Objects.equals(this.getGroupName(), other.getGroupName())) {
      difference.add(GROUP_NAME);
    }
    return difference;
  }

  /**
   * Returns the config value of the given name.
   *
   * @param name name of the config
   * @return the value of the config or {@code null} if the config doesn't exist.
   */
  @Nullable
  public String getConfig(String name) {
    return configs.get(name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NamespaceConfig that = (NamespaceConfig) o;
    return Objects.equals(configs, that.configs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(configs);
  }

  @Override
  public String toString() {
    return "NamespaceConfig{"
        + "configs=" + configs
        + '}';
  }

  /**
   * Returns the raw full config map. This is for the {@link NamespaceConfigCodec} to use.
   */
  public Map<String, String> getConfigs() {
    return configs;
  }
}
