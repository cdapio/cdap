/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

import io.cdap.cdap.proto.id.NamespaceId;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents metadata for namespaces
 */
public final class NamespaceMeta {

  public static final NamespaceMeta DEFAULT =
      new NamespaceMeta.Builder()
          .setName(NamespaceId.DEFAULT)
          .setIdentity(NamespaceId.DEFAULT.getNamespace())
          .setDescription(
              "This is the default namespace, which is automatically created, and is always available.")
          .build();

  public static final NamespaceMeta SYSTEM =
      new NamespaceMeta.Builder()
          .setName(NamespaceId.SYSTEM)
          .setIdentity(NamespaceId.SYSTEM.getNamespace())
          .setDescription("The system namespace, which is used for internal purposes.")
          .build();

  private final String name;
  private final String identity;
  private final String description;
  private final long generation;
  private final NamespaceConfig config;

  private NamespaceMeta(String name, String description, long generation, NamespaceConfig config,
      String identity) {
    this.name = name;
    this.description = description;
    this.generation = generation;
    this.config = config;
    this.identity = identity;
  }

  public String getName() {
    return name;
  }

  public String getIdentity() {
    return identity;
  }

  public String getDescription() {
    return description;
  }

  /**
   * Get the namespace generation. The generation is set when the namespace is created. If the
   * namespace is deleted and then created again, it will have a higher generation.
   *
   * @return the namespace generation
   */
  public long getGeneration() {
    return generation;
  }

  public NamespaceConfig getConfig() {
    return config;
  }

  /**
   * Builder used to build {@link NamespaceMeta}
   */
  public static final class Builder {

    private String name;
    private String description;
    private String identity;
    private String schedulerQueueName;
    private String rootDirectory;
    private String hbaseNamespace;
    private String hiveDatabase;
    private String principal;
    private String groupName;
    private String keytabUriWithoutVersion;
    private int keytabUriVersion;
    private long generation;
    private Map<String, String> configMap = new HashMap<>();

    public Builder() {
      // No-Op
    }

    /**
     * Constructs the namespace meta instance.
     * @param meta {@link NamespaceMeta}
     */
    public Builder(NamespaceMeta meta) {
      this.name = meta.getName();
      this.description = meta.getDescription();
      this.generation = meta.getGeneration();
      this.identity = meta.getIdentity();
      NamespaceConfig config = meta.getConfig();
      if (config != null) {
        this.configMap = config.getConfigs();
        this.schedulerQueueName = config.getSchedulerQueueName();
        this.rootDirectory = config.getRootDirectory();
        this.hbaseNamespace = config.getHbaseNamespace();
        this.hiveDatabase = config.getHiveDatabase();
        this.principal = config.getPrincipal();
        this.groupName = config.getGroupName();
        this.keytabUriWithoutVersion = config.getKeytabUriWithoutVersion();
        this.keytabUriVersion = config.getKeytabUriVersion();
      }
    }

    public Builder setName(NamespaceId id) {
      this.name = id.getNamespace();
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    public Builder setIdentity(String identity) {
      this.identity = identity;
      return this;
    }

    public Builder setSchedulerQueueName(String schedulerQueueName) {
      this.schedulerQueueName = schedulerQueueName;
      return this;
    }

    public Builder setRootDirectory(final String hdfsDirectory) {
      this.rootDirectory = hdfsDirectory;
      return this;
    }

    public Builder setHBaseNamespace(final String hbaseNamespace) {
      this.hbaseNamespace = hbaseNamespace;
      return this;
    }

    public Builder setHiveDatabase(final String hiveDatabase) {
      this.hiveDatabase = hiveDatabase;
      return this;
    }

    public Builder setPrincipal(String principal) {
      this.principal = principal;
      return this;
    }

    public Builder setGroupName(String groupName) {
      this.groupName = groupName;
      return this;
    }

    public Builder setKeytabUri(String keytabUri) {
      this.keytabUriWithoutVersion = keytabUri;
      this.keytabUriVersion = 0;
      return this;
    }

    public Builder setKeytabUriWithoutVersion(String keytabUriWithoutVersion) {
      this.keytabUriWithoutVersion = keytabUriWithoutVersion;
      return this;
    }

    public Builder incrementKeytabUriVersion() {
      keytabUriVersion++;
      return this;
    }

    public void setKeytabUriVersion(int keytabUriVersion) {
      this.keytabUriVersion = keytabUriVersion;
    }

    public Builder setGeneration(long generation) {
      this.generation = generation;
      return this;
    }

    public Builder setConfig(Map<String, String> configMap) {
      this.configMap = configMap;
      return this;
    }

    public NamespaceMeta buildWithoutKeytabUriVersion() {
      return build(keytabUriWithoutVersion);
    }

    /**
     * Builds the namespace metadata.
     *
     * @return {@link NamespaceMeta}.
     */
    public NamespaceMeta build() {
      // combine the keytab Uri with the version if the version is not 0
      String uri = keytabUriVersion == 0 ? keytabUriWithoutVersion
          : keytabUriWithoutVersion + "#" + keytabUriVersion;
      return build(uri);
    }

    private NamespaceMeta build(@Nullable String keytabUri) {
      if (name == null) {
        throw new IllegalArgumentException("Namespace id cannot be null.");
      }
      if (description == null) {
        description = "";
      }

      // scheduler queue name is kept non nullable unlike others like root directory, hbase namespace etc for backward
      // compatibility
      if (schedulerQueueName == null) {
        schedulerQueueName = "";
      }

      return new NamespaceMeta(name, description, generation,
          new NamespaceConfig(schedulerQueueName, rootDirectory,
              hbaseNamespace, hiveDatabase,
              principal, groupName, keytabUri,
              configMap), identity);
    }
  }

  public NamespaceId getNamespaceId() {
    return new NamespaceId(name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NamespaceMeta other = (NamespaceMeta) o;
    return Objects.equals(name, other.name)
        && generation == other.generation
        && Objects.equals(description, other.description)
        && Objects.equals(config, other.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, generation, config);
  }

  @Override
  public String toString() {
    return "NamespaceMeta{"
        + "name='" + name + '\''
        + ", description='" + description + '\''
        + ", generation=" + generation
        + ", config=" + config
        + '}';
  }
}
