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
      .setDescription("This is the default namespace, which is automatically created, and is always available.")
      .build();

  public static final NamespaceMeta SYSTEM =
    new NamespaceMeta.Builder()
      .setName(NamespaceId.SYSTEM)
      .setDescription("The system namespace, which is used for internal purposes.")
      .build();

  private final String name;
  private final String description;
  private final long generation;
  private final NamespaceConfig config;
  private final NamespaceRepositoryConfig repository;

  private NamespaceMeta(String name, String description, long generation,
                        NamespaceConfig config, NamespaceRepositoryConfig repo) {
    this.name = name;
    this.description = description;
    this.generation = generation;
    this.config = config;
    this.repository = repo;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  /**
   * Get the namespace generation. The generation is set when the namespace is created. If the namespace is deleted
   * and then created again, it will have a higher generation.
   *
   * @return the namespace generation
   */
  public long getGeneration() {
    return generation;
  }

  public NamespaceConfig getConfig() {
    return config;
  }

  public NamespaceRepositoryConfig getRepoConfig() {
    return repository;
  }

  /**
   * Builder used to build {@link NamespaceMeta}
   */
  public static final class Builder {
    private String name;
    private String description;
    private String schedulerQueueName;
    private String rootDirectory;
    private String hbaseNamespace;
    private String hiveDatabase;
    private String principal;
    private String groupName;
    private String keytabURIWithoutVersion;
    private int keytabURIVersion;
    private long generation;
    private Map<String, String> configMap = new HashMap<>();
    // The repository configuration properties
    private String repoProvider;
    private String repoLink;
    private String repoDefaultBranch;
    private String repoAuthType;
    private String repoUserName;
    private String repoPathPrefix;
    private Map<String, String> repoConfigMap = new HashMap<>();

    public Builder() {
      // No-Op
    }

    public Builder(NamespaceMeta meta) {
      this.name = meta.getName();
      this.description = meta.getDescription();
      this.generation = meta.getGeneration();
      NamespaceConfig config = meta.getConfig();
      if (config != null) {
        this.configMap = config.getConfigs();
        this.schedulerQueueName = config.getSchedulerQueueName();
        this.rootDirectory = config.getRootDirectory();
        this.hbaseNamespace = config.getHbaseNamespace();
        this.hiveDatabase = config.getHiveDatabase();
        this.principal = config.getPrincipal();
        this.groupName = config.getGroupName();
        this.keytabURIWithoutVersion = config.getKeytabURIWithoutVersion();
        this.keytabURIVersion = config.getKeytabURIVersion();
      }

      NamespaceRepositoryConfig repoConfig = meta.getRepoConfig();
      if (repoConfig != null) {
        this.repoConfigMap = repoConfig.getConfigs();
        this.repoProvider = repoConfig.getProvider();
        this.repoLink = repoConfig.getRepositoryLink();
        this.repoDefaultBranch = repoConfig.getDefaultBranch();
        this.repoAuthType = repoConfig.getAuthType();
        this.repoUserName = repoConfig.getUsername();
        this.repoPathPrefix = repoConfig.getPathPrefix();
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

    public Builder setKeytabURI(String keytabURI) {
      this.keytabURIWithoutVersion = keytabURI;
      this.keytabURIVersion = 0;
      return this;
    }

    public Builder setKeytabURIWithoutVersion(String keytabURIWithoutVersion) {
      this.keytabURIWithoutVersion = keytabURIWithoutVersion;
      return this;
    }

    public Builder incrementKeytabURIVersion() {
      keytabURIVersion++;
      return this;
    }

    public void setKeytabURIVersion(int keytabURIVersion) {
      this.keytabURIVersion = keytabURIVersion;
    }

    public Builder setGeneration(long generation) {
      this.generation = generation;
      return this;
    }

    public Builder setConfig(Map<String, String> configMap) {
      this.configMap = configMap;
      return this;
    }

    public Builder setRepoProvider(String provider) {
      this.repoProvider = provider;
      return this;
    }

    public Builder setRepoLink(String repoLink) {
      this.repoLink = repoLink;
      return this;
    }

    public Builder setRepoDefaultBranch(String defaultBranch) {
      this.repoDefaultBranch = defaultBranch;
      return this;
    }

    public Builder setRepoAuthType(String authType) {
      this.repoAuthType = authType;
      return this;
    }

    public Builder setRepoUserName(String userName) {
      this.repoUserName = userName;
      return this;
    }
    
    public Builder setRepoPathPrefix(String pathPrefix) {
      this.repoPathPrefix = pathPrefix;
      return this;
    }

    public Builder setRepoConfig(Map<String, String> repoConfigMap) {
      this.repoConfigMap = repoConfigMap;
      return this;
    }

    public Builder deleteRepoConfig() {
      this.repoConfigMap = new HashMap<>();
      this.repoProvider = null;
      this.repoLink = null;
      this.repoDefaultBranch = null;
      this.repoAuthType = null;
      this.repoUserName = null;
      this.repoPathPrefix = null;
      return this;
    }

    public NamespaceMeta build() {
      // combine the keytab URI with the version if the version is not 0
      String uri = keytabURIVersion == 0 ? keytabURIWithoutVersion : keytabURIWithoutVersion + "#" + keytabURIVersion;
      return build(uri);
    }

    public NamespaceMeta buildWithoutKeytabURIVersion() {
      return build(keytabURIWithoutVersion);
    }

    private NamespaceMeta build(@Nullable String keytabURI) {
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
                                                   principal, groupName, keytabURI,
                                                   configMap),
                               new NamespaceRepositoryConfig(repoProvider, repoLink,
                                                             repoDefaultBranch, repoAuthType,
                                                             repoUserName, repoPathPrefix,
                                                             repoConfigMap));
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
      && Objects.equals(config, other.config)
      && Objects.equals(repository, other.repository);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, generation, config, repository);
  }

  @Override
  public String toString() {
    return "NamespaceMeta{" +
      "name='" + name + '\'' +
      ", description='" + description + '\'' +
      ", generation=" + generation +
      ", config=" + config +
      ", repoConfig=" + repository +
      '}';
  }
}
