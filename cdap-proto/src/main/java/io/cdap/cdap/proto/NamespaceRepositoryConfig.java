/*
 * Copyright © 2023 Cask Data, Inc.
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
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents the repository configuration of a namespace. This class needs to be GSON serializable.
 */
@JsonAdapter(NamespaceRepositoryConfigCodec.class)
public class NamespaceRepositoryConfig {
  public static final String PROVIDER = "provider";
  public static final String REPOSITORY_LINK = "repository.link";
  public static final String DEFAULT_BRANCH = "default.branch";
  public static final String AUTH_TYPE = "auth.type";
  public static final String USERNAME = "username";
  public static final String PATH_PREFIX = "path.prefix";

  private final Map<String, String> configs;

  public NamespaceRepositoryConfig(@Nullable String provider, @Nullable String repoLink, @Nullable String defaultBranch,
                                   @Nullable String authType, @Nullable String username, @Nullable String pathPrefix) {
    this(provider, username, repoLink, defaultBranch, authType, pathPrefix, new HashMap<>());
  }
  
  public NamespaceRepositoryConfig(@Nullable String provider, @Nullable String repoLink, @Nullable String defaultBranch,
                                   @Nullable String authType, @Nullable String username, @Nullable String pathPrefix,
                                   Map<String, String> existingConfigs) {
    Map<String, String> configs = new HashMap<>(existingConfigs);

    if (provider != null) {
      configs.put(PROVIDER, provider);
    }

    if (repoLink != null) {
      configs.put(REPOSITORY_LINK, repoLink);
    }

    if (defaultBranch != null) {
      configs.put(DEFAULT_BRANCH, defaultBranch);
    }

    if (authType != null) {
      configs.put(AUTH_TYPE, authType);
    }

    if (username != null) {
      configs.put(USERNAME, username);
    }

    if (pathPrefix != null) {
      configs.put(PATH_PREFIX, pathPrefix);
    }

    this.configs = Collections.unmodifiableMap(configs);
  }

  /**
   * Creates a new instance with the given set of configurations.
   */
  public NamespaceRepositoryConfig(Map<String, String> configs) {
    this.configs = Collections.unmodifiableMap(new HashMap<>(configs));
  }

  public String getProvider() {
    return getConfig(PROVIDER);
  }

  public String getRepositoryLink() {
    return getConfig(REPOSITORY_LINK);
  }

  public String getAuthType() {
    return getConfig(AUTH_TYPE);
  }

  public String getDefaultBranch() {
    return getConfig(DEFAULT_BRANCH);
  }

  @Nullable
  public String getUsername() {
    return getConfig(USERNAME);
  }
  
  @Nullable
  public String getPathPrefix() {
    return getConfig(PATH_PREFIX);
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
    NamespaceRepositoryConfig that = (NamespaceRepositoryConfig) o;
    return Objects.equals(configs, that.configs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(configs);
  }

  @Override
  public String toString() {
    return "NamespaceRepositoryConfig{" +
      "configs=" + configs +
      '}';
  }

  /**
   * Returns the raw full config map. This is for the {@link NamespaceRepositoryConfigCodec} to use.
   */
  public Map<String, String> getConfigs() {
    return configs;
  }
}
