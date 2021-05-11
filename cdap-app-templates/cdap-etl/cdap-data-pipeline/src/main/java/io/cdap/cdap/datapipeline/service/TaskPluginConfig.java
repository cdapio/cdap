/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline.service;

import java.util.Map;

/**
 * POJO for holding input params for task
 */
public class TaskPluginConfig {

  private String artifactName;
  private String artifactScope;
  private String artifactVersion;
  private String namespace;
  private String pluginName;
  private String type;
  private String stageName;
  private Map<String, String> pluginProperties;

  public TaskPluginConfig(String artifactName, String artifactScope, String artifactVersion, String namespace,
                          String pluginName, String type, Map<String, String> pluginProperties,
                          String stageName) {
    this.artifactName = artifactName;
    this.artifactScope = artifactScope;
    this.artifactVersion = artifactVersion;
    this.namespace = namespace;
    this.pluginName = pluginName;
    this.type = type;
    this.pluginProperties = pluginProperties;
    this.stageName = stageName;
  }

  public String getArtifactName() {
    return artifactName;
  }

  public String getArtifactScope() {
    return artifactScope;
  }

  public String getArtifactVersion() {
    return artifactVersion;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getPluginName() {
    return pluginName;
  }

  public String getType() {
    return type;
  }

  public Map<String, String> getPluginProperties() {
    return pluginProperties;
  }

  public String getStageName() {
    return stageName;
  }
}
