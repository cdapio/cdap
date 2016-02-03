/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.tool.console;

import co.cask.cdap.etl.tool.config.PluginArtifactFinder;
import co.cask.cdap.proto.artifact.ArtifactSummary;

import java.util.Map;

/**
 * Data UI stores for plugin templates.
 * This object can be serialized/deserialized directly.
 *
 *     "pluginTemplates": {
 *       "[namespace]": {
 *         "cdap-etl-batch": {
 *           "batchsource": {
 *             "[templatename]": {
 *               "description": "",
 *               "lock": {
 *                 "[propertyname]": true/false,
 *                 ...
 *               },
 *               "pluginName": "Database",
 *               "pluginTemplate": "[templatename]",
 *               "pluginType": "batchsource",
 *               "properties": {
 *                 "[propertyname]": "[propertyvalue]",
 *                 ...
 *               },
 *               "templateType": "cdap-etl-batch"
 *             }
 *           }
 *         }
 *       }
 *     },
 */
public class PluginTemplate {
  private final String description;
  private final Map<String, Boolean> lock;
  private final String pluginName;
  private final String pluginTemplate;
  private final String pluginType;
  private final Map<String, String> properties;
  private final String templateType;
  private ArtifactSummary artifact;

  public PluginTemplate(String description, Map<String, Boolean> lock, String pluginName, String pluginTemplate,
                        String pluginType, Map<String, String> properties, String templateType) {
    this.description = description;
    this.lock = lock;
    this.pluginName = pluginName;
    this.pluginTemplate = pluginTemplate;
    this.pluginType = pluginType;
    this.properties = properties;
    this.templateType = templateType;
  }

  public String getDescription() {
    return description;
  }

  public Map<String, Boolean> getLock() {
    return lock;
  }

  public String getPluginName() {
    return pluginName;
  }

  public String getPluginTemplate() {
    return pluginTemplate;
  }

  public String getPluginType() {
    return pluginType;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public String getTemplateType() {
    return templateType;
  }

  public ArtifactSummary getArtifact() {
    return artifact;
  }

  // sets the artifact field based on what artifacts contain are available to the etl app.
  public void setArtifact(PluginArtifactFinder pluginArtifactFinder) throws Exception {
    if ("transform".equals(pluginType)) {
      this.artifact = pluginArtifactFinder.getTransformPluginArtifact(pluginName);
    } else if (pluginType.endsWith("source")) {
      this.artifact = pluginArtifactFinder.getSourcePluginArtifact(pluginName);
    } else if (pluginType.endsWith("sink")) {
      this.artifact = pluginArtifactFinder.getSinkPluginArtifact(pluginName);
    }
  }
}
