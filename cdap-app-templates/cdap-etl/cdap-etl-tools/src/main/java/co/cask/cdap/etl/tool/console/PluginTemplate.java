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

import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
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
// fields are mainly here for Gson deserialization/serialization
@SuppressWarnings({"FieldCanBeLocal", "unused"})
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

  public ArtifactSummary getArtifact() {
    return artifact;
  }

  // sets the artifact field based on what artifacts contain are available to the etl app.
  public void setArtifact(PluginArtifactFinder pluginArtifactFinder) {
    if (Transform.PLUGIN_TYPE.equals(pluginType)) {
      this.artifact = pluginArtifactFinder.getTransformPluginArtifact(pluginName);
    } else if (BatchSource.PLUGIN_TYPE.equals(pluginType) || RealtimeSource.PLUGIN_TYPE.equals(pluginType)) {
      this.artifact = pluginArtifactFinder.getSourcePluginArtifact(pluginName);
    } else if (BatchSink.PLUGIN_TYPE.equals(pluginType) || RealtimeSink.PLUGIN_TYPE.equals(pluginType)) {
      this.artifact = pluginArtifactFinder.getSinkPluginArtifact(pluginName);
    }
  }
}
