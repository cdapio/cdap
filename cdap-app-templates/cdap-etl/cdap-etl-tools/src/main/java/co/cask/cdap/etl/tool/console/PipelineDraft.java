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

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.etl.tool.config.OldETLBatchConfig;
import co.cask.cdap.etl.tool.config.OldETLRealtimeConfig;
import co.cask.cdap.etl.tool.config.PluginArtifactFinder;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * If the UI has stored drafts under "adapterDrafts",
 * it will contain a mapping of pipeline name to one of these objects.
 * This object can be deserialized directly.
 *
 * {
 *   "artifact": { "name": "cdap-etl-batch", "scope": "SYSTEM", "version": "3.2.1" },
 *   "config": { [OldETLBatchConfig },
 *   "name": "",
 *   "description": "",
 *   "ui": { ... }
 * }
 */
public class PipelineDraft {
  private static final Gson GSON = new Gson();
  private final ArtifactSummary artifact;
  private final JsonObject config;
  // name and description are not used by the backend at all. But they are used by the UI...
  // description exists in 3.2.x drafts
  private final String description;
  // name does NOT exist in 3.2.x drafts, but is required by 3.3.x....
  private String name;

  public PipelineDraft(ArtifactSummary artifact, String name, String description, JsonObject config) {
    this.artifact = artifact;
    this.description = description;
    this.name = name;
    this.config = config;
  }

  // name does NOT exist in 3.2.x drafts, but is required by 3.3.x so we have to have a setter...
  public void setName(String name) {
    this.name = name;
  }

  public boolean isOldBatch() {
    return artifact.getScope() == ArtifactScope.SYSTEM &&
      artifact.getVersion().startsWith("3.2") &&
      "cdap-etl-batch".equals(artifact.getName());
  }

  public boolean isOldRealtime() {
    return artifact.getScope() == ArtifactScope.SYSTEM &&
      artifact.getVersion().startsWith("3.2") &&
      "cdap-etl-realtime".equals(artifact.getName());
  }

  public PipelineDraft getUpgradedBatch(ArtifactSummary newArtifact, PluginArtifactFinder pluginArtifactFinder) {
    OldETLBatchConfig oldConfig = GSON.fromJson(config, OldETLBatchConfig.class);
    JsonObject upgradedConfig = GSON.toJsonTree(oldConfig.getNewConfig(pluginArtifactFinder)).getAsJsonObject();
    return new PipelineDraft(newArtifact, name, description, upgradedConfig);
  }

  public PipelineDraft getUpgradedRealtime(ArtifactSummary newArtifact, PluginArtifactFinder pluginArtifactFinder) {
    OldETLRealtimeConfig oldConfig = GSON.fromJson(config, OldETLRealtimeConfig.class);
    JsonObject upgradedConfig = GSON.toJsonTree(oldConfig.getNewConfig(pluginArtifactFinder)).getAsJsonObject();
    return new PipelineDraft(newArtifact, name, description, upgradedConfig);
  }
}
