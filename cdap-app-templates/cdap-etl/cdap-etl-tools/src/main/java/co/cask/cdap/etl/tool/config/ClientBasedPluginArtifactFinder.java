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

package co.cask.cdap.etl.tool.config;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.artifact.PluginInfo;

import java.util.List;

/**
 * Uses an ArtifactClient to get the artifact for a specific plugin.
 */
public abstract class ClientBasedPluginArtifactFinder implements PluginArtifactFinder {
  protected final ArtifactClient artifactClient;
  protected final Id.Artifact artifactId;

  protected ClientBasedPluginArtifactFinder(ArtifactClient artifactClient, Id.Artifact artifactId) {
    this.artifactClient = artifactClient;
    this.artifactId = artifactId;
  }

  @Override
  public ArtifactSummary getTransformPluginArtifact(String pluginName) {
    return getArtifact("transform", pluginName);
  }

  protected ArtifactSummary getArtifact(String pluginType, String pluginName) {
    try {
      List<PluginInfo> plugins =
        artifactClient.getPluginInfo(artifactId, pluginType, pluginName, ArtifactScope.SYSTEM);

      if (plugins.isEmpty()) {
        return null;
      }

      return plugins.get(plugins.size() - 1).getArtifact();
    } catch (Exception e) {
      return null;
    }
  }
}
