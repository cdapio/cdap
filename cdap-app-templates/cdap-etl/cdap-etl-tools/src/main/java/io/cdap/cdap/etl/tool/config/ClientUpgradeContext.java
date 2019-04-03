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

package io.cdap.cdap.etl.tool.config;

import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.client.ArtifactClient;
import io.cdap.cdap.client.NamespaceClient;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.UpgradeContext;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.artifact.PluginInfo;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Uses an ArtifactClient to get the artifact for a specific plugin.
 */
public class ClientUpgradeContext implements UpgradeContext {
  private static final Logger LOG = LoggerFactory.getLogger(ClientUpgradeContext.class);
  private final NamespaceClient namespaceClient;
  private final ArtifactClient artifactClient;
  private final String artifactName;
  private final String artifactVersion;
  private ArtifactId artifactId;

  public ClientUpgradeContext(NamespaceClient namespaceClient, ArtifactClient artifactClient, String artifactName,
                              String artifactVersion) {
    this.namespaceClient = namespaceClient;
    this.artifactClient = artifactClient;
    this.artifactName = artifactName;
    this.artifactVersion = artifactVersion;
  }

  @Nullable
  @Override
  public ArtifactSelectorConfig getPluginArtifact(String pluginType, String pluginName) {
    try {
      List<PluginInfo> plugins =
        artifactClient.getPluginInfo(getArtifactId(), pluginType, pluginName, ArtifactScope.SYSTEM);

      if (plugins.isEmpty()) {
        return null;
      }

      // doesn't really matter which one we choose, as all of them should be valid.
      // choosing the last one because that tends to be the one with the highest version.
      // order is not guaranteed though.
      ArtifactSummary chosenArtifact = plugins.get(plugins.size() - 1).getArtifact();
      return new ArtifactSelectorConfig(chosenArtifact.getScope().name(),
                                        chosenArtifact.getName(),
                                        chosenArtifact.getVersion());
    } catch (Exception e) {
      LOG.warn("Unable to find an artifact for plugin of type {} and name {}. " +
                 "Plugin artifact section will be left empty.", pluginType, pluginName);
      return null;
    }
  }

  private ArtifactId getArtifactId() {
    if (artifactId == null) {
      // just need to get a namespace that exists, ArtifactScope.SYSTEM will take care of the rest.
      NamespaceId namespaceId = NamespaceId.DEFAULT;
      try {
        List<NamespaceMeta> namespaces = namespaceClient.list();
        if (!namespaces.isEmpty()) {
          namespaceId = namespaceClient.list().get(0).getNamespaceId();
        }
      } catch (Exception e) {
        LOG.warn("Unable to list namespaces. Plugin artifact sections will likely be left empty.", e);
      }
      artifactId = namespaceId.artifact(artifactName, artifactVersion);
    }
    return artifactId;
  }
}
