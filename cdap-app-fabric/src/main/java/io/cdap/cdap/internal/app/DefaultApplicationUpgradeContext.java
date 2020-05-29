/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app;

import com.google.common.base.Predicate;
import io.cdap.cdap.api.app.ApplicationUpgradeContext;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.id.Id.Artifact;
import io.cdap.cdap.common.id.Id.Namespace;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link ApplicationUpgradeContext}.
 *
 * Used during upgrade of an Application config to use helper methods such as getting latest plugin version available.
 */
public class DefaultApplicationUpgradeContext implements ApplicationUpgradeContext {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultApplicationUpgradeContext.class);

  private final ArtifactId applicationArtifactId;
  private final ArtifactRepository artifactRepository;
  private final ApplicationId applicationId;
  private final NamespaceId namespaceId;

  public DefaultApplicationUpgradeContext(NamespaceId namespaceId, ApplicationId applicationId,
                                          ArtifactId applicationArtifactId, ArtifactRepository artifactRepository) {
    this.namespaceId = namespaceId;
    this.applicationId = applicationId;
    this.artifactRepository = artifactRepository;
    this.applicationArtifactId = applicationArtifactId;
  }

  @Nullable
  @Override
  public ArtifactId getLatestPluginArtifact(String pluginType, String pluginName) {
    // Candidates for upgrading given plugin.
    List<ArtifactId> candidates = getPluginArtifacts(pluginType, pluginName, ArtifactSortOrder.ASC);
    if (candidates.isEmpty()) return null;

    // Choosing the last one as that would have the highest version as the result should be sorted in ascending order.
    ArtifactId chosenArtifact = candidates.get(candidates.size() - 1);
    return chosenArtifact;
  }

  private List<ArtifactId> getPluginArtifacts(String pluginType, String pluginName, ArtifactSortOrder sortOrder) {
    List<ArtifactId> pluginArtifacts = new ArrayList<>();
    try {
      Map<ArtifactDescriptor, PluginClass> plugins =
          artifactRepository.getPlugins(NamespaceId.SYSTEM,
                                        Artifact.from(Namespace.fromEntityId(namespaceId), applicationArtifactId),
                                        pluginType, pluginName, /*predicate=*/null,
                                        /*limitNumber=*/Integer.MAX_VALUE, sortOrder);
      for (Map.Entry<ArtifactDescriptor, PluginClass> pluginsEntry : plugins.entrySet()) {
        ArtifactId plugin = pluginsEntry.getKey().getArtifactId();
        // Only consider non-SNAPSHOT plugins for upgrade.
        if(!plugin.getVersion().isSnapshot()) {
          pluginArtifacts.add(plugin);
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to get plugin details for artifact {} for plugin {} of type {}",
               applicationArtifactId, pluginName, pluginType, e);
    }
    return pluginArtifacts;
  }
}
