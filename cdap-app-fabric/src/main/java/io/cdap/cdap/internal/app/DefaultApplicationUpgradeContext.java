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
import com.google.common.collect.Lists;
import io.cdap.cdap.api.app.ApplicationUpgradeContext;
import io.cdap.cdap.api.app.ArtifactSelectorConfig;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.id.Id.Artifact;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.proto.artifact.PluginInfo;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultApplicationUpgradeContext implements ApplicationUpgradeContext {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultApplicationUpgradeContext.class);

  private final NamespaceId namespaceId;
  private final ApplicationId applicationId;
  private final ArtifactRepository artifactRepository;
  private final ArtifactId artifactId;

  public DefaultApplicationUpgradeContext(NamespaceId namespaceId, ApplicationId applicationId,
      ArtifactId artifactId,
      ArtifactRepository artifactRepository) {
    this.namespaceId = namespaceId;
    this.applicationId = applicationId;
    this.artifactRepository = artifactRepository;
    this.artifactId = artifactId;
  }
  @Nullable
  @Override
  public ArtifactSelectorConfig getPluginArtifact(String pluginType, String pluginName) {
    LOG.info("Jay Pandya reached in getPluginArtifact in Data Pipeline app 1");
    try {
      List<PluginInfo> plugins = getPlugins(pluginType, pluginName);
      LOG.info("Jay Pandya reached in getPluginArtifact in Data Pipeline app 10");
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

  private List<PluginInfo> getPlugins(String pluginType, String pluginName) {
    LOG.info("Jay Pandya reached in getPluginArtifact in Data Pipeline app 2");
    ArtifactScope pluginScope = ArtifactScope.SYSTEM;
    NamespaceId pluginArtifactNamespace = NamespaceId.SYSTEM;
    Predicate<ArtifactId> predicate = new Predicate<ArtifactId>() {
      @Override
      public boolean apply(ArtifactId input) {
        // should check if the artifact is from SYSTEM namespace, if not, check if it is from the scoped namespace.
        // by default, the scoped namespace is for USER scope
        return (((pluginScope == null && NamespaceId.SYSTEM.equals(input.getParent()))
            || pluginArtifactNamespace.equals(input.getParent())));
      }
    };
    LOG.info("Jay Pandya reached in getPluginArtifact in Data Pipeline app 3");
    List<PluginInfo> pluginInfos = null;
    try {
      LOG.info("Jay Pandya reached in getPluginArtifact in Data Pipeline app 4");
      SortedMap<ArtifactDescriptor, PluginClass> plugins =
          artifactRepository.getPlugins(this.namespaceId, Id.Artifact.fromEntityId(this.artifactId),
              pluginType, pluginName, predicate, /*limitNumber=*/ Integer.MAX_VALUE,
              ArtifactSortOrder.ASC);
      LOG.info("Jay Pandya reached in getPluginArtifact in Data Pipeline app 5");
      pluginInfos = Lists.newArrayList();

      LOG.info("Jay Pandya reached in getPluginArtifact in Data Pipeline app 6");
      // flatten the map
      for (Map.Entry<ArtifactDescriptor, PluginClass> pluginsEntry : plugins.entrySet()) {
        LOG.info("Jay Pandya reached in getPluginArtifact in Data Pipeline app 7");
        ArtifactDescriptor pluginArtifact = pluginsEntry.getKey();
        ArtifactSummary pluginArtifactSummary = ArtifactSummary
            .from(pluginArtifact.getArtifactId());

        PluginClass pluginClass = pluginsEntry.getValue();
        pluginInfos.add(new PluginInfo(pluginClass, pluginArtifactSummary));
        LOG.info("Jay Pandya reached in getPluginArtifact in Data Pipeline app 8");
      }
    } catch (Exception e) {
      LOG.error(
          "Issue in fetching plugin information for plugin name %s of plugin type %s for application %s",
          pluginName, pluginType, applicationId, e);
      // Throw exception here to abort upgrade if no plugin found or any error.
    }
    LOG.info("Jay Pandya reached in getPluginArtifact in Data Pipeline app 9");
    return pluginInfos;
  }
}
