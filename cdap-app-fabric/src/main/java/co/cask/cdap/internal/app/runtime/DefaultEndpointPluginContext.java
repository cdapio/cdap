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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.api.plugin.EndpointPluginContext;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.data2.security.Impersonator;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.plugin.FindPluginHelper;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Throwables;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * An implementation of {@link EndpointPluginContext} that uses {@link PluginInstantiator}
 */
public class DefaultEndpointPluginContext implements EndpointPluginContext {

  private final ArtifactRepository artifactRepository;
  // this is the namespace of the artifact
  private final NamespaceId namespace;

  private final PluginInstantiator pluginInstantiator;
  private final Id.Artifact parentArtifactId;
  private final Impersonator impersonator;
  private final LocationFactory locationFactory;

  public DefaultEndpointPluginContext(NamespaceId namespace, ArtifactRepository artifactRepository,
                                      PluginInstantiator pluginInstantiator, Id.Artifact parentArtifactId,
                                      Impersonator impersonator, LocationFactory locationFactory) {
    this.namespace = namespace;
    this.artifactRepository = artifactRepository;
    this.pluginInstantiator = pluginInstantiator;
    this.parentArtifactId = parentArtifactId;
    this.impersonator = impersonator;
    this.locationFactory = locationFactory;
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginType, String pluginName) {
    return loadPluginClass(pluginType, pluginName, PluginProperties.builder().build(), new PluginSelector());
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginType, String pluginName,
                                      PluginProperties pluginProperties) {
    return loadPluginClass(pluginType, pluginName, pluginProperties, new PluginSelector());
  }

  @Nullable
  @Override
  public <T> Class<T> loadPluginClass(String pluginType, String pluginName, PluginProperties pluginProperties,
                                      PluginSelector pluginSelector) {
    Plugin plugin;
    try {
      plugin = FindPluginHelper.findPlugin(artifactRepository, pluginInstantiator, namespace, parentArtifactId,
                                           pluginType, pluginName, pluginProperties, pluginSelector, impersonator,
                                           locationFactory);
    } catch (PluginNotExistsException e) {
      // Plugin not found, hence return null
      return null;
    } catch (ArtifactNotFoundException e) {
      // this shouldn't happen, it means the artifact for this app does not exist.
      throw new IllegalStateException(
        String.format("Application artifact '%s' no longer exists. Please check if it was deleted.",
                      parentArtifactId));
    }

    try {
      return pluginInstantiator.loadClass(plugin);
    } catch (IOException e) {
      // If the plugin jar is deleted without notifying the artifact service.
      return null;
    } catch (ClassNotFoundException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }
}
