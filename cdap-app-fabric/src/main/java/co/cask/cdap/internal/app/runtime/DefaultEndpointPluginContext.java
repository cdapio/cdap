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

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.plugin.EndpointPluginContext;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.plugin.FindPluginHelper;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * An implementation of {@link EndpointPluginContext} that uses {@link PluginInstantiator}
 */
public class DefaultEndpointPluginContext implements EndpointPluginContext {
  private final ArtifactRepository artifactRepository;
  // this is the namespace of the artifact
  private final NamespaceId namespace;

  private final PluginInstantiator pluginInstantiator;
  private final Set<ArtifactRange> parentArtifacts;

  public DefaultEndpointPluginContext(NamespaceId namespace, ArtifactRepository artifactRepository,
                                      PluginInstantiator pluginInstantiator, Set<ArtifactRange> parentArtifacts) {
    this.namespace = namespace;
    this.artifactRepository = artifactRepository;
    this.pluginInstantiator = pluginInstantiator;
    this.parentArtifacts = parentArtifacts;
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

  private Plugin findAndGetPlugin(String pluginType, String pluginName,
                                  @Nullable PluginProperties pluginProperties,
                                  PluginSelector pluginSelector) throws IllegalStateException {
    pluginProperties = pluginProperties == null ? PluginProperties.builder().build() : pluginProperties;
    for (ArtifactRange artifactRange : parentArtifacts) {
      try {
        Map.Entry<ArtifactDescriptor, PluginClass> pluginEntry
          = artifactRepository.findPlugin(namespace, artifactRange, pluginType, pluginName, pluginSelector);
        return FindPluginHelper.getPlugin(ImmutableList.<ArtifactId>of(), pluginEntry, pluginProperties,
                                          pluginType, pluginName, pluginInstantiator);
      } catch (PluginNotExistsException | ArtifactNotFoundException e) {
        // PluginNotExists means plugin does not belong to this parent artifact, we will try next parent artifact
        // ArtifactNotFound means the artifact for this app does not exist. we will try next artifact
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    // none of the parents were able to find the plugin
    throw new IllegalStateException(
      String.format("Unable to load plugin, with type %s and name %s.", pluginType,  pluginName));
  }

  @Nullable
  @Override
  public <T> Class<T> loadPluginClass(String pluginType, String pluginName, PluginProperties pluginProperties,
                                      PluginSelector pluginSelector) {
    Plugin plugin = findAndGetPlugin(pluginType, pluginName, pluginProperties, pluginSelector);
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
