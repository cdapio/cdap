/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates;

import co.cask.cdap.api.templates.AdapterPluginRegistry;
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginInfo;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.api.templates.plugins.PluginSelector;
import co.cask.cdap.internal.app.runtime.adapter.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.adapter.PluginRepository;
import co.cask.cdap.internal.plugins.AdapterPlugin;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public class DefaultPluginRegistry implements AdapterPluginRegistry {

  private final PluginRepository pluginRepository;
  private final PluginInstantiator pluginInstantiator;
  private final Id.Artifact artifactId;

  protected final Map<String, AdapterPlugin> plugins = Maps.newHashMap();

  public DefaultPluginRegistry(PluginRepository pluginRepository, PluginInstantiator pluginInstantiator,
                               Id.Artifact artifactId) {
    this.pluginRepository = pluginRepository;
    this.pluginInstantiator = pluginInstantiator;
    this.artifactId = artifactId;
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties) {
    return usePlugin(pluginType, pluginName, pluginId, properties, new PluginSelector());
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                         PluginSelector selector) {
    Preconditions.checkArgument(!plugins.containsKey(pluginId),
                                "Plugin of type {}, name {} was already added.", pluginType, pluginName);
    Preconditions.checkArgument(properties != null, "Plugin properties cannot be null");

    // TODO: PluginRepository should ideally require namespace, artifact name, vesion.
    Map.Entry<PluginInfo, PluginClass> pluginEntry = pluginRepository.findPlugin(artifactId.toString(),
                                                                                 pluginType, pluginName, selector);
    if (pluginEntry == null) {
      return null;
    }
    try {
      T instance = pluginInstantiator.newInstance(pluginEntry.getKey(), pluginEntry.getValue(), properties);
      registerPlugin(pluginId, pluginEntry.getKey(), pluginEntry.getValue(), properties);
      return instance;
    } catch (IOException e) {
      // If the plugin jar is deleted without notifying the adapter service.
      return null;
    } catch (ClassNotFoundException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties properties) {
    return usePluginClass(pluginType, pluginName, pluginId, properties, new PluginSelector());
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                                     PluginSelector selector) {
    Preconditions.checkArgument(!plugins.containsKey(pluginId),
                                "Plugin of type %s, name %s was already added.", pluginType, pluginName);
    Preconditions.checkArgument(properties != null, "Plugin properties cannot be null");

    // TODO: PluginRepository should ideally require namespace, artifact name, vesion.
    Map.Entry<PluginInfo, PluginClass> pluginEntry = pluginRepository.findPlugin(artifactId.toString(),
                                                                                 pluginType, pluginName, selector);
    if (pluginEntry == null) {
      return null;
    }

    // Just verify if all required properties are provided.
    // No type checking is done for now.
    for (PluginPropertyField field : pluginEntry.getValue().getProperties().values()) {
      Preconditions.checkArgument(!field.isRequired() || properties.getProperties().containsKey(field.getName()),
                                  "Required property '%s' missing for plugin of type %s, name %s.",
                                  field.getName(), pluginType, pluginName);
    }

    try {
      Class<T> cls = pluginInstantiator.loadClass(pluginEntry.getKey(), pluginEntry.getValue());
      registerPlugin(pluginId, pluginEntry.getKey(), pluginEntry.getValue(), properties);
      return cls;
    } catch (IOException e) {
      // If the plugin jar is deleted without notifying the adapter service.
      return null;
    } catch (ClassNotFoundException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }

  /**
   * Register the given plugin in this configurer.
   */
  private void registerPlugin(String pluginId, PluginInfo pluginInfo, PluginClass pluginClass,
                              PluginProperties properties) {
    plugins.put(pluginId, new AdapterPlugin(pluginInfo, pluginClass, properties));
  }
}
