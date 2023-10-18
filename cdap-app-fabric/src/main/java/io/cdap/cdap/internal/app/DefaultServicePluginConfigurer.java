/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.app;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.api.service.http.ServicePluginConfigurer;
import io.cdap.cdap.common.lang.CombineClassLoader;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginClassLoaders;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.common.PluginNotExistsException;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Default service plugin configurer which supports creating a combined class loader for all the
 * plugins used.
 */
public class DefaultServicePluginConfigurer extends DefaultPluginConfigurer implements
    ServicePluginConfigurer {

  private final ClassLoader programClassLoader;

  public DefaultServicePluginConfigurer(ArtifactId artifactId, NamespaceId pluginNamespaceId,
      PluginInstantiator pluginInstantiator, PluginFinder pluginFinder,
      ClassLoader programClassLoader) {
    super(artifactId, pluginNamespaceId, pluginInstantiator, pluginFinder);
    this.programClassLoader = programClassLoader;
  }

  @Override
  public ClassLoader createClassLoader() {
    Map<String, Plugin> plugins = getPlugins().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getPlugin()));

    ClassLoader pluginsClassLoader =
        PluginClassLoaders.createFilteredPluginsClassLoader(plugins, getPluginInstantiator());
    return new CombineClassLoader(null, programClassLoader, pluginsClassLoader,
        getClass().getClassLoader());
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId,
      PluginProperties properties,
      PluginSelector selector, MacroEvaluator macroEvaluator, MacroParserOptions options) {
    try {
      Plugin plugin = addPlugin(pluginType, pluginName, pluginId, properties, selector);
      return pluginInstantiator.newInstance(plugin, macroEvaluator, options);
    } catch (PluginNotExistsException | IOException e) {
      return null;
    } catch (ClassNotFoundException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }
}
