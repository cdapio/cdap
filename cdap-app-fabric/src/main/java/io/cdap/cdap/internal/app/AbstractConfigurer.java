/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

import com.google.common.base.Preconditions;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.api.DefaultDatasetConfigurer;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Abstract base implementation for application and program configurer.
 */
public abstract class AbstractConfigurer extends DefaultDatasetConfigurer implements PluginConfigurer {

  private final DefaultPluginConfigurer pluginConfigurer;
  private final Map<String, Plugin> extraPlugins;
  // this is the namespace that the app will be deployed in, which can be different than the namespace of
  // the artifact. If the artifact is a system artifact, it will have the system namespace.
  protected final Id.Namespace deployNamespace;

  protected AbstractConfigurer(Id.Namespace deployNamespace, Id.Artifact artifactId,
                               PluginFinder pluginFinder, PluginInstantiator pluginInstantiator) {
    this.deployNamespace = deployNamespace;
    this.extraPlugins = new HashMap<>();
    this.pluginConfigurer = new DefaultPluginConfigurer(artifactId.toEntityId(),
                                                        deployNamespace.toEntityId(), pluginInstantiator,
                                                        pluginFinder);
  }

  public Map<String, Plugin> getPlugins() {
    Map<String, Plugin> plugins = pluginConfigurer.getPlugins().entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPlugin()));

    plugins.putAll(extraPlugins);
    return plugins;
  }

  protected void addPlugins(Map<String, Plugin> pluginsToAdd) {
    Map<String, Plugin> existingPlugins = getPlugins();
    // We don't allow adding different plugin with same id. Adding same plugin to an id is allowed as it just
    // means that the plugin is being registered again.
    Map<String, MapDifference.ValueDifference<Plugin>> differentPlugins =
      Maps.difference(existingPlugins, pluginsToAdd).entriesDiffering();
    Preconditions.checkArgument(differentPlugins.isEmpty(),
                                "Plugins %s have been used already. Use different ids or remove duplicates",
                                differentPlugins.entrySet());
    extraPlugins.putAll(pluginsToAdd);
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                         PluginSelector selector) {
    return pluginConfigurer.usePlugin(pluginType, pluginName, pluginId, properties, selector);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                                     PluginSelector selector) {
    return pluginConfigurer.usePluginClass(pluginType, pluginName, pluginId, properties, selector);
  }

  @Override
  public Map<String, String> evaluateMacros(Map<String, String> properties, MacroEvaluator evaluator,
                                            MacroParserOptions options) throws InvalidMacroException {
    return pluginConfigurer.evaluateMacros(properties, evaluator, options);
  }
}
