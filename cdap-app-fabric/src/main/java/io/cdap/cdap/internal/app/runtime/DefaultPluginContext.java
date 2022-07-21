/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.proto.id.ProgramId;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An implementation of {@link PluginContext} that uses {@link PluginInstantiator}.
 */
public class DefaultPluginContext implements PluginContext {

  @Nullable
  private final PluginInstantiator pluginInstantiator;
  private final ProgramId programId;
  private final Map<String, Plugin> plugins;
  private final FeatureFlagsProvider featureFlagsProvider;

  public DefaultPluginContext(@Nullable PluginInstantiator pluginInstantiator,
                              ProgramId programId, Map<String, Plugin> plugins,
                              FeatureFlagsProvider featureFlagsProvider) {
    this.pluginInstantiator = pluginInstantiator;
    this.programId = programId;
    this.plugins = plugins;
    this.featureFlagsProvider = featureFlagsProvider;
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return getPlugin(pluginId).getProperties();
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId, MacroEvaluator evaluator) {
    Plugin plugin = getPlugin(pluginId);
    if (pluginInstantiator == null) {
      throw new UnsupportedOperationException("Plugin is not supported");
    }
    return pluginInstantiator.substituteMacros(plugin, evaluator, null);
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    try {
      if (pluginInstantiator == null) {
        throw new UnsupportedOperationException("Plugin is not supported");
      }
      Plugin plugin = getPlugin(pluginId);
      return pluginInstantiator.loadClass(plugin);
    } catch (ClassNotFoundException e) {
      // Shouldn't happen, unless there is bug in file localization
      throw new IllegalArgumentException("Plugin class not found", e);
    } catch (IOException e) {
      // This is fatal, since jar cannot be expanded.
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return newPluginInstance(pluginId, null);
  }

  @Override
  public <T> T newPluginInstance(String pluginId, @Nullable MacroEvaluator evaluator) throws InstantiationException {
    try {
      Plugin plugin = getPlugin(pluginId);
      if (pluginInstantiator == null) {
        throw new UnsupportedOperationException("Plugin is not supported");
      }
      return pluginInstantiator.newInstance(plugin, evaluator);
    } catch (InvalidPluginConfigException e) {
      throw e;
    } catch (ClassNotFoundException e) {
      // Shouldn't happen, unless there is bug in file localization
      throw new IllegalArgumentException("Plugin class not found", e);
    } catch (IOException e) {
      // This is fatal, since jar cannot be expanded.
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isFeatureEnabled(String name) {
    return featureFlagsProvider.isFeatureEnabled(name);
  }

  private Plugin getPlugin(String pluginId) {
    Plugin plugin = plugins.get(pluginId);
    Preconditions.checkArgument(plugin != null, "Plugin with id %s does not exist in program %s of application %s.",
                                pluginId, programId.getProgram(), programId.getApplication());
    return plugin;
  }
}
