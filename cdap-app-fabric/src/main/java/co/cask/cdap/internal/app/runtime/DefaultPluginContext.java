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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An implementation of {@link PluginContext} that uses {@link PluginInstantiator}.
 */
public class DefaultPluginContext implements PluginContext {
  private static final Logger LOG = LoggerFactory.getLogger(PluginInstantiator.class);

  @Nullable
  private final PluginInstantiator pluginInstantiator;
  private final ProgramId programId;
  private final Map<String, Plugin> plugins;

  /**
   * Constructor.
   *
   * @deprecated Use {@link #DefaultPluginContext(PluginInstantiator, ProgramId, Map)} instead.
   */
  @Deprecated
  public DefaultPluginContext(@Nullable PluginInstantiator pluginInstantiator,
                              Id.Program programId, Map<String, Plugin> plugins) {
    this(pluginInstantiator, programId.toEntityId(), plugins);
  }

  public DefaultPluginContext(@Nullable PluginInstantiator pluginInstantiator,
                              ProgramId programId, Map<String, Plugin> plugins) {
    this.pluginInstantiator = pluginInstantiator;
    this.programId = programId;
    this.plugins = plugins;
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return getPlugin(pluginId).getProperties();
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
    } catch (ClassNotFoundException e) {
      // Shouldn't happen, unless there is bug in file localization
      throw new IllegalArgumentException("Plugin class not found", e);
    } catch (IOException e) {
      // This is fatal, since jar cannot be expanded.
      throw Throwables.propagate(e);
    }
  }

  @Nullable
  @Override
  public <T> T newPluginInstance(String pluginType, String pluginName, PluginProperties properties) {
    // temporary hack - we need a way to load plugin without parents CDAP-9080
    LOG.info("ProgramId is {}", programId);
    LOG.info("Trying to instantiate plugin type {} name {} ", pluginType, pluginName);
    T newPlugin = pluginInstantiator.newInstance(programId.getNamespace(), pluginType, pluginName, properties);
    if (newPlugin != null) {
      LOG.info("instantiated plugin type {} name {} with artifact of plugin {}", pluginType, pluginName);
      return newPlugin;
    }
    // we return null if we cant load the plugin.
    return null;
  }

  private Plugin getPlugin(String pluginId) {
    Plugin plugin = plugins.get(pluginId);
    Preconditions.checkArgument(plugin != null, "Plugin with id %s does not exist in program %s of application %s.",
                                pluginId, programId.getProgram(), programId.getApplication());
    return plugin;
  }
}
