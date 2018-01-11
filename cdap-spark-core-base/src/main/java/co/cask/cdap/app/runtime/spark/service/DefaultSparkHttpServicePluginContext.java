/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.service;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.macro.InvalidMacroException;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.api.spark.service.SparkHttpServiceContext;
import co.cask.cdap.api.spark.service.SparkHttpServicePluginContext;
import co.cask.cdap.app.runtime.spark.SparkRuntimeContext;
import co.cask.cdap.app.runtime.spark.SparkRuntimeContextProvider;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.DefaultPluginConfigurer;
import co.cask.cdap.internal.app.PluginWithLocation;
import co.cask.cdap.internal.app.runtime.plugin.PluginClassLoader;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskCompletionListener;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A serialable implementation of {@link SparkHttpServicePluginContext}.
 */
public class DefaultSparkHttpServicePluginContext implements SparkHttpServicePluginContext, Externalizable {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSparkHttpServicePluginContext.class);
  private static final Type PLUGINS_TYPE = new TypeToken<Map<String, PluginWithLocation>>() { }.getType();

  private final SparkRuntimeContext runtimeContext;
  private final Map<String, PluginWithLocation> extraPlugins;
  private final PluginInstantiator pluginInstantiator;
  private final DefaultPluginConfigurer pluginConfigurer;

  /**
   * Constructor for deserialization. Shouldn't be called directly.
   */
  public DefaultSparkHttpServicePluginContext() throws IOException {
    this.runtimeContext = SparkRuntimeContextProvider.get();
    this.pluginInstantiator = createPluginsInstantiator(runtimeContext);
    this.pluginConfigurer = null;
    this.extraPlugins = new HashMap<>();

    // Each deserizliaed instance of this class should be used for the current task excution only,
    // hence we can do the cleanup on task completion.
    TaskContext.get().addTaskCompletionListener(new TaskCompletionListener() {
      @Override
      public void onTaskCompletion(TaskContext context) {
        Closeables.closeQuietly(pluginInstantiator);
      }
    });
  }

  /**
   * Constructor. This should be called from the service handler via
   * the {@link SparkHttpServiceContext#getPluginContext()} method.
   */
  public DefaultSparkHttpServicePluginContext(SparkRuntimeContext runtimeContext) throws IOException {
    this.runtimeContext = runtimeContext;
    this.pluginInstantiator = createPluginsInstantiator(runtimeContext);
    this.pluginConfigurer = new DefaultPluginConfigurer(runtimeContext.getArtifactId(),
                                                        runtimeContext.getProgram().getId().getNamespaceId(),
                                                        pluginInstantiator,
                                                        runtimeContext.getPluginFinder());
    this.extraPlugins = null;
  }

  /**
   * Creates a {@link PluginInstantiator} instance.
   */
  private static PluginInstantiator createPluginsInstantiator(SparkRuntimeContext runtimeContext) throws IOException {
    CConfiguration cConf = runtimeContext.getCConfiguration();
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    File pluginsDir = Files.createTempDirectory(tmpDir.toPath(), "plugins").toFile();
    return new PluginInstantiator(cConf, runtimeContext.getProgram().getClassLoader(), pluginsDir) {

      @Override
      public PluginClassLoader getPluginClassLoader(ArtifactId artifactId,
                                                    List<ArtifactId> pluginParents) throws IOException {
        try {
          // Try to get it from the runtime context first
          return runtimeContext.getPluginInstantiator().getPluginClassLoader(artifactId, pluginParents);
        } catch (Exception e) {
          // If failed to get from runtime context, try to get it from itself
          return super.getPluginClassLoader(artifactId, pluginParents);
        }
      }

      @Override
      public void close() throws IOException {
        try {
          super.close();
        } finally {
          DirUtils.deleteDirectoryContents(pluginsDir, true);
        }
      }
    };
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId,
                         PluginProperties properties, PluginSelector selector) {
    checkCanConfigure(pluginId);
    return pluginConfigurer.usePlugin(pluginType, pluginName, pluginId, properties, selector);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName,
                                     String pluginId, PluginProperties properties, PluginSelector selector) {
    checkCanConfigure(pluginId);
    return pluginConfigurer.usePluginClass(pluginType, pluginName, pluginId, properties, selector);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    try {
      return runtimeContext.getPluginProperties(pluginId);
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      // Expected if the plugin is not in the runtime context. Keep going.
    }
    return getExtraPlugin(pluginId).getProperties();
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId, MacroEvaluator evaluator) throws InvalidMacroException {
    try {
      return runtimeContext.getPluginProperties(pluginId, evaluator);
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      // Expected if the plugin is not in the runtime context. Keep going.
    }
    return pluginInstantiator.substituteMacros(getExtraPlugin(pluginId), evaluator);
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    // Try to load it from the runtime context
    try {
      return runtimeContext.loadPluginClass(pluginId);
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      // Expected if the plugin is not in the runtime context. Keep going.
    }

    try {
      return pluginInstantiator.loadClass(getExtraPlugin(pluginId));
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
  public <T> T newPluginInstance(String pluginId,
                                 @Nullable MacroEvaluator evaluator) throws InstantiationException,
                                                                            InvalidMacroException {
    // Try to get it from the runtime context
    try {
      return evaluator == null ?
        runtimeContext.newPluginInstance(pluginId) : runtimeContext.newPluginInstance(pluginId, evaluator);
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      // Expected if the plugin in not in the runtime context. Keep going.
    }

    try {
      return pluginInstantiator.newInstance(getExtraPlugin(pluginId), evaluator);
    } catch (ClassNotFoundException e) {
      // Shouldn't happen, unless there is bug in file localization
      throw new IllegalArgumentException("Plugin class not found", e);
    } catch (IOException e) {
      // This is fatal, since jar cannot be expanded.
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    if (pluginConfigurer == null) {
      throw new IllegalStateException("Serialization not supported from an executor");
    }

    byte[] plugins = getGson().toJson(pluginConfigurer.getPlugins(), PLUGINS_TYPE).getBytes(StandardCharsets.UTF_8);
    out.writeInt(plugins.length);
    out.write(plugins);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int len = in.readInt();
    byte[] bytes = new byte[len];
    in.readFully(bytes);
    Map<String, PluginWithLocation> plugins = getGson().fromJson(new InputStreamReader(new ByteArrayInputStream(bytes),
                                                                                       StandardCharsets.UTF_8),
                                                                 PLUGINS_TYPE);
    extraPlugins.putAll(plugins);

    for (PluginWithLocation plugin : plugins.values()) {
      pluginInstantiator.addArtifact(plugin.getArtifactLocation(), plugin.getPlugin().getArtifactId());
    }
  }

  @Override
  public void close() {
    Closeables.closeQuietly(pluginInstantiator);
  }

  /**
   * Checks if a plugin of the given id can be configured through this context object.
   */
  private void checkCanConfigure(String pluginId) {
    if (pluginConfigurer == null) {
      throw new IllegalStateException("The usePlugin method cannot be called in executor");
    }

    // Check if the pluginId is already added during configure time
    try {
      runtimeContext.getPluginProperties(pluginId);
      throw new IllegalStateException("A plugin with id " + pluginId + " has already been used.");
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      // Expected, meaning the pluginId is not used. Keep going.
    }
  }

  /**
   * Returns the {@link Plugin} information for the given plugin id that were configured via this context.
   */
  private Plugin getExtraPlugin(String pluginId) {
    // Get the map of plugins from either the configurer (handler method) or the extra plugins map (executor)
    Map<String, PluginWithLocation> plugins = pluginConfigurer == null ? extraPlugins : pluginConfigurer.getPlugins();
    PluginWithLocation plugin = plugins.get(pluginId);
    if (plugin == null) {
      throw new IllegalArgumentException("Plugin with id " + pluginId + " does not exist");
    }
    return plugin.getPlugin();
  }

  /**
   * Creates a {@link Gson} object for serialization.
   */
  private Gson getGson() {
    return new GsonBuilder()
      .registerTypeAdapter(Location.class, new TypeAdapter<Location>() {
        @Override
        public void write(JsonWriter out, Location value) throws IOException {
          out.value(value.toURI().getPath());
        }

        @Override
        public Location read(JsonReader in) throws IOException {
          return Locations.getLocationFromAbsolutePath(runtimeContext.getLocationFactory(), in.nextString());
        }
      })
      .create();
  }
}
