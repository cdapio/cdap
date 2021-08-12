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

package io.cdap.cdap.internal.app;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.FindPluginHelper;
import io.cdap.cdap.internal.app.runtime.plugin.MacroParser;
import io.cdap.cdap.internal.app.runtime.plugin.PluginClassLoader;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.internal.lang.CallerClassSecurityManager;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Abstract base implementation of {@link PluginConfigurer}.
 */
public class DefaultPluginConfigurer implements PluginConfigurer {

  protected final PluginInstantiator pluginInstantiator;
  private final ArtifactId artifactId;
  private final NamespaceId pluginNamespaceId;
  private final PluginFinder pluginFinder;
  private final Map<String, PluginWithLocation> plugins;

  public DefaultPluginConfigurer(ArtifactId artifactId, NamespaceId pluginNamespaceId,
                                 PluginInstantiator pluginInstantiator, PluginFinder pluginFinder) {
    this.artifactId = artifactId;
    this.pluginNamespaceId = pluginNamespaceId;
    this.pluginInstantiator = pluginInstantiator;
    this.pluginFinder = pluginFinder;
    this.plugins = new HashMap<>();
  }

  public PluginInstantiator getPluginInstantiator() {
    return pluginInstantiator;
  }

  public Map<String, PluginWithLocation> getPlugins() {
    return Collections.unmodifiableMap(plugins);
  }

  @Nullable
  @Override
  public final <T> T usePlugin(String pluginType, String pluginName, String pluginId,
                               PluginProperties properties, PluginSelector selector) {
    try {
      Plugin plugin = addPlugin(pluginType, pluginName, pluginId, properties, selector);
      return pluginInstantiator.newInstance(plugin);
    } catch (PluginNotExistsException | IOException e) {
      return null;
    } catch (ClassNotFoundException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }

  @Nullable
  @Override
  public final <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                           PluginProperties properties, PluginSelector selector) {
    try {
      Plugin plugin = addPlugin(pluginType, pluginName, pluginId, properties, selector);
      return pluginInstantiator.loadClass(plugin);
    } catch (PluginNotExistsException | IOException e) {
      return null;
    } catch (ClassNotFoundException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Map<String, String> evaluateMacros(Map<String, String> properties, MacroEvaluator evaluator,
                                            MacroParserOptions options) throws InvalidMacroException {
    MacroParser macroParser = new MacroParser(evaluator, options);
    Map<String, String> evaluated = new HashMap<>();
    properties.forEach((key, val) -> evaluated.put(key, macroParser.parse(val)));
    return evaluated;
  }

  Plugin addPlugin(String pluginType, String pluginName, String pluginId,
                   PluginProperties properties, PluginSelector selector) throws PluginNotExistsException {
    PluginWithLocation existing = plugins.get(pluginId);
    if (existing != null) {
      throw new IllegalArgumentException(String.format("Plugin of type %s, name %s was already added as id %s.",
                                                       existing.getPlugin().getPluginClass().getType(),
                                                       existing.getPlugin().getPluginClass().getName(), pluginId));
    }

    final Class[] callerClasses = CallerClassSecurityManager.getCallerClasses();
    if (callerClasses.length < 3) {
      // This shouldn't happen as there should be someone calling this method.
      throw new IllegalStateException("Invalid call stack.");
    }

    Set<ArtifactId> parents = new LinkedHashSet<>();
    // go through the call stack to get the parent plugins. This is to support plugins of plugins.
    // for example, suppose appX uses pluginY, which uses pluginZ. When we try to use pluginZ, we need to first
    // check if pluginZ extends pluginY. If it does not, we then check if it extends appX.
    // 0 is the CallerClassSecurityManager, 1 is this class, hence 2 is the actual caller
    for (int i = 2; i < callerClasses.length; i++) {
      ClassLoader classloader = callerClasses[i].getClassLoader();
      if (classloader instanceof PluginClassLoader) {
        // if this is the first time we've seen this plugin artifact, it must be a new plugin parent.
        io.cdap.cdap.api.artifact.ArtifactId pluginCallerArtifactId = ((PluginClassLoader) classloader).getArtifactId();
        parents.add((pluginCallerArtifactId.getScope() == ArtifactScope.SYSTEM ? NamespaceId.SYSTEM : pluginNamespaceId)
                      .artifact(pluginCallerArtifactId.getName(), pluginCallerArtifactId.getVersion().getVersion()));
      }
    }

    PluginNotExistsException exception = null;
    for (ArtifactId parentId : Iterables.concat(parents, Collections.singleton(artifactId))) {
      try {
        Map.Entry<ArtifactDescriptor, PluginClass> pluginEntry = pluginFinder.findPlugin(pluginNamespaceId, parentId,
                                                                                         pluginType, pluginName,
                                                                                         selector);
        Plugin plugin = FindPluginHelper.getPlugin(Iterables.transform(parents, ArtifactId::toApiArtifactId),
                                                   pluginEntry, properties, pluginInstantiator);
        plugins.put(pluginId, new PluginWithLocation(plugin, pluginEntry.getKey().getLocation()));
        return plugin;
      } catch (PluginNotExistsException e) {
        // ignore this in case the plugin extends something higher up in the call stack.
        // For example, suppose the app uses pluginA, which uses pluginB. However, pluginB is a plugin that
        // has the app as its parent and not pluginA as its parent. In this case, we want to keep going up the call
        // stack until we get to the app as the parent, which would be able to find plugin B.
        exception = e;
      }
    }

    throw exception == null ? new PluginNotExistsException(pluginNamespaceId, pluginType, pluginName) : exception;
  }
}
