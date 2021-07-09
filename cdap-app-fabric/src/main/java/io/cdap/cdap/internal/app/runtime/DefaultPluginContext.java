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
import com.google.common.collect.Iterables;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.internal.app.PluginWithLocation;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.FindPluginHelper;
import io.cdap.cdap.internal.app.runtime.plugin.PluginClassLoader;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.internal.lang.CallerClassSecurityManager;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * An implementation of {@link PluginContext} that uses {@link PluginInstantiator}.
 */
public class DefaultPluginContext implements PluginContext {

  private final ArtifactId artifactId;
  private final PluginFinder pluginFinder;
  @Nullable
  private final PluginInstantiator pluginInstantiator;
  private final ProgramId programId;
  private final Map<String, Plugin> plugins;

  public DefaultPluginContext(@Nullable PluginInstantiator pluginInstantiator,
                              ProgramId programId, Map<String, Plugin> plugins, ArtifactId artifactId,
                              PluginFinder pluginFinder) {
    this.pluginInstantiator = pluginInstantiator;
    this.programId = programId;
    this.plugins = plugins;
    this.artifactId = artifactId;
    this.pluginFinder = pluginFinder;
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
    return pluginInstantiator.substituteMacros(plugin, evaluator);
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

  @Nullable
  @Override
  public <T> Class<T> loadClass(String pluginType, String pluginName, String pluginId,
                         PluginProperties properties) throws IOException, ClassNotFoundException {
    Plugin plugin = null;
    try {
      plugin = addPlugin(pluginType, pluginName, pluginId, properties, new PluginSelector());
    } catch (PluginNotExistsException e) {
      throw new RuntimeException("");
    }
    return pluginInstantiator.loadClass(plugin);
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

  private Plugin getPlugin(String pluginId) {
    Plugin plugin = plugins.get(pluginId);
    Preconditions.checkArgument(plugin != null, "Plugin with id %s does not exist in program %s of application %s.",
                                pluginId, programId.getProgram(), programId.getApplication());
    return plugin;
  }

  private Plugin addPlugin(String pluginType, String pluginName, String pluginId,
                           PluginProperties properties, PluginSelector selector) throws PluginNotExistsException {
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
        parents.add((pluginCallerArtifactId.getScope() == ArtifactScope.SYSTEM ? NamespaceId.SYSTEM :
                       programId.getNamespaceId())
                      .artifact(pluginCallerArtifactId.getName(), pluginCallerArtifactId.getVersion().getVersion()));
      }
    }

    PluginNotExistsException exception = null;
    for (ArtifactId parentId : Iterables.concat(parents, Collections.singleton(artifactId))) {
      try {
        Map.Entry<ArtifactDescriptor, PluginClass> pluginEntry = pluginFinder.findPlugin(programId.getNamespaceId(),
                                                                                         parentId,
                                                                                         pluginType, pluginName,
                                                                                         selector);
        Plugin plugin = FindPluginHelper.getPlugin(Iterables.transform(parents, ArtifactId::toApiArtifactId),
                                                   pluginEntry, properties, pluginInstantiator);
        return plugin;
      } catch (PluginNotExistsException e) {
        // ignore this in case the plugin extends something higher up in the call stack.
        // For example, suppose the app uses pluginA, which uses pluginB. However, pluginB is a plugin that
        // has the app as its parent and not pluginA as its parent. In this case, we want to keep going up the call
        // stack until we get to the app as the parent, which would be able to find plugin B.
        exception = e;
      }
    }

    throw exception == null ? new PluginNotExistsException(programId.getNamespaceId(),
                                                           pluginType, pluginName) : exception;
  }
}
