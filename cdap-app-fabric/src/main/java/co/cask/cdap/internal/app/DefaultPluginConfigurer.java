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

package co.cask.cdap.internal.app;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.internal.api.DefaultDatasetConfigurer;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.plugin.FindPluginHelper;
import co.cask.cdap.internal.app.runtime.plugin.PluginClassLoader;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.internal.lang.CallerClassSecurityManager;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Contains implementation of methods in {@link PluginConfigurer} thus assisting Program configurers who can extend
 * this class.
 */
public class DefaultPluginConfigurer extends DefaultDatasetConfigurer implements PluginConfigurer {

  private final Id.Artifact artifactId;
  private final ArtifactRepository artifactRepository;
  private final PluginInstantiator pluginInstantiator;
  private final Map<String, Plugin> plugins;
  // this is the namespace that the app will be deployed in, which can be different than the namespace of
  // the artifact. If the artifact is a system artifact, it will have the system namespace.
  protected final Id.Namespace deployNamespace;

  public DefaultPluginConfigurer(Id.Namespace deployNamespace, Id.Artifact artifactId,
                                 ArtifactRepository artifactRepository, PluginInstantiator pluginInstantiator) {
    this.deployNamespace = deployNamespace;
    this.artifactId = artifactId;
    this.artifactRepository = artifactRepository;
    this.pluginInstantiator = pluginInstantiator;
    this.plugins = new HashMap<>();
  }

  public Map<String, Plugin> getPlugins() {
    return plugins;
  }

  public void addPlugins(Map<String, Plugin> plugins) {
    Set<String> duplicatePlugins = Sets.intersection(plugins.keySet(), this.plugins.keySet());
    Preconditions.checkArgument(duplicatePlugins.isEmpty(),
                                "Plugins %s have been used already. Use different ids or remove duplicates",
                                duplicatePlugins);
    this.plugins.putAll(plugins);
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
    Plugin plugin;
    try {
      plugin = findPlugin(pluginType, pluginName, pluginId, properties, selector);
    } catch (PluginNotExistsException e) {
      // Plugin not found, hence return null
      return null;
    } catch (ArtifactNotFoundException e) {
      // this shouldn't happen, it means the artifact for this app does not exist.
      throw new IllegalStateException(
        String.format("Application artifact '%s' no longer exists. Please check if it was deleted.", artifactId));
    }

    try {
      T instance = pluginInstantiator.newInstance(plugin);
      plugins.put(pluginId, plugin);
      return instance;
    } catch (IOException e) {
      // If the plugin jar is deleted without notifying the artifact service.
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
    Plugin plugin;
    try {
      plugin = findPlugin(pluginType, pluginName, pluginId, properties, selector);
    } catch (PluginNotExistsException e) {
      // Plugin not found, hence return null
      return null;
    } catch (ArtifactNotFoundException e) {
      // this shouldn't happen, it means the artifact for this app does not exist.
      throw new IllegalStateException(
        String.format("Application artifact '%s' no longer exists. Please check if it was deleted.", artifactId));
    }

    try {
      Class<T> cls = pluginInstantiator.loadClass(plugin);
      plugins.put(pluginId, plugin);
      return cls;
    } catch (IOException e) {
      // If the plugin jar is deleted without notifying the artifact service.
      return null;
    } catch (ClassNotFoundException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }

  private Plugin findPlugin(String pluginType, String pluginName, String pluginId,
                            PluginProperties properties, PluginSelector selector)
    throws PluginNotExistsException, ArtifactNotFoundException {
    Preconditions.checkArgument(!plugins.containsKey(pluginId),
                                "Plugin of type %s, name %s was already added as id %s.",
                                pluginType, pluginName, pluginId);


    final Class[] callerClasses = CallerClassSecurityManager.getCallerClasses();
    if (callerClasses.length < 3) {
      // This shouldn't happen as there should be someone calling this method.
      throw new IllegalStateException("Invalid call stack.");
    }

    LinkedHashSet<ArtifactId> parents = new LinkedHashSet<>();
    // go through the call stack to get the parent plugins. This is to support plugins of plugins.
    // for example, suppose appX uses pluginY, which uses pluginZ. When we try to use pluginZ, we need to first
    // check if pluginZ extends pluginY. If it does not, we then check if it extends appX.
    // 0 is the CallerClassSecurityManager, 1 is this class, hence 2 is the actual caller
    for (int i = 2; i < callerClasses.length; i++) {
      ClassLoader callerClassloader = callerClasses[i].getClassLoader();
      if (callerClassloader instanceof PluginClassLoader) {
        // if this is the first time we've seen this plugin artifact, it must be a new plugin parent.
        ArtifactId pluginCallerArtifactId = ((PluginClassLoader) callerClassloader).getArtifactId();
        parents.add(pluginCallerArtifactId);
      }
    }

    List<ArtifactId> searchParents = new ArrayList<>(parents);
    searchParents.add(artifactId.toArtifactId());
    PluginNotExistsException lastException = null;
    for (ArtifactId pluginParent : searchParents) {
      Id.Artifact parentId = Id.Artifact.from(deployNamespace, pluginParent);
      ArtifactRange parentRange = new ArtifactRange(parentId.getNamespace().getId(), parentId.getName(),
                                                    parentId.getVersion(), true, parentId.getVersion(), true);
      try {
        Map.Entry<ArtifactDescriptor, PluginClass> pluginEntry =
          artifactRepository.findPlugin(deployNamespace.toEntityId(), parentRange, pluginType, pluginName, selector);
        return FindPluginHelper.getPlugin(parents, pluginEntry, properties, pluginType, pluginName, pluginInstantiator);
      } catch (PluginNotExistsException e) {
        // ignore this in case the plugin extends something higher up in the call stack.
        // For example, suppose the app uses pluginA, which uses pluginB. However, pluginB is a plugin that
        // has the app as its parent and not pluginA as its parent. In this case, we want to keep going up the call
        // stack until we get to the app as the parent, which would be able to find plugin B.
        lastException = e;
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    // this is impossible since we'll always loop at least once above, but putting this here to get rid of the warning
    if (lastException == null) {
      throw new PluginNotExistsException(deployNamespace, pluginType, pluginName);
    }
    throw lastException;
  }

}
