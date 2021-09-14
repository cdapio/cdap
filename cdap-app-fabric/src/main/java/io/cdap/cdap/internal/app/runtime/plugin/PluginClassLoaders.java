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

package io.cdap.cdap.internal.app.runtime.plugin;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.common.lang.CombineClassLoader;
import io.cdap.cdap.common.lang.FilterClassLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A utility class to help constructing ClassLoaders for plugins in program context.
 */
public final class PluginClassLoaders {

  private static final Function<String, String> CLASS_TO_RESOURCE_NAME = new Function<String, String>() {
    @Override
    public String apply(String className) {
      return className.replace('.', '/') + ".class";
    }
  };

  /**
   * Returns a {@link ClassLoader} that only allows loading of plugin classes and plugin exported classes.
   * It should only be used in context when a single ClassLoader is needed to load all different kinds of user classes
   * (e.g. in MapReduce/Spark).
   */
  public static ClassLoader createFilteredPluginsClassLoader(Map<String, Plugin> plugins,
                                                             @Nullable PluginInstantiator pluginInstantiator) {
    if (plugins.isEmpty() || pluginInstantiator == null) {
      return new CombineClassLoader(null);
    }

    try {
      // Gather all explicitly used plugin class names. It is needed for external plugin case.
      Multimap<Plugin, String> artifactPluginClasses = getArtifactPluginClasses(plugins);

      List<ClassLoader> pluginClassLoaders = new ArrayList<>();
      for (Plugin plugin : plugins.values()) {
        ClassLoader pluginClassLoader = pluginInstantiator.getPluginClassLoader(plugin);
        if (pluginClassLoader instanceof PluginClassLoader) {

          // A ClassLoader to allow loading of all plugin classes used by the program.
          Collection<String> pluginClasses = artifactPluginClasses.get(plugin);
          if (!pluginClasses.isEmpty()) {
            pluginClassLoaders.add(createClassFilteredClassLoader(pluginClasses, pluginClassLoader));
          }

          // A ClassLoader to allow all export package classes to be loadable.
          pluginClassLoaders.add(((PluginClassLoader) pluginClassLoader).getExportPackagesClassLoader());
        }
      }
      return new CombineClassLoader(null, pluginClassLoaders);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Returns a {@link Multimap} from {@link Plugin} to set of classes used by the program.
   */
  private static Multimap<Plugin, String> getArtifactPluginClasses(Map<String, Plugin> plugins) {
    Multimap<Plugin, String> result = HashMultimap.create();
    for (Map.Entry<String, Plugin> entry : plugins.entrySet()) {
      result.put(entry.getValue(), entry.getValue().getPluginClass().getClassName());
      result.putAll(entry.getValue(), entry.getValue().getPluginClass().getRuntimeClassNames());
    }
    return result;
  }

  private static ClassLoader createClassFilteredClassLoader(Iterable<String> allowedClasses,
                                                            ClassLoader parentClassLoader) {
    final Set<String> allowedResources = ImmutableSet.copyOf(Iterables.transform(allowedClasses,
                                                                                 CLASS_TO_RESOURCE_NAME));
    return new FilterClassLoader(parentClassLoader, new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return allowedResources.contains(resource);
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return true;
      }
    });
  }

  private PluginClassLoaders() {
    // no-op
  }
}
