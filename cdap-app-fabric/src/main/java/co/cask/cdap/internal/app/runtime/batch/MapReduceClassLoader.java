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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.templates.plugins.PluginInfo;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.adapter.PluginClassLoader;
import co.cask.cdap.internal.app.runtime.adapter.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.batch.distributed.MapReduceContainerLauncher;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.templates.AdapterDefinition;
import co.cask.cdap.templates.AdapterPlugin;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A {@link ClassLoader} for YARN application isolation. Classes from
 * the application JARs are loaded in preference to the parent loader.
 *
 * The delegation order is:
 *
 * ProgramClassLoader -> Plugin Lib ClassLoader -> Plugins Export-Package ClassLoaders -> System ClassLoader
 */
public class MapReduceClassLoader extends CombineClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceClassLoader.class);
  private static final Function<String, String> CLASS_TO_RESOURCE_NAME = new Function<String, String>() {
    @Override
    public String apply(String className) {
      return className.replace('.', '/') + ".class";
    }
  };

  private final Parameters parameters;

  /**
   * Constructor. It creates classloader for MapReduce from information
   * gathered through {@link MapReduceContextConfig}. This method is called by {@link MapReduceContainerLauncher}.
   */
  @SuppressWarnings("unused")
  public MapReduceClassLoader() {
    this(new Parameters());
  }

  /**
   * Constructor for using the given program ClassLoader to have higher precedence than the system ClassLoader.
   */
  public MapReduceClassLoader(ClassLoader programClassLoader) {
    this(new Parameters(programClassLoader));
  }

  /**
   * Constructs a ClassLoader that load classes from the programClassLoader, then from the plugin lib ClassLoader,
   * followed by plugin Export-Package ClassLoader and with the sytem ClassLoader last.
   */
  public MapReduceClassLoader(ClassLoader programClassLoader,
                              @Nullable AdapterDefinition adapterSpec,
                              @Nullable PluginInstantiator pluginInstantiator) {
    this(new Parameters(programClassLoader, adapterSpec, pluginInstantiator));
  }

  private MapReduceClassLoader(Parameters parameters) {
    super(null, createDelegates(parameters));
    this.parameters = parameters;
  }

  public ClassLoader getProgramClassLoader() {
    return parameters.getProgramClassLoader();
  }

  @Nullable
  public PluginInstantiator getPluginInstantiator() {
    return parameters.getPluginInstantiator();
  }

  /**
   * Creates the delegating list of ClassLoader.
   */
  private static List<ClassLoader> createDelegates(Parameters parameters) {
    ImmutableList.Builder<ClassLoader> builder = ImmutableList.builder();
    builder.add(parameters.getProgramClassLoader());
    builder.addAll(parameters.getFilteredPluginClassLoaders());
    builder.add(MapReduceClassLoader.class.getClassLoader());

    return builder.build();
  }

  /**
   * A container class for holding parameters for the construction of the MapReduceClassLoader.
   * It is needed because we need all parameters available when calling super constructor.
   */
  private static final class Parameters {

    private final ClassLoader programClassLoader;
    private final PluginInstantiator pluginInstantiator;
    private final List<ClassLoader> filteredPluginClassLoaders;

    /**
     * Creates from the Job Configuration
     */
    Parameters() {
      this(createContextConfig());
    }

    /**
     * Creates from the given ProgramClassLoader without plugin support.
     */
    Parameters(ClassLoader programClassLoader) {
      this.programClassLoader = programClassLoader;
      this.pluginInstantiator = null;
      this.filteredPluginClassLoaders = ImmutableList.of();
    }

    Parameters(MapReduceContextConfig contextConfig) {
      this(contextConfig, createProgramClassLoader(contextConfig));
    }

    Parameters(MapReduceContextConfig contextConfig, ClassLoader programClassLoader) {
      this(programClassLoader, contextConfig.getAdapterSpec(),
           createPluginInstantiator(contextConfig, programClassLoader));
    }

    /**
     * Creates from the given ProgramClassLoader with plugin classloading support.
     */
    Parameters(ClassLoader programClassLoader,
               @Nullable AdapterDefinition adapterSpec,
               @Nullable PluginInstantiator pluginInstantiator) {
      this.programClassLoader = programClassLoader;
      this.pluginInstantiator = pluginInstantiator;
      this.filteredPluginClassLoaders = createFilteredPluginClassLoaders(adapterSpec, pluginInstantiator);
    }

    public ClassLoader getProgramClassLoader() {
      return programClassLoader;
    }

    @Nullable
    public PluginInstantiator getPluginInstantiator() {
      return pluginInstantiator;
    }

    public List<ClassLoader> getFilteredPluginClassLoaders() {
      return filteredPluginClassLoaders;
    }

    private static MapReduceContextConfig createContextConfig() {
      Configuration conf = new Configuration(new YarnConfiguration());
      conf.addResource(new Path(MRJobConfig.JOB_CONF_FILE));
      return new MapReduceContextConfig(conf);
    }

    /**
     * Creates a program {@link ClassLoader} based on the MR job config.
     */
    private static ClassLoader createProgramClassLoader(MapReduceContextConfig contextConfig) {
      // In distributed mode, the program is created by expanding the program jar.
      // The program jar is localized to container with the program jar name.
      // It's ok to expand to a temp dir in local directory, as the YARN container will be gone.
      Location programLocation = new LocalLocationFactory()
        .create(new File(contextConfig.getProgramJarName()).getAbsoluteFile().toURI());
      try {
        File unpackDir = DirUtils.createTempDir(new File(System.getProperty("user.dir")));
        LOG.info("Create ProgramClassLoader from {}, expand to {}", programLocation.toURI(), unpackDir);

        BundleJarUtil.unpackProgramJar(programLocation, unpackDir);
        return ProgramClassLoader.create(unpackDir,
                                         contextConfig.getConfiguration().getClassLoader(), ProgramType.MAPREDUCE);
      } catch (IOException e) {
        LOG.error("Failed to create ProgramClassLoader", e);
        throw Throwables.propagate(e);
      }
    }

    /**
     * Returns a new {@link PluginInstantiator} or {@code null} if no plugin is supported.
     */
    @Nullable
    private static PluginInstantiator createPluginInstantiator(MapReduceContextConfig contextConfig,
                                                               ClassLoader programClassLoader) {
      if (contextConfig.getAdapterSpec() == null) {
        return null;
      }
      return new PluginInstantiator(contextConfig.getConf(), contextConfig.getAdapterSpec().getTemplate(),
                                    programClassLoader);
    }

    /**
     * Returns a list of {@link ClassLoader} for loading plugin classes. The ordering is:
     *
     * Plugin Lib ClassLoader, Plugin Export-Package ClassLoader, ...
     *
     * The ordering of the plugins are defined by the ordering of {@link PluginInfo}.
     */
    private static List<ClassLoader> createFilteredPluginClassLoaders(@Nullable AdapterDefinition adapterSpec,
                                                                      @Nullable PluginInstantiator pluginInstantiator) {
      if (pluginInstantiator == null || adapterSpec == null) {
        return ImmutableList.of();
      }

      try {
        // Gather all explicitly used plugin class names. It is needed for external plugin case,
        Multimap<PluginInfo, String> adapterPluginClasses = getAdapterPluginClasses(adapterSpec);

        // There should be just one plugin lib ClassLoader shared across all plugins
        List<ClassLoader> pluginClassLoaders = Lists.newArrayList();
        pluginClassLoaders.add(pluginInstantiator.getParentClassLoader());

        for (PluginInfo pluginInfo : adapterSpec.getPluginInfos()) {
          ClassLoader pluginClassLoader = pluginInstantiator.getPluginClassLoader(pluginInfo);

          if (pluginClassLoader instanceof PluginClassLoader) {
            // One with filter by class name
            Collection<String> allowedClasses = adapterPluginClasses.get(pluginInfo);
            if (!allowedClasses.isEmpty()) {
              pluginClassLoaders.add(createClassFilteredClassLoader(allowedClasses, pluginClassLoader));
            }
            // One with Export-Package
            pluginClassLoaders.add(((PluginClassLoader) pluginClassLoader).getExportPackagesClassLoader());
          }
        }
        return ImmutableList.copyOf(pluginClassLoaders);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    /**
     * Returns a {@link Multimap} from {@link PluginInfo} to set of classes used by the adapter.
     */
    private static Multimap<PluginInfo, String> getAdapterPluginClasses(AdapterDefinition adapterSpec) {
      Multimap<PluginInfo, String> result = HashMultimap.create();
      for (Map.Entry<String, AdapterPlugin> entry : adapterSpec.getPlugins().entrySet()) {
        result.put(entry.getValue().getPluginInfo(), entry.getValue().getPluginClass().getClassName());
      }
      return result;
    }

    private static ClassLoader createClassFilteredClassLoader(Iterable<String> allowedClasses,
                                                              ClassLoader parentClassLoader) {
      Set<String> allowedResources = ImmutableSet.copyOf(Iterables.transform(allowedClasses, CLASS_TO_RESOURCE_NAME));
      return FilterClassLoader.create(Predicates.in(allowedResources),
        Predicates.<String>alwaysTrue(), parentClassLoader);
    }
  }
}
