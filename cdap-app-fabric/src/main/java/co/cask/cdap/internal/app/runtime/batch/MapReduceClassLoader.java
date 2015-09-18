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

import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.Delegators;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.batch.distributed.DistributedMapReduceTaskContextProvider;
import co.cask.cdap.internal.app.runtime.batch.distributed.MapReduceContainerLauncher;
import co.cask.cdap.internal.app.runtime.batch.inmemory.InMemoryMapReduceTaskContextProvider;
import co.cask.cdap.internal.app.runtime.plugin.PluginClassLoader;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
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
public class MapReduceClassLoader extends CombineClassLoader implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceClassLoader.class);
  private static final Function<String, String> CLASS_TO_RESOURCE_NAME = new Function<String, String>() {
    @Override
    public String apply(String className) {
      return className.replace('.', '/') + ".class";
    }
  };

  private final Parameters parameters;
  // Supplier for MapReduceTaskContextProvider. Need to wrap it with a supplier to delay calling
  // MapReduceTaskContextProvider.start() since it shouldn't be called in constructor.
  private final Supplier<MapReduceTaskContextProvider> taskContextProviderSupplier;

  /**
   * Finds the {@link MapReduceClassLoader} from the {@link ClassLoader} inside the given {@link Configuration}.
   *
   * @throws IllegalArgumentException if no {@link MapReduceClassLoader} can be found from the {@link Configuration}.
   */
  public static MapReduceClassLoader getFromConfiguration(Configuration configuration) {
    return Delegators.getDelegate(configuration.getClassLoader(), MapReduceClassLoader.class);
  }

  /**
   * Constructor. It creates classloader for MapReduce from information
   * gathered through {@link MapReduceContextConfig}. This method is called by {@link MapReduceContainerLauncher}.
   */
  @SuppressWarnings("unused")
  public MapReduceClassLoader() {
    this(new Parameters(), new TaskContextProviderFactory() {
      @Override
      public MapReduceTaskContextProvider create(CConfiguration cConf, Configuration hConf) {
        Preconditions.checkState(!MapReduceTaskContextProvider.isLocal(hConf), "Expected to be in distributed mode.");
        return new DistributedMapReduceTaskContextProvider(cConf, hConf);
      }
    });
  }

  /**
   * Constructs a ClassLoader that load classes from the programClassLoader, then from the plugin lib ClassLoader,
   * followed by plugin Export-Package ClassLoader and with the system ClassLoader last.
   * This constructor should only be called from {@link MapReduceRuntimeService} only.
   */
  MapReduceClassLoader(final Injector injector, CConfiguration cConf, Configuration hConf,
                       ClassLoader programClassLoader, Map<String, Plugin> plugins,
                       @Nullable PluginInstantiator pluginInstantiator) {
    this(new Parameters(cConf, hConf,
                        programClassLoader, plugins, pluginInstantiator), new TaskContextProviderFactory() {
      @Override
      public MapReduceTaskContextProvider create(CConfiguration cConf, Configuration hConf) {
        Preconditions.checkState(MapReduceTaskContextProvider.isLocal(hConf), "Expected to be in local mode.");
        return new InMemoryMapReduceTaskContextProvider(injector);
      }
    });
  }

  /**
   * Constructs a ClassLoader based on the given {@link Parameters} and also uses the given
   * {@link TaskContextProviderFactory} to create {@link MapReduceTaskContextProvider} on demand.
   */
  private MapReduceClassLoader(final Parameters parameters, final TaskContextProviderFactory contextProviderFactory) {
    super(null, createDelegates(parameters));
    this.parameters = parameters;
    this.taskContextProviderSupplier = Suppliers.memoize(new Supplier<MapReduceTaskContextProvider>() {
      @Override
      public MapReduceTaskContextProvider get() {
        return contextProviderFactory.create(parameters.getCConf(), parameters.getHConf());
      }
    });
  }

  /**
   * Returns the {@link MapReduceTaskContextProvider} associated with this ClassLoader.
   */
  public MapReduceTaskContextProvider getTaskContextProvider() {
    MapReduceTaskContextProvider provider = taskContextProviderSupplier.get();
    // Start the provider. If it is already started, the call will return immediate
    // It will only throw exception if the start failed.
    provider.startAndWait();
    return provider;
  }

  /**
   * Returns the program {@link ProgramClassLoader} used to construct this ClassLoader.
   */
  public ClassLoader getProgramClassLoader() {
    return parameters.getProgramClassLoader();
  }

  /**
   * Returns the {@link PluginInstantiator} associated with this ClassLoader.
   */
  public PluginInstantiator getPluginInstantiator() {
    return parameters.getPluginInstantiator();
  }

  @Override
  public void close() throws Exception {
    try {
      MapReduceTaskContextProvider provider = taskContextProviderSupplier.get();
      Service.State state = provider.state();
      if (state == Service.State.STARTING || state == Service.State.RUNNING) {
        provider.stopAndWait();
      }
    } catch (Exception e) {
      // This is non-fatal, since the container is already done.
      LOG.warn("Exception while stopping MapReduceTaskContextProvider", e);
    }
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

    private final CConfiguration cConf;
    private final Configuration hConf;
    private final ClassLoader programClassLoader;
    private final PluginInstantiator pluginInstantiator;
    private final List<ClassLoader> filteredPluginClassLoaders;

    /**
     * Creates from the Job Configuration
     */
    Parameters() {
      this(createContextConfig());
    }

    Parameters(MapReduceContextConfig contextConfig) {
      this(contextConfig, createProgramClassLoader(contextConfig));
    }

    Parameters(MapReduceContextConfig contextConfig, ClassLoader programClassLoader) {
      this(contextConfig.getCConf(), contextConfig.getHConf(), programClassLoader, contextConfig.getPlugins(),
           createPluginInstantiator(contextConfig, programClassLoader, new File(Constants.Plugin.DIRECTORY)));
    }

    /**
     * Creates from the given ProgramClassLoader with plugin classloading support.
     */
    Parameters(CConfiguration cConf, Configuration hConf,
               ClassLoader programClassLoader,
               Map<String, Plugin> plugins,
               @Nullable PluginInstantiator pluginInstantiator) {
      this.cConf = cConf;
      this.hConf = hConf;
      this.programClassLoader = programClassLoader;
      this.pluginInstantiator = pluginInstantiator;
      this.filteredPluginClassLoaders = createFilteredPluginClassLoaders(plugins, pluginInstantiator);
    }

    ClassLoader getProgramClassLoader() {
      return programClassLoader;
    }

    PluginInstantiator getPluginInstantiator() {
      return pluginInstantiator;
    }

    List<ClassLoader> getFilteredPluginClassLoaders() {
      return filteredPluginClassLoaders;
    }

    CConfiguration getCConf() {
      return cConf;
    }

    Configuration getHConf() {
      return hConf;
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
        return ProgramClassLoader.create(unpackDir, contextConfig.getHConf().getClassLoader(), ProgramType.MAPREDUCE);
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
                                                               ClassLoader programClassLoader, File basePath) {
      return new PluginInstantiator(contextConfig.getCConf(), programClassLoader, basePath);
    }

    /**
     * Returns a list of {@link ClassLoader} for loading plugin classes. The ordering is:
     *
     * Plugin Lib ClassLoader, Plugin Export-Package ClassLoader, ...
     */
    private static List<ClassLoader> createFilteredPluginClassLoaders(Map<String, Plugin> plugins,
                                                                      PluginInstantiator pluginInstantiator) {
      if (plugins.isEmpty()) {
        return ImmutableList.of();
      }

      try {
        Multimap<Plugin, String> artifactPluginClasses = getArtifactPluginClasses(plugins);
        List<ClassLoader> pluginClassLoaders = Lists.newArrayList();
        for (Plugin plugin : plugins.values()) {
          ClassLoader pluginClassLoader = pluginInstantiator.getArtifactClassLoader(plugin.getArtifactId());
          if (pluginClassLoader instanceof PluginClassLoader) {
            Collection<String> allowedClasses = artifactPluginClasses.get(plugin);
            if (!allowedClasses.isEmpty()) {
              pluginClassLoaders.add(createClassFilteredClassLoader(allowedClasses, pluginClassLoader));
            }
            pluginClassLoaders.add(((PluginClassLoader) pluginClassLoader).getExportPackagesClassLoader());
          }
        }
        return ImmutableList.copyOf(pluginClassLoaders);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    private static Multimap<Plugin, String> getArtifactPluginClasses(Map<String, Plugin> plugins) {
      Multimap<Plugin, String> result = HashMultimap.create();
      for (Map.Entry<String, Plugin> entry : plugins.entrySet()) {
        result.put(entry.getValue(), entry.getValue().getPluginClass().getClassName());
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

  /**
   * A private interface to help abstract out which type of {@link MapReduceTaskContextProvider} is created,
   * depending on the runtime environment.
   */
  private interface TaskContextProviderFactory {

    /**
     * Returns a new instance of {@link MapReduceTaskContextProvider}.
     */
    MapReduceTaskContextProvider create(CConfiguration cConf, Configuration hConf);
  }
}
