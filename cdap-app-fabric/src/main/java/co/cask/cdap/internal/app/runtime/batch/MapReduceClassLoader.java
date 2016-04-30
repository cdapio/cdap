/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.Delegators;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.batch.distributed.DistributedMapReduceTaskContextProvider;
import co.cask.cdap.internal.app.runtime.batch.distributed.MapReduceContainerLauncher;
import co.cask.cdap.internal.app.runtime.plugin.PluginClassLoaders;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.cdap.logging.context.MapReduceLoggingContext;
import co.cask.cdap.logging.context.WorkflowProgramLoggingContext;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
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

  private final Parameters parameters;
  // Supplier for MapReduceTaskContextProvider. Need to wrap it with a supplier to delay calling
  // MapReduceTaskContextProvider.start() since it shouldn't be called in constructor.
  private final Supplier<MapReduceTaskContextProvider> taskContextProviderSupplier;
  private MapReduceTaskContextProvider taskContextProvider;

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
        return new MapReduceTaskContextProvider(injector);
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
    this.taskContextProviderSupplier = new Supplier<MapReduceTaskContextProvider>() {
      @Override
      public MapReduceTaskContextProvider get() {
        return contextProviderFactory.create(parameters.getCConf(), parameters.getHConf());
      }
    };
  }

  /**
   * Returns the {@link MapReduceTaskContextProvider} associated with this ClassLoader.
   */
  public MapReduceTaskContextProvider getTaskContextProvider() {
    // Logging context needs to be set in main thread.
    LoggingContext loggingContext = createMapReduceLoggingContext();
    LoggingContextAccessor.setLoggingContext(loggingContext);

    synchronized (this) {
      taskContextProvider = Optional.fromNullable(taskContextProvider).or(taskContextProviderSupplier);
    }
    taskContextProvider.startAndWait();
    return taskContextProvider;
  }

  /**
   * Creates logging context for MapReduce program. If the program is started
   * by Workflow an instance of {@link WorkflowProgramLoggingContext} is returned,
   * otherwise an instance of {@link MapReduceLoggingContext} is returned.
   */
  private LoggingContext createMapReduceLoggingContext() {
    MapReduceContextConfig contextConfig = new MapReduceContextConfig(parameters.getHConf());
    ProgramId programId = contextConfig.getProgramId();
    RunId runId = contextConfig.getRunId();
    WorkflowProgramInfo workflowProgramInfo = contextConfig.getWorkflowProgramInfo();
    if (workflowProgramInfo == null) {
      return new MapReduceLoggingContext(programId.getNamespace(), programId.getApplication(),
                                         programId.getProgram(), runId.getId());
    }
    String workflowId = workflowProgramInfo.getName();
    String workflowRunId = workflowProgramInfo.getRunId().getId();
    return new WorkflowProgramLoggingContext(programId.getNamespace(), programId.getApplication(), workflowId,
                                             workflowRunId, ProgramType.MAPREDUCE, programId.getProgram());
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
  @Nullable
  public PluginInstantiator getPluginInstantiator() {
    return parameters.getPluginInstantiator();
  }

  @Override
  public void close() throws Exception {
    try {
      MapReduceTaskContextProvider provider;
      synchronized (this) {
        provider = taskContextProvider;
      }
      if (provider != null) {
        Service.State state = provider.state();
        if (state == Service.State.STARTING || state == Service.State.RUNNING) {
          provider.stopAndWait();
        }
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
    return ImmutableList.of(
      parameters.getProgramClassLoader(),
      parameters.getFilteredPluginsClassLoader(),
      MapReduceClassLoader.class.getClassLoader()
    );
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
    private final ClassLoader filteredPluginsClassLoader;

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
           createPluginInstantiator(contextConfig, programClassLoader));
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
      this.filteredPluginsClassLoader = PluginClassLoaders.createFilteredPluginsClassLoader(plugins,
                                                                                            pluginInstantiator);
    }

    ClassLoader getProgramClassLoader() {
      return programClassLoader;
    }

    PluginInstantiator getPluginInstantiator() {
      return pluginInstantiator;
    }

    ClassLoader getFilteredPluginsClassLoader() {
      return filteredPluginsClassLoader;
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
      Location programLocation = Locations.toLocation(new File(contextConfig.getProgramJarName()));
      try {
        File unpackDir = DirUtils.createTempDir(new File(System.getProperty("user.dir")));
        LOG.info("Create ProgramClassLoader from {}, expand to {}", programLocation, unpackDir);

        BundleJarUtil.unJar(programLocation, unpackDir);
        return ProgramClassLoader.create(contextConfig.getCConf(), unpackDir,
                                         contextConfig.getHConf().getClassLoader());
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
      String pluginArchive = contextConfig.getHConf().get(Constants.Plugin.ARCHIVE);
      if (pluginArchive == null) {
        return null;
      }
      return new PluginInstantiator(contextConfig.getCConf(), programClassLoader, new File(pluginArchive));
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
