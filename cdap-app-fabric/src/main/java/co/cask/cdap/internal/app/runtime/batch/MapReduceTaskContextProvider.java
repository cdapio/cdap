/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.program.DefaultProgram;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.ProgramContextAware;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.BasicProgramContext;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.workflow.NameMappedDatasetFramework;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCodec;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Provides access to MapReduceTaskContext for mapreduce job tasks.
 */
public class MapReduceTaskContextProvider extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceTaskContextProvider.class);
  private final Injector injector;
  // Maintain a cache of taskId to MapReduceTaskContext
  // Each task should have it's own instance of MapReduceTaskContext so that different dataset instance will
  // be created for different task, which is needed in local mode since job runs with multiple threads
  private final LoadingCache<ContextCacheKey, BasicMapReduceTaskContext> taskContexts;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;
  private final MapReduceClassLoader mapReduceClassLoader;

  /**
   * Helper method to tell if the MR is running in local mode or not. This method doesn't really belongs to this
   * class, but currently there is no better place for it.
   */
  static boolean isLocal(Configuration hConf) {
    String mrFramework = hConf.get(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    return MRConfig.LOCAL_FRAMEWORK_NAME.equals(mrFramework);
  }

  /**
   * Creates an instance with the given {@link Injector} that will be used for getting service instances.
   */
  protected MapReduceTaskContextProvider(Injector injector, MapReduceClassLoader mapReduceClassLoader) {
    this.injector = injector;
    this.taskContexts = CacheBuilder.newBuilder().build(createCacheLoader(injector));
    this.authorizationEnforcer = injector.getInstance(AuthorizationEnforcer.class);
    this.authenticationContext = injector.getInstance(AuthenticationContext.class);
    this.mapReduceClassLoader = mapReduceClassLoader;
  }

  protected Injector getInjector() {
    return injector;
  }

  @Override
  protected void startUp() throws Exception {
    // no-op
  }

  @Override
  protected void shutDown() throws Exception {
    // Close all the contexts to release resources
    for (BasicMapReduceTaskContext context : taskContexts.asMap().values()) {
      try {
        context.close();
      } catch (Exception e) {
        LOG.warn("Exception when closing context {}", context, e);
      }
    }
  }

  /**
   * Returns the {@link BasicMapReduceTaskContext} for the given task.
   */
  public final <K, V> BasicMapReduceTaskContext<K, V> get(TaskAttemptContext taskAttemptContext) {
    return get(new ContextCacheKey(taskAttemptContext));
  }

  /**
   * Returns the {@link BasicMapReduceTaskContext} for the given configuration. Since TaskAttemptContext is not
   * provided, the returned MapReduceTaskContext will not have Metrics available.
   * */
  public final <K, V> BasicMapReduceTaskContext<K, V> get(Configuration configuration) {
    return get(new ContextCacheKey(null, configuration));
  }

  private <K, V> BasicMapReduceTaskContext<K, V> get(ContextCacheKey key) {
    @SuppressWarnings("unchecked")
    BasicMapReduceTaskContext<K, V> context = (BasicMapReduceTaskContext<K, V>) taskContexts.getUnchecked(key);
    return context;
  }


  /**
   * Creates a {@link Program} instance based on the information from the {@link MapReduceContextConfig}, using
   * the given program ClassLoader.
   */
  private Program createProgram(MapReduceContextConfig contextConfig, ClassLoader programClassLoader) {
    Location programLocation;
    LocationFactory locationFactory = new LocalLocationFactory();

    if (isLocal(contextConfig.getHConf())) {
      // Just create a local location factory. It's for temp usage only as the program location is always absolute.
      programLocation = locationFactory.create(contextConfig.getProgramJarURI());
    } else {
      // In distributed mode, the program jar is localized to the container
      programLocation = locationFactory.create(new File(contextConfig.getProgramJarName()).getAbsoluteFile().toURI());
    }

    return new DefaultProgram(new ProgramDescriptor(contextConfig.getProgramId(),
                                                    contextConfig.getApplicationSpecification()),
                              programLocation, programClassLoader);
  }

  /**
   * Creates a {@link CacheLoader} for the task context cache.
   */
  private CacheLoader<ContextCacheKey, BasicMapReduceTaskContext> createCacheLoader(final Injector injector) {
    final DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    final DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    final SecureStore secureStore = injector.getInstance(SecureStore.class);
    final SecureStoreManager secureStoreManager = injector.getInstance(SecureStoreManager.class);
    final MessagingService messagingService = injector.getInstance(MessagingService.class);
    // Multiple instances of BasicMapReduceTaskContext can share the same program.
    final AtomicReference<Program> programRef = new AtomicReference<>();

    return new CacheLoader<ContextCacheKey, BasicMapReduceTaskContext>() {
      @Override
      public BasicMapReduceTaskContext load(ContextCacheKey key) throws Exception {
        TaskAttemptID taskAttemptId = key.getTaskAttemptID();
        // taskAttemptId could be null if used from a org.apache.hadoop.mapreduce.Partitioner or
        // from a org.apache.hadoop.io.RawComparator, in which case we can get the JobId from the conf. Note that the
        // JobId isn't in the conf for the OutputCommitter#setupJob method, in which case we use the taskAttemptId
        Path txFile = MainOutputCommitter.getTxFile(key.getConfiguration(),
                                                    taskAttemptId != null ? taskAttemptId.getJobID() : null);
        FileSystem fs = txFile.getFileSystem(key.getConfiguration());
        Preconditions.checkArgument(fs.exists(txFile));

        Transaction tx;
        try (FSDataInputStream txFileInputStream = fs.open(txFile)) {
          byte[] txByteArray = ByteStreams.toByteArray(txFileInputStream);
          tx = new TransactionCodec().decode(txByteArray);
        }

        MapReduceContextConfig contextConfig = new MapReduceContextConfig(key.getConfiguration());
        MapReduceClassLoader classLoader = MapReduceClassLoader.getFromConfiguration(key.getConfiguration());

        Program program = programRef.get();
        if (program == null) {
          // Creation of program is relatively cheap, so just create and do compare and set.
          programRef.compareAndSet(null, createProgram(contextConfig, classLoader.getProgramClassLoader()));
          program = programRef.get();
        }

        WorkflowProgramInfo workflowInfo = contextConfig.getWorkflowProgramInfo();
        DatasetFramework programDatasetFramework = workflowInfo == null ?
          datasetFramework :
          NameMappedDatasetFramework.createFromWorkflowProgramInfo(datasetFramework, workflowInfo,
                                                                   program.getApplicationSpecification());

        // Setup dataset framework context, if required
        if (programDatasetFramework instanceof ProgramContextAware) {
          ProgramRunId programRunId = program.getId().run(ProgramRunners.getRunId(contextConfig.getProgramOptions()));
          ((ProgramContextAware) programDatasetFramework).setContext(new BasicProgramContext(programRunId));
        }

        MapReduceSpecification spec = program.getApplicationSpecification().getMapReduce().get(program.getName());

        MetricsCollectionService metricsCollectionService = null;
        MapReduceMetrics.TaskType taskType = null;
        String taskId = null;

        // taskAttemptId can be null, if used from a org.apache.hadoop.mapreduce.Partitioner or
        // from a org.apache.hadoop.io.RawComparator
        if (taskAttemptId != null) {
          taskId = taskAttemptId.getTaskID().toString();
          if (MapReduceMetrics.TaskType.hasType(taskAttemptId.getTaskType())) {
            taskType = MapReduceMetrics.TaskType.from(taskAttemptId.getTaskType());
            // if this is not for a mapper or a reducer, we don't need the metrics collection service
            metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
          }
        }
        CConfiguration cConf = injector.getInstance(CConfiguration.class);
        TransactionSystemClient txClient = injector.getInstance(TransactionSystemClient.class);
        ProgramOptions options = contextConfig.getProgramOptions();

        ProgramOptions updatedOptions =
          new SimpleProgramOptions(options.getProgramId(), options.getArguments(),
                                   new BasicArguments(
                                     RuntimeArguments.extractScope(
                                       "task", taskType.getId().toLowerCase(),
                                       contextConfig.getProgramOptions().getUserArguments().asMap())),
                                   options.isDebug());
        return new BasicMapReduceTaskContext(
          program, updatedOptions, cConf, taskType, taskId,
          spec, workflowInfo, discoveryServiceClient, metricsCollectionService, txClient,
          tx, programDatasetFramework, classLoader.getPluginInstantiator(),
          contextConfig.getLocalizedResources(), secureStore, secureStoreManager,
          authorizationEnforcer, authenticationContext, messagingService, mapReduceClassLoader
        );
      }
    };
  }

  /**
   * Private class to represent the caching key for the {@link BasicMapReduceTaskContext} instances.
   */
  private static final class ContextCacheKey {

    private final TaskAttemptID taskAttemptID;
    private final Configuration configuration;

    private ContextCacheKey(TaskAttemptContext context) {
      this(context.getTaskAttemptID(), context.getConfiguration());
    }

    private ContextCacheKey(@Nullable TaskAttemptID taskAttemptID, Configuration configuration) {
      this.taskAttemptID = taskAttemptID;
      this.configuration = configuration;
    }

    @Nullable
    TaskAttemptID getTaskAttemptID() {
      return taskAttemptID;
    }

    public Configuration getConfiguration() {
      return configuration;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      // Only compares with the task ID
      ContextCacheKey that = (ContextCacheKey) o;
      return Objects.equals(taskAttemptID, that.taskAttemptID);
    }

    @Override
    public int hashCode() {
      return Objects.hash(taskAttemptID);
    }
  }
}
