/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.common.LogWriter;
import co.cask.cdap.common.logging.logback.CAppender;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.writer.ProgramContextAware;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.AbstractProgramRunnerWithPlugin;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.workflow.NameMappedDatasetFramework;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Runs {@link MapReduce} programs.
 */
public class MapReduceProgramRunner extends AbstractProgramRunnerWithPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceProgramRunner.class);

  private final Injector injector;
  private final StreamAdmin streamAdmin;
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final NamespacedLocationFactory locationFactory;
  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;
  private final RuntimeStore runtimeStore;
  private final TransactionSystemClient txSystemClient;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  public MapReduceProgramRunner(Injector injector, CConfiguration cConf, Configuration hConf,
                                NamespacedLocationFactory locationFactory,
                                StreamAdmin streamAdmin,
                                DatasetFramework datasetFramework,
                                TransactionSystemClient txSystemClient,
                                MetricsCollectionService metricsCollectionService,
                                DiscoveryServiceClient discoveryServiceClient, RuntimeStore runtimeStore,
                                SecureStore secureStore, SecureStoreManager secureStoreManager,
                                AuthorizationEnforcer authorizationEnforcer,
                                AuthenticationContext authenticationContext) {
    super(cConf);
    this.injector = injector;
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.streamAdmin = streamAdmin;
    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = datasetFramework;
    this.txSystemClient = txSystemClient;
    this.discoveryServiceClient = discoveryServiceClient;
    this.runtimeStore = runtimeStore;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
  }

  @Inject (optional = true)
  void setLogWriter(@Nullable LogWriter logWriter) {
    if (logWriter != null) {
      CAppender.logWriter = logWriter;
    }
  }

  @Override
  public ProgramController run(final Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.MAPREDUCE, "Only MAPREDUCE process type is supported.");

    MapReduceSpecification spec = appSpec.getMapReduce().get(program.getName());
    Preconditions.checkNotNull(spec, "Missing MapReduceSpecification for %s", program.getName());

    // Optionally get runId. If the map-reduce started by other program (e.g. Workflow), it inherit the runId.
    Arguments arguments = options.getArguments();
    RunId runId = ProgramRunners.getRunId(options);

    WorkflowProgramInfo workflowInfo = WorkflowProgramInfo.create(arguments);
    DatasetFramework programDatasetFramework = workflowInfo == null ?
      datasetFramework :
      NameMappedDatasetFramework.createFromWorkflowProgramInfo(datasetFramework, workflowInfo, appSpec);

    // Setup dataset framework context, if required
    if (programDatasetFramework instanceof ProgramContextAware) {
      Id.Program programId = program.getId();
      ((ProgramContextAware) programDatasetFramework).initContext(new Id.Run(programId, runId.getId()));
    }

    MapReduce mapReduce;
    try {
      mapReduce = new InstantiatorFactory(false).get(TypeToken.of(program.<MapReduce>getMainClass())).create();
    } catch (Exception e) {
      LOG.error("Failed to instantiate MapReduce class for {}", spec.getClassName(), e);
      throw Throwables.propagate(e);
    }

    // List of all Closeable resources that needs to be cleanup
    List<Closeable> closeables = new ArrayList<>();
    try {
      PluginInstantiator pluginInstantiator = createPluginInstantiator(options, program.getClassLoader());
      if (pluginInstantiator != null) {
        closeables.add(pluginInstantiator);
      }

      final BasicMapReduceContext context =
        new BasicMapReduceContext(program, options, spec,
                                  workflowInfo, discoveryServiceClient,
                                  metricsCollectionService, txSystemClient, programDatasetFramework, streamAdmin,
                                  getPluginArchive(options), pluginInstantiator, secureStore, secureStoreManager);

      Reflections.visit(mapReduce, mapReduce.getClass(),
                        new PropertyFieldSetter(context.getSpecification().getProperties()),
                        new MetricsFieldSetter(context.getMetrics()),
                        new DataSetFieldSetter(context));

      // note: this sets logging context on the thread level
      LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

      final Service mapReduceRuntimeService = new MapReduceRuntimeService(injector, cConf, hConf, mapReduce, spec,
                                                                          context, program.getJarLocation(),
                                                                          locationFactory, streamAdmin,
                                                                          txSystemClient, authorizationEnforcer,
                                                                          authenticationContext);
      mapReduceRuntimeService.addListener(
        createRuntimeServiceListener(program, runId, closeables, arguments, options.getUserArguments()),
        Threads.SAME_THREAD_EXECUTOR);

      final ProgramController controller = new MapReduceProgramController(mapReduceRuntimeService, context);

      LOG.info("Starting MapReduce Job: {}", context.toString());
      // if security is not enabled, start the job as the user we're using to access hdfs with.
      // if this is not done, the mapred job will be launched as the user that runs the program
      // runner, which is probably the yarn user. This may cause permissions issues if the program
      // tries to access cdap data. For example, writing to a FileSet will fail, as the yarn user will
      // be running the job, but the data directory will be owned by cdap.
      if (MapReduceTaskContextProvider.isLocal(hConf) || UserGroupInformation.isSecurityEnabled()) {
        mapReduceRuntimeService.start();
      } else {
        ProgramRunners.startAsUser(cConf.get(Constants.CFG_HDFS_USER), mapReduceRuntimeService);
      }
      return controller;
    } catch (Exception e) {
      closeAllQuietly(closeables);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a service listener to reactor on state changes on {@link MapReduceRuntimeService}.
   */
  private Service.Listener createRuntimeServiceListener(final Program program, final RunId runId,
                                                        final Iterable<Closeable> closeables,
                                                        final Arguments arguments, final Arguments userArgs) {

    final String twillRunId = arguments.getOption(ProgramOptionConstants.TWILL_RUN_ID);

    return new ServiceListenerAdapter() {
      @Override
      public void starting() {
        //Get start time from RunId
        long startTimeInSeconds = RunIds.getTime(runId, TimeUnit.SECONDS);
        if (startTimeInSeconds == -1) {
          // If RunId is not time-based, use current time as start time
          startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        }
        runtimeStore.setStart(program.getId(), runId.getId(), startTimeInSeconds, twillRunId, userArgs.asMap(),
                              arguments.asMap());
      }

      @Override
      public void terminated(Service.State from) {
        closeAllQuietly(closeables);
        ProgramRunStatus runStatus = ProgramController.State.COMPLETED.getRunStatus();
        if (from == Service.State.STOPPING) {
          // Service was killed
          runStatus = ProgramController.State.KILLED.getRunStatus();
        }

        runtimeStore.setStop(program.getId(), runId.getId(),
                             TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), runStatus);
      }

      @Override
      public void failed(Service.State from, @Nullable Throwable failure) {
        closeAllQuietly(closeables);
        runtimeStore.setStop(program.getId(), runId.getId(),
                             TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                             ProgramController.State.ERROR.getRunStatus(), new BasicThrowable(failure));
      }
    };
  }

  private void closeAllQuietly(Iterable<Closeable> closeables) {
    for (Closeable c : closeables) {
      Closeables.closeQuietly(c);
    }
  }

  @Nullable
  private File getPluginArchive(ProgramOptions options) {
    if (!options.getArguments().hasOption(ProgramOptionConstants.PLUGIN_ARCHIVE)) {
      return null;
    }
    return new File(options.getArguments().getOption(ProgramOptionConstants.PLUGIN_ARCHIVE));
  }
}
