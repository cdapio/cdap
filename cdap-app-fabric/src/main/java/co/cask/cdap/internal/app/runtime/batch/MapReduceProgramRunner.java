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
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.common.LogWriter;
import co.cask.cdap.common.logging.logback.CAppender;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.writer.ProgramContextAware;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.AbstractProgramRunnerWithPlugin;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;
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
  private static final Gson GSON = new Gson();

  private final Injector injector;
  private final StreamAdmin streamAdmin;
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;
  private final Store store;
  private final TransactionSystemClient txSystemClient;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final UsageRegistry usageRegistry;

  @Inject
  public MapReduceProgramRunner(Injector injector, CConfiguration cConf, Configuration hConf,
                                LocationFactory locationFactory,
                                StreamAdmin streamAdmin,
                                DatasetFramework datasetFramework,
                                TransactionSystemClient txSystemClient,
                                MetricsCollectionService metricsCollectionService,
                                DiscoveryServiceClient discoveryServiceClient, Store store,
                                UsageRegistry usageRegistry) {
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
    this.store = store;
    this.usageRegistry = usageRegistry;
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

    final RunId runId = RunIds.fromString(arguments.getOption(ProgramOptionConstants.RUN_ID));

    long logicalStartTime = arguments.hasOption(ProgramOptionConstants.LOGICAL_START_TIME)
                                ? Long.parseLong(arguments
                                                   .getOption(ProgramOptionConstants.LOGICAL_START_TIME))
                                : System.currentTimeMillis();

    String programNameInWorkflow = arguments.getOption(ProgramOptionConstants.PROGRAM_NAME_IN_WORKFLOW);

    WorkflowToken workflowToken = null;
    if (arguments.hasOption(ProgramOptionConstants.WORKFLOW_TOKEN)) {
      workflowToken = GSON.fromJson(arguments.getOption(ProgramOptionConstants.WORKFLOW_TOKEN),
                                    BasicWorkflowToken.class);
    }

    // Setup dataset framework context, if required
    if (datasetFramework instanceof ProgramContextAware) {
      Id.Program programId = program.getId();
      ((ProgramContextAware) datasetFramework).initContext(new Id.Run(programId, runId.getId()));
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
        new BasicMapReduceContext(program, runId, options.getUserArguments(), spec,
                                  logicalStartTime, programNameInWorkflow, workflowToken, discoveryServiceClient,
                                  metricsCollectionService, txSystemClient, datasetFramework, streamAdmin,
                                  getPluginArchive(options), pluginInstantiator);

      Reflections.visit(mapReduce, mapReduce.getClass(),
                        new PropertyFieldSetter(context.getSpecification().getProperties()),
                        new MetricsFieldSetter(context.getMetrics()),
                        new DataSetFieldSetter(context));

      // note: this sets logging context on the thread level
      LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

      final Service mapReduceRuntimeService = new MapReduceRuntimeService(injector, cConf, hConf, mapReduce, spec,
                                                                          context, program.getJarLocation(),
                                                                          locationFactory, streamAdmin,
                                                                          txSystemClient, usageRegistry);
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
    final String workflowName = arguments.getOption(ProgramOptionConstants.WORKFLOW_NAME);
    final String workflowNodeId = arguments.getOption(ProgramOptionConstants.WORKFLOW_NODE_ID);
    final String workflowRunId = arguments.getOption(ProgramOptionConstants.WORKFLOW_RUN_ID);

    return new ServiceListenerAdapter() {
      @Override
      public void starting() {
        //Get start time from RunId
        long startTimeInSeconds = RunIds.getTime(runId, TimeUnit.SECONDS);
        if (startTimeInSeconds == -1) {
          // If RunId is not time-based, use current time as start time
          startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        }
        if (workflowName == null) {
          store.setStart(program.getId(), runId.getId(), startTimeInSeconds, twillRunId,
                         userArgs.asMap(), arguments.asMap());
        } else {
          // Program started by Workflow
          store.setWorkflowProgramStart(program.getId(), runId.getId(), workflowName, workflowRunId, workflowNodeId,
                                        startTimeInSeconds, twillRunId);
        }
      }

      @Override
      public void terminated(Service.State from) {
        closeAllQuietly(closeables);
        if (from == Service.State.STOPPING) {
          // Service was killed
          store.setStop(program.getId(), runId.getId(), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                        ProgramController.State.KILLED.getRunStatus());
        } else {
          // Service completed by itself.
          store.setStop(program.getId(), runId.getId(), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                        ProgramController.State.COMPLETED.getRunStatus());
        }
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        closeAllQuietly(closeables);
        store.setStop(program.getId(), runId.getId(), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                      ProgramController.State.ERROR.getRunStatus());
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
