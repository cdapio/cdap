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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkSpecification;
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
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
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
 * Runs {@link Spark} programs
 */
public class SparkProgramRunner extends AbstractProgramRunnerWithPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(SparkProgramRunner.class);

  private final DatasetFramework datasetFramework;
  private final Configuration hConf;
  private final CConfiguration cConf;
  private final MetricsCollectionService metricsCollectionService;
  private final TransactionSystemClient txSystemClient;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final StreamAdmin streamAdmin;
  private final Store store;

  @Inject
  public SparkProgramRunner(CConfiguration cConf, Configuration hConf, TransactionSystemClient txSystemClient,
                            DatasetFramework datasetFramework, MetricsCollectionService metricsCollectionService,
                            DiscoveryServiceClient discoveryServiceClient, StreamAdmin streamAdmin, Store store) {
    super(cConf);
    this.hConf = hConf;
    this.datasetFramework = datasetFramework;
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
    this.txSystemClient = txSystemClient;
    this.discoveryServiceClient = discoveryServiceClient;
    this.streamAdmin = streamAdmin;
    this.store = store;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    final ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.SPARK, "Only Spark process type is supported.");

    final SparkSpecification spec = appSpec.getSpark().get(program.getName());
    Preconditions.checkNotNull(spec, "Missing SparkSpecification for %s", program.getName());

    // Optionally get runId. If the spark started by other program (e.g. Workflow), it inherit the runId.
    Arguments arguments = options.getArguments();
    RunId runId = RunIds.fromString(arguments.getOption(ProgramOptionConstants.RUN_ID));

    WorkflowProgramInfo workflowInfo = WorkflowProgramInfo.create(arguments);
    DatasetFramework programDatasetFramework = workflowInfo == null ?
      datasetFramework :
      NameMappedDatasetFramework.createFromWorkflowProgramInfo(datasetFramework, workflowInfo, appSpec);

    // Setup dataset framework context, if required
    if (programDatasetFramework instanceof ProgramContextAware) {
      Id.Program programId = program.getId();
      ((ProgramContextAware) programDatasetFramework).initContext(new Id.Run(programId, runId.getId()));
    }

    List<Closeable> closeables = new ArrayList<>();
    try {
      PluginInstantiator pluginInstantiator = createPluginInstantiator(options, program.getClassLoader());
      if (pluginInstantiator != null) {
        closeables.add(pluginInstantiator);
      }

      ClientSparkContext context = new ClientSparkContext(program, runId,
                                                          options.getUserArguments().asMap(),
                                                          txSystemClient, programDatasetFramework,
                                                          discoveryServiceClient, metricsCollectionService,
                                                          getPluginArchive(options), pluginInstantiator, workflowInfo);
      closeables.add(context);
      Spark spark;
      try {
        spark = new InstantiatorFactory(false).get(TypeToken.of(program.<Spark>getMainClass())).create();

        // Fields injection
        Reflections.visit(spark, spark.getClass(),
                          new PropertyFieldSetter(spec.getProperties()),
                          new DataSetFieldSetter(context),
                          new MetricsFieldSetter(context.getMetrics()));
      } catch (Exception e) {
        LOG.error("Failed to instantiate Spark class for {}", spec.getClassName(), e);
        throw Throwables.propagate(e);
      }

      SparkSubmitter submitter = SparkContextConfig.isLocal(hConf) ? new LocalSparkSubmitter()
        : new DistributedSparkSubmitter(options.getArguments().getOption(Constants.AppFabric.APP_SCHEDULER_QUEUE));
      Service sparkRuntimeService = new SparkRuntimeService(
        cConf, hConf, spark, new SparkContextFactory(hConf, context, programDatasetFramework,
                                                     txSystemClient, streamAdmin),
        submitter, program.getJarLocation(), txSystemClient
      );

      sparkRuntimeService.addListener(
        createRuntimeServiceListener(program.getId(), runId, arguments, options.getUserArguments(), closeables),
        Threads.SAME_THREAD_EXECUTOR);
      ProgramController controller = new SparkProgramController(sparkRuntimeService, context);

      LOG.info("Starting Spark Job: {}", context.toString());
      if (SparkContextConfig.isLocal(hConf) || UserGroupInformation.isSecurityEnabled()) {
        sparkRuntimeService.start();
      } else {
        ProgramRunners.startAsUser(cConf.get(Constants.CFG_HDFS_USER), sparkRuntimeService);
      }
      return controller;
    } catch (Throwable t) {
      closeAll(closeables);
      throw Throwables.propagate(t);
    }
  }

  /**
   * Creates a service listener to reactor on state changes on {@link SparkRuntimeService}.
   */
  private Service.Listener createRuntimeServiceListener(final Id.Program programId, final RunId runId,
                                                        final Arguments arguments, final Arguments userArgs,
                                                        final List<Closeable> closeables) {

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
        store.setStart(programId, runId.getId(), startTimeInSeconds, twillRunId, userArgs.asMap(), arguments.asMap());
      }

      @Override
      public void terminated(Service.State from) {
        closeAll(closeables);
        ProgramRunStatus runStatus = ProgramController.State.COMPLETED.getRunStatus();
        if (from == Service.State.STOPPING) {
          // Service was killed
          runStatus = ProgramController.State.KILLED.getRunStatus();
        }
        store.setStop(programId, runId.getId(), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                      runStatus);
      }

      @Override
      public void failed(Service.State from, @Nullable Throwable failure) {
        closeAll(closeables);
        store.setStop(programId, runId.getId(), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                      ProgramController.State.ERROR.getRunStatus(), failure);
      }
    };
  }

  private void closeAll(List<Closeable> closeables) {
    for (Closeable closeable : closeables) {
      Closeables.closeQuietly(closeable);
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
