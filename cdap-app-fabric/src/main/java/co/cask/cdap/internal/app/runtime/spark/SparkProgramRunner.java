/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.RunId;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Runs {@link Spark} programs
 */
public class SparkProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SparkProgramRunner.class);

  private final DatasetFramework datasetFramework;
  private final Configuration hConf;
  private final CConfiguration cConf;
  private final MetricsCollectionService metricsCollectionService;
  private final TransactionSystemClient txSystemClient;
  private final LocationFactory locationFactory;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final StreamAdmin streamAdmin;
  private final Store store;

  @Inject
  public SparkProgramRunner(DatasetFramework datasetFramework, CConfiguration cConf,
                            MetricsCollectionService metricsCollectionService, Configuration hConf,
                            TransactionSystemClient txSystemClient, LocationFactory locationFactory,
                            DiscoveryServiceClient discoveryServiceClient, StreamAdmin streamAdmin, Store store) {
    this.hConf = hConf;
    this.datasetFramework = datasetFramework;
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
    this.locationFactory = locationFactory;
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

    long logicalStartTime = arguments.hasOption(ProgramOptionConstants.LOGICAL_START_TIME)
      ? Long.parseLong(arguments.getOption(ProgramOptionConstants.LOGICAL_START_TIME)) : System.currentTimeMillis();

    String workflowBatch = arguments.getOption(ProgramOptionConstants.WORKFLOW_BATCH);

    Spark spark;
    try {
      spark = new InstantiatorFactory(false).get(TypeToken.of(program.<Spark>getMainClass())).create();
    } catch (Exception e) {
      LOG.error("Failed to instantiate Spark class for {}", spec.getClassName(), e);
      throw Throwables.propagate(e);
    }

    final BasicSparkContext context = new BasicSparkContext(program, runId, options.getUserArguments(),
                                                            appSpec.getDatasets().keySet(), spec,
                                                            logicalStartTime, workflowBatch,
                                                            metricsCollectionService, datasetFramework,
                                                            discoveryServiceClient, streamAdmin);

    LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

    Service sparkRuntimeService = new SparkRuntimeService(cConf, hConf, spark, spec, context,
                                                          program.getJarLocation(), locationFactory,
                                                          txSystemClient);

    sparkRuntimeService.addListener(createRuntimeServiceListener(program, runId, arguments),
                                    Threads.SAME_THREAD_EXECUTOR);
    ProgramController controller = new SparkProgramController(sparkRuntimeService, context);

    LOG.info("Starting Spark Job: {}", context.toString());
    sparkRuntimeService.start();
    return controller;
  }

  /**
   * Creates a service listener to reactor on state changes on {@link SparkRuntimeService}.
   */
  private Service.Listener createRuntimeServiceListener(final Program program, final RunId runId, Arguments arguments) {

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
          store.setStart(program.getId(), runId.getId(), startTimeInSeconds, null, twillRunId);
        } else {
          // Program started by Workflow
          store.setWorkflowProgramStart(program.getId(), runId.getId(), workflowName, workflowRunId, workflowNodeId,
                                        startTimeInSeconds, null, twillRunId);
        }
      }

      @Override
      public void terminated(Service.State from) {
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
        store.setStop(program.getId(), runId.getId(), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                      ProgramController.State.ERROR.getRunStatus());
      }
    };
  }
}
