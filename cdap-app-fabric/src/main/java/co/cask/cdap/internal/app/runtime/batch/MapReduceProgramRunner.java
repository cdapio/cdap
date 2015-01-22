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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.common.LogWriter;
import co.cask.cdap.common.logging.logback.CAppender;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import javax.annotation.Nullable;

/**
 * Runs {@link MapReduce} programs.
 */
public class MapReduceProgramRunner implements ProgramRunner {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceProgramRunner.class);

  private final StreamAdmin streamAdmin;
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;

  private final TransactionSystemClient txSystemClient;
  private final DiscoveryServiceClient discoveryServiceClient;

  @Inject
  public MapReduceProgramRunner(CConfiguration cConf, Configuration hConf,
                                LocationFactory locationFactory,
                                StreamAdmin streamAdmin,
                                DatasetFramework datasetFramework,
                                TransactionSystemClient txSystemClient,
                                MetricsCollectionService metricsCollectionService,
                                DiscoveryServiceClient discoveryServiceClient) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.streamAdmin = streamAdmin;
    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = datasetFramework;
    this.txSystemClient = txSystemClient;
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Inject (optional = true)
  void setLogWriter(@Nullable LogWriter logWriter) {
    if (logWriter != null) {
      CAppender.logWriter = logWriter;
    }
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
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
    RunId runId = arguments.hasOption(ProgramOptionConstants.RUN_ID)
                    ? RunIds.fromString(arguments.getOption(ProgramOptionConstants.RUN_ID))
                    : RunIds.generate();

    long logicalStartTime = arguments.hasOption(ProgramOptionConstants.LOGICAL_START_TIME)
                                ? Long.parseLong(arguments
                                                   .getOption(ProgramOptionConstants.LOGICAL_START_TIME))
                                : System.currentTimeMillis();

    String workflowBatch = arguments.getOption(ProgramOptionConstants.WORKFLOW_BATCH);
    MapReduce mapReduce;
    try {
      mapReduce = new InstantiatorFactory(false).get(TypeToken.of(program.<MapReduce>getMainClass())).create();
    } catch (Exception e) {
      LOG.error("Failed to instantiate MapReduce class for {}", spec.getClassName(), e);
      throw Throwables.propagate(e);
    }

    final BasicMapReduceContext context =
      new BasicMapReduceContext(program, null, runId, null, options.getUserArguments(),
                                program.getApplicationSpecification().getDatasets().keySet(), spec,
                                logicalStartTime,
                                workflowBatch, discoveryServiceClient, metricsCollectionService,
                                datasetFramework, cConf);


    Reflections.visit(mapReduce, TypeToken.of(mapReduce.getClass()),
                      new PropertyFieldSetter(context.getSpecification().getProperties()),
                      new MetricsFieldSetter(context.getMetrics()),
                      new DataSetFieldSetter(context));

    // note: this sets logging context on the thread level
    LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

    final Service mapReduceRuntimeService = new MapReduceRuntimeService(cConf, hConf, mapReduce, spec, context,
                                                                        program.getJarLocation(), locationFactory,
                                                                        streamAdmin, txSystemClient, datasetFramework);
    ProgramController controller = new MapReduceProgramController(mapReduceRuntimeService, context);

    LOG.info("Starting MapReduce Job: {}", context.toString());
    // if security is not enabled, start the job as the user we're using to access hdfs with.
    // if this is not done, the mapred job will be launched as the user that runs the program
    // runner, which is probably the yarn user. This may cause permissions issues if the program
    // tries to access cdap data. For example, writing to a FileSet will fail, as the yarn user will
    // be running the job, but the data directory will be owned by cdap.
    if (!UserGroupInformation.isSecurityEnabled()) {
      String runAs = cConf.get(Constants.CFG_HDFS_USER);
      try {
        UserGroupInformation.createRemoteUser(runAs)
          .doAs(new PrivilegedExceptionAction<ListenableFuture<Service.State>>() {
            @Override
            public ListenableFuture<Service.State> run() throws Exception {
              return mapReduceRuntimeService.start();
            }
          });
      } catch (Exception e) {
        LOG.error("Exception running mapreduce job as user {}.", runAs, e);
        throw Throwables.propagate(e);
      }
    } else {
      mapReduceRuntimeService.start();
    }
    return controller;
  }
}
