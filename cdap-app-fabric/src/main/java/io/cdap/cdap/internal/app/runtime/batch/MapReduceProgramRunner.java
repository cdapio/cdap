/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.mapreduce.MapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.InstantiatorFactory;
import io.cdap.cdap.common.lang.PropertyFieldSetter;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data.ProgramContextAware;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.AbstractProgramRunnerWithPlugin;
import io.cdap.cdap.internal.app.runtime.BasicProgramContext;
import io.cdap.cdap.internal.app.runtime.DataSetFieldSetter;
import io.cdap.cdap.internal.app.runtime.MetricsFieldSetter;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.app.runtime.workflow.NameMappedDatasetFramework;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.internal.lang.Reflections;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Runs {@link MapReduce} programs.
 */
public class MapReduceProgramRunner extends AbstractProgramRunnerWithPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceProgramRunner.class);

  private final Injector injector;
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final NamespacePathLocator locationFactory;
  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;
  private final TransactionSystemClient txSystemClient;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;
  private final MessagingService messagingService;
  private final MetadataReader metadataReader;
  private final FieldLineageWriter fieldLineageWriter;
  private final MetadataPublisher metadataPublisher;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  public MapReduceProgramRunner(Injector injector, CConfiguration cConf, Configuration hConf,
                                NamespacePathLocator locationFactory,
                                DatasetFramework datasetFramework,
                                TransactionSystemClient txSystemClient,
                                MetricsCollectionService metricsCollectionService,
                                DiscoveryServiceClient discoveryServiceClient,
                                SecureStore secureStore, SecureStoreManager secureStoreManager,
                                MessagingService messagingService, MetadataReader metadataReader,
                                MetadataPublisher metadataPublisher, FieldLineageWriter fieldLineageWriter,
                                NamespaceQueryAdmin namespaceQueryAdmin) {
    super(cConf);
    this.injector = injector;
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = datasetFramework;
    this.txSystemClient = txSystemClient;
    this.discoveryServiceClient = discoveryServiceClient;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
    this.messagingService = messagingService;
    this.metadataReader = metadataReader;
    this.metadataPublisher = metadataPublisher;
    this.fieldLineageWriter = fieldLineageWriter;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
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

    Arguments arguments = options.getArguments();
    RunId runId = ProgramRunners.getRunId(options);

    WorkflowProgramInfo workflowInfo = WorkflowProgramInfo.create(arguments);
    DatasetFramework programDatasetFramework = workflowInfo == null ?
      datasetFramework :
      NameMappedDatasetFramework.createFromWorkflowProgramInfo(datasetFramework, workflowInfo, appSpec);

    // Setup dataset framework context, if required
    if (programDatasetFramework instanceof ProgramContextAware) {
      ProgramId programId = program.getId();
      ((ProgramContextAware) programDatasetFramework).setContext(new BasicProgramContext(programId.run(runId)));
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
        new BasicMapReduceContext(program, options, cConf, spec, workflowInfo, discoveryServiceClient,
                                  metricsCollectionService, txSystemClient, programDatasetFramework,
                                  getPluginArchive(options), pluginInstantiator, secureStore, secureStoreManager,
                                  messagingService, metadataReader, metadataPublisher, namespaceQueryAdmin,
                                  fieldLineageWriter);
      closeables.add(context);

      Reflections.visit(mapReduce, mapReduce.getClass(),
                        new PropertyFieldSetter(context.getSpecification().getProperties()),
                        new MetricsFieldSetter(context.getMetrics()),
                        new DataSetFieldSetter(context));

      // note: this sets logging context on the thread level
      LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

      // Set the job queue to hConf if it is provided
      Configuration hConf = new Configuration(this.hConf);
      String schedulerQueue = options.getArguments().getOption(Constants.AppFabric.APP_SCHEDULER_QUEUE);
      if (schedulerQueue != null && !schedulerQueue.isEmpty()) {
        hConf.set(JobContext.QUEUE_NAME, schedulerQueue);
      }

      ClusterMode clusterMode = ProgramRunners.getClusterMode(options);
      Service mapReduceRuntimeService = new MapReduceRuntimeService(injector, cConf, hConf, mapReduce, spec,
                                                                    context, program.getJarLocation(), locationFactory,
                                                                    clusterMode, fieldLineageWriter);
      mapReduceRuntimeService.addListener(createRuntimeServiceListener(closeables), Threads.SAME_THREAD_EXECUTOR);

      ProgramController controller = new MapReduceProgramController(mapReduceRuntimeService, context);

      LOG.debug("Starting MapReduce Job: {}", context);
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

  @Nullable
  private File getPluginArchive(ProgramOptions options) {
    if (!options.getArguments().hasOption(ProgramOptionConstants.PLUGIN_ARCHIVE)) {
      return null;
    }
    return new File(options.getArguments().getOption(ProgramOptionConstants.PLUGIN_ARCHIVE));
  }
}
