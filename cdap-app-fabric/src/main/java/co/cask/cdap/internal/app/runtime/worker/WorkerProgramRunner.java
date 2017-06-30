/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.worker;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramEventPublisher;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.stream.StreamWriterFactory;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.data.ProgramContextAware;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractProgramRunnerWithPlugin;
import co.cask.cdap.internal.app.runtime.BasicProgramContext;
import co.cask.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A {@link ProgramRunner} that runs a {@link Worker}.
 */
public class WorkerProgramRunner extends AbstractProgramRunnerWithPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerProgramRunner.class);
  private final CConfiguration cConf;
  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final TransactionSystemClient txClient;
  private final StreamWriterFactory streamWriterFactory;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;
  private final MessagingService messagingService;

  @Inject
  public WorkerProgramRunner(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                             DatasetFramework datasetFramework, DiscoveryServiceClient discoveryServiceClient,
                             TransactionSystemClient txClient, StreamWriterFactory streamWriterFactory,
                             SecureStore secureStore, SecureStoreManager secureStoreManager,
                             MessagingService messagingService) {
    super(cConf);
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = datasetFramework;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txClient = txClient;
    this.streamWriterFactory = streamWriterFactory;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
    this.messagingService = messagingService;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    int instanceId = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCE_ID, "-1"));
    Preconditions.checkArgument(instanceId >= 0, "Missing instance Id");

    int instanceCount = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCES, "0"));
    Preconditions.checkArgument(instanceCount > 0, "Invalid or missing instance count");

    final RunId runId = ProgramRunners.getRunId(options);
    final ProgramId programId = program.getId();
    final Arguments systemArgs = options.getArguments();
    final Arguments userArgs = options.getUserArguments();
    final String twillRunId = systemArgs.getOption(ProgramOptionConstants.TWILL_RUN_ID);

    ProgramType programType = program.getType();
    Preconditions.checkNotNull(programType, "Missing processor type.");
    Preconditions.checkArgument(programType == ProgramType.WORKER, "Only Worker process type is supported.");

    WorkerSpecification workerSpec = appSpec.getWorkers().get(program.getName());
    Preconditions.checkArgument(workerSpec != null, "Missing Worker specification for %s", program.getId());
    String instances = options.getArguments().getOption(ProgramOptionConstants.INSTANCES,
                                                        String.valueOf(workerSpec.getInstances()));

    WorkerSpecification newWorkerSpec = new WorkerSpecification(workerSpec.getClassName(), workerSpec.getName(),
                                                                workerSpec.getDescription(), workerSpec.getProperties(),
                                                                workerSpec.getDatasets(), workerSpec.getResources(),
                                                                Integer.valueOf(instances));

    // Setup dataset framework context, if required
    if (datasetFramework instanceof ProgramContextAware) {
      ((ProgramContextAware) datasetFramework).setContext(new BasicProgramContext(programId.run(runId)));
    }

    final PluginInstantiator pluginInstantiator = createPluginInstantiator(options, program.getClassLoader());
    try {
      BasicWorkerContext context = new BasicWorkerContext(newWorkerSpec, program, options,
                                                          cConf, instanceId, instanceCount,
                                                          metricsCollectionService, datasetFramework, txClient,
                                                          discoveryServiceClient, streamWriterFactory,
                                                          pluginInstantiator, secureStore, secureStoreManager,
                                                          messagingService);

      // Setup Program Event Publisher
      final ProgramEventPublisher programEventPublisher = new ProgramEventPublisher(cConf, messagingService,
                                                                                    programId, runId);

      WorkerDriver worker = new WorkerDriver(program, newWorkerSpec, context);

      // Add a service listener to make sure the plugin instantiator is closed when the worker driver finished.
      worker.addListener(new ServiceListenerAdapter() {
        @Override
        public void starting() {
          //Get start time from RunId
          long startTimeInSeconds = RunIds.getTime(runId, TimeUnit.SECONDS);
          if (startTimeInSeconds == -1) {
            // If RunId is not time-based, use current time as start time
            startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
          }

          final long finalStartTimeInSeconds = startTimeInSeconds;
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              programEventPublisher.running(twillRunId, finalStartTimeInSeconds, userArgs, systemArgs);
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
        }

        @Override
        public void terminated(Service.State from) {
          Closeables.closeQuietly(pluginInstantiator);

          ProgramRunStatus runStatus = ProgramController.State.COMPLETED.getRunStatus();
          if (from == Service.State.STOPPING) {
            // Service was killed
            runStatus = ProgramController.State.KILLED.getRunStatus();
          }

          final ProgramRunStatus finalRunStatus = runStatus;
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              programEventPublisher.stop(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                         finalRunStatus, null);
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
        }

        @Override
        public void failed(Service.State from, @Nullable final Throwable failure) {
          Closeables.closeQuietly(pluginInstantiator);

          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              programEventPublisher.stop(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                         ProgramController.State.ERROR.getRunStatus(), failure);
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      ProgramController controller = new WorkerControllerServiceAdapter(worker, program.getId(), runId,
                                                                        workerSpec.getName() + "-" + instanceId);
      worker.start();
      return controller;
    } catch (Throwable t) {
      Closeables.closeQuietly(pluginInstantiator);
      throw t;
    }
  }

  private static final class WorkerControllerServiceAdapter extends ProgramControllerServiceAdapter {
    private final WorkerDriver workerDriver;

    WorkerControllerServiceAdapter(WorkerDriver workerDriver, ProgramId programId, RunId runId, String componentName) {
      super(workerDriver, programId, runId, componentName);
      this.workerDriver = workerDriver;
    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {
      super.doCommand(name, value);
      if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Integer)) {
        return;
      }

      workerDriver.setInstanceCount((Integer) value);
    }
  }
}
