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

package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.data.ProgramContextAware;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.AbstractProgramRunnerWithPlugin;
import co.cask.cdap.internal.app.runtime.BasicProgramContext;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A {@link ProgramRunner} that runs a {@link Workflow}.
 */
public class WorkflowProgramRunner extends AbstractProgramRunnerWithPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowProgramRunner.class);
  private final ProgramRunnerFactory programRunnerFactory;
  private final ServiceAnnouncer serviceAnnouncer;
  private final InetAddress hostname;
  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final TransactionSystemClient txClient;
  private final RuntimeStore runtimeStore;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;
  private final MessagingService messagingService;
  private final CConfiguration cConf;

  @Inject
  public WorkflowProgramRunner(ProgramRunnerFactory programRunnerFactory, ServiceAnnouncer serviceAnnouncer,
                               @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS) InetAddress hostname,
                               MetricsCollectionService metricsCollectionService, DatasetFramework datasetFramework,
                               DiscoveryServiceClient discoveryServiceClient, TransactionSystemClient txClient,
                               RuntimeStore runtimeStore, CConfiguration cConf, SecureStore secureStore,
                               SecureStoreManager secureStoreManager, MessagingService messagingService) {
    super(cConf);
    this.programRunnerFactory = programRunnerFactory;
    this.serviceAnnouncer = serviceAnnouncer;
    this.hostname = hostname;
    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = datasetFramework;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txClient = txClient;
    this.runtimeStore = runtimeStore;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
    this.messagingService = messagingService;
    this.cConf = cConf;
  }

  @Override
  public ProgramController run(final Program program, final ProgramOptions options) {
    // Extract and verify options
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.WORKFLOW, "Only WORKFLOW process type is supported.");

    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(program.getName());
    Preconditions.checkNotNull(workflowSpec, "Missing WorkflowSpecification for %s", program.getName());

    final RunId runId = ProgramRunners.getRunId(options);

    // Setup dataset framework context, if required
    if (datasetFramework instanceof ProgramContextAware) {
      ProgramId programId = program.getId();
      ((ProgramContextAware) datasetFramework).setContext(new BasicProgramContext(programId.run(runId)));
    }

    // List of all Closeable resources that needs to be cleanup
    final List<Closeable> closeables = new ArrayList<>();
    try {
      PluginInstantiator pluginInstantiator = createPluginInstantiator(options, program.getClassLoader());
      if (pluginInstantiator != null) {
        closeables.add(pluginInstantiator);
      }

      WorkflowDriver driver = new WorkflowDriver(program, options, hostname, workflowSpec, programRunnerFactory,
                                                 metricsCollectionService, datasetFramework, discoveryServiceClient,
                                                 txClient, runtimeStore, cConf, pluginInstantiator,
                                                 secureStore, secureStoreManager, messagingService);

      // Controller needs to be created before starting the driver so that the state change of the driver
      // service can be fully captured by the controller.
      final ProgramController controller = new WorkflowProgramController(program, driver, serviceAnnouncer, runId);
      final String twillRunId = options.getArguments().getOption(ProgramOptionConstants.TWILL_RUN_ID);

      controller.addListener(new AbstractListener() {
        @Override
        public void init(ProgramController.State state, @Nullable Throwable cause) {
          // Get start time from RunId
          long startTimeInSeconds = RunIds.getTime(controller.getRunId(), TimeUnit.SECONDS);
          if (startTimeInSeconds == -1) {
            // If RunId is not time-based, use current time as start time
            startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
          }

          final long finalStartTimeInSeconds = startTimeInSeconds;
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              runtimeStore.setStart(program.getId(), runId.getId(), finalStartTimeInSeconds, twillRunId,
                                    options.getUserArguments().asMap(), options.getArguments().asMap());
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));

          // Check if program already reached to COMPLETED state.
          // This can happen if there is a delay in calling the init listener
          if (state == ProgramController.State.COMPLETED) {
            completed();
          }

          // Check if program already reached to ERROR state
          // This can happen if there is a delay in calling the init listener
          if (state == ProgramController.State.ERROR) {
            error(controller.getFailureCause());
          }
        }

        @Override
        public void completed() {
          LOG.debug("Program {} with run id {} completed successfully.", program.getId(), runId.getId());
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              runtimeStore.setStop(program.getId(), runId.getId(),
                                   TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                   ProgramController.State.COMPLETED.getRunStatus());
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
        }

        @Override
        public void killed() {
          LOG.debug("Program {} with run id {} killed.", program.getId(), runId.getId());
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              runtimeStore.setStop(program.getId(), runId.getId(),
                                   TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                   ProgramController.State.KILLED.getRunStatus());
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
        }

        @Override
        public void suspended() {
          LOG.debug("Suspending Program {} with run id {}.", program.getId(), runId.getId());
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              runtimeStore.setSuspend(program.getId(), runId.getId());
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
        }

        @Override
        public void resuming() {
          LOG.debug("Resuming Program {} {}.", program.getId(), runId.getId());
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              runtimeStore.setResume(program.getId(), runId.getId());
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
        }

        @Override
        public void error(final Throwable cause) {
          LOG.info("Program {} with run id {} stopped because of error {}.", program.getId(), runId.getId(), cause);
          closeAllQuietly(closeables);
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              runtimeStore.setStop(program.getId(), runId.getId(),
                                   TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                   ProgramController.State.ERROR.getRunStatus(), new BasicThrowable(cause));
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      driver.start();

      return controller;
    } catch (Exception e) {
      closeAllQuietly(closeables);
      throw Throwables.propagate(e);
    }
  }

  private void closeAllQuietly(Iterable<Closeable> closeables) {
    for (Closeable c : closeables) {
      Closeables.closeQuietly(c);
    }
  }
}
