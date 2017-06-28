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

package co.cask.cdap.internal.app.runtime.service;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
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
import co.cask.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.artifact.DefaultArtifactManager;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.services.ServiceHttpServer;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A {@link ProgramRunner} that runs an HTTP Server inside a Service.
 */
public class ServiceProgramRunner extends AbstractProgramRunnerWithPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceProgramRunner.class);
  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final TransactionSystemClient txClient;
  private final ServiceAnnouncer serviceAnnouncer;
  private final RuntimeStore runtimeStore;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;
  private final MessagingService messagingService;
  private final DefaultArtifactManager defaultArtifactManager;

  @Inject
  public ServiceProgramRunner(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                              DatasetFramework datasetFramework, DiscoveryServiceClient discoveryServiceClient,
                              TransactionSystemClient txClient, ServiceAnnouncer serviceAnnouncer,
                              RuntimeStore runtimeStore, SecureStore secureStore, SecureStoreManager secureStoreManager,
                              MessagingService messagingService,
                              DefaultArtifactManager defaultArtifactManager) {
    super(cConf);
    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = datasetFramework;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txClient = txClient;
    this.serviceAnnouncer = serviceAnnouncer;
    this.runtimeStore = runtimeStore;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
    this.messagingService = messagingService;
    this.defaultArtifactManager = defaultArtifactManager;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    int instanceId = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCE_ID, "-1"));
    Preconditions.checkArgument(instanceId >= 0, "Missing instance Id");

    int instanceCount = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCES, "0"));
    Preconditions.checkArgument(instanceCount > 0, "Invalid or missing instance count");

    final RunId runId = ProgramRunners.getRunId(options);
    final ProgramId programId = program.getId();
    final Arguments systemArgs = options.getArguments();
    final Arguments userArgs = options.getUserArguments();
    final String twillRunId = systemArgs.getOption(ProgramOptionConstants.TWILL_RUN_ID);

    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType programType = program.getType();
    Preconditions.checkNotNull(programType, "Missing processor type.");
    Preconditions.checkArgument(programType == ProgramType.SERVICE, "Only Service process type is supported.");

    ServiceSpecification spec = appSpec.getServices().get(program.getName());

    String host = options.getArguments().getOption(ProgramOptionConstants.HOST);
    Preconditions.checkArgument(host != null, "No hostname is provided");

    // Setup dataset framework context, if required
    if (datasetFramework instanceof ProgramContextAware) {
      ((ProgramContextAware) datasetFramework).setContext(new BasicProgramContext(programId.run(runId)));
    }

    final PluginInstantiator pluginInstantiator = createPluginInstantiator(options, program.getClassLoader());
    try {
      ServiceHttpServer component = new ServiceHttpServer(host, program, options, cConf, spec,
                                                          instanceId, instanceCount, serviceAnnouncer,
                                                          metricsCollectionService, datasetFramework,
                                                          txClient, discoveryServiceClient,
                                                          pluginInstantiator, secureStore, secureStoreManager,
                                                          messagingService, defaultArtifactManager);

      // Add a service listener to make sure the plugin instantiator is closed when the http server is finished.
      component.addListener(new ServiceListenerAdapter() {
        @Override
        public void terminated(Service.State from) {
          Closeables.closeQuietly(pluginInstantiator);
        }

        @Override
        public void failed(Service.State from, Throwable failure) {
          Closeables.closeQuietly(pluginInstantiator);
        }
      }, Threads.SAME_THREAD_EXECUTOR);


      final ProgramController controller = new ServiceProgramControllerAdapter(component, program.getId(), runId,
                                                                               spec.getName() + "-" + instanceId);
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
              runtimeStore.setStart(programId, runId.getId(), finalStartTimeInSeconds, twillRunId,
                                    userArgs.asMap(), systemArgs.asMap());
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));

          if (state == ProgramController.State.COMPLETED) {
            completed();
          }
          if (state == ProgramController.State.ERROR) {
            error(controller.getFailureCause());
          }
        }

        @Override
        public void completed() {
          LOG.debug("Program {} completed successfully.", programId);
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              runtimeStore.setStop(programId, runId.getId(),
                                   TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                   ProgramController.State.COMPLETED.getRunStatus());
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
        }

        @Override
        public void killed() {
          LOG.debug("Program {} killed.", programId);
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              runtimeStore.setStop(programId, runId.getId(),
                                   TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                   ProgramController.State.KILLED.getRunStatus());
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
        }

        @Override
        public void suspended() {
          LOG.debug("Suspending Program {} {}.", programId, runId);
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              runtimeStore.setSuspend(programId, runId.getId());
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
        }

        @Override
        public void resuming() {
          LOG.debug("Resuming Program {} {}.", programId, runId);
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              runtimeStore.setResume(programId, runId.getId());
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
        }

        @Override
        public void error(final Throwable cause) {
          LOG.info("Program stopped with error {}, {}", programId, runId, cause);
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              runtimeStore.setStop(programId, runId.getId(),
                                   TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                   ProgramController.State.ERROR.getRunStatus(), new BasicThrowable(cause));
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      component.start();
      return controller;
    } catch (Throwable t) {
      Closeables.closeQuietly(pluginInstantiator);
      throw t;
    }
  }

  private static final class ServiceProgramControllerAdapter extends ProgramControllerServiceAdapter {
    private final ServiceHttpServer service;

    ServiceProgramControllerAdapter(ServiceHttpServer service, ProgramId programId,
                                    RunId runId, String componentName) {
      super(service, programId, runId, componentName);
      this.service = service;
    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {
      super.doCommand(name, value);
      if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Integer)) {
        return;
      }
      service.setInstanceCount((Integer) value);
    }
  }
}
