/*
 * Copyright © 2016-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.preview.DataTracerFactory;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.NamespaceAlreadyExistsException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.internal.app.deploy.ProgramTerminator;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramNotificationSubscriberService;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.preview.PreviewConfig;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * Default implementation of the {@link PreviewRunner}.
 */
public class DefaultPreviewRunner extends AbstractIdleService implements PreviewRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultPreviewRunner.class);
  private static final Gson GSON = new Gson();
  // default preview running time is 15min
  private static final long PREVIEW_TIMEOUT = TimeUnit.MINUTES.toMillis(15);

  private static final ProgramTerminator NOOP_PROGRAM_TERMINATOR = programId -> {
    // no-op
  };

  private final MessagingService messagingService;
  private final DatasetOpExecutorService dsOpExecService;
  private final DatasetService datasetService;
  private final LogAppenderInitializer logAppenderInitializer;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ProgramRuntimeService programRuntimeService;
  private final ProgramLifecycleService programLifecycleService;
  private final PreviewStore previewStore;
  private final DataTracerFactory dataTracerFactory;
  private final NamespaceAdmin namespaceAdmin;
  private final MetricsCollectionService metricsCollectionService;
  private final ProgramNotificationSubscriberService programNotificationSubscriberService;
  private final LevelDBTableService levelDBTableService;
  private final StructuredTableAdmin structuredTableAdmin;
  private final StructuredTableRegistry structuredTableRegistry;

  @Inject
  DefaultPreviewRunner(MessagingService messagingService,
                       DatasetOpExecutorService dsOpExecService,
                       DatasetService datasetService,
                       LogAppenderInitializer logAppenderInitializer,
                       ApplicationLifecycleService applicationLifecycleService,
                       ProgramRuntimeService programRuntimeService,
                       ProgramLifecycleService programLifecycleService,
                       PreviewStore previewStore, DataTracerFactory dataTracerFactory,
                       NamespaceAdmin namespaceAdmin,
                       MetricsCollectionService metricsCollectionService,
                       ProgramNotificationSubscriberService programNotificationSubscriberService,
                       LevelDBTableService levelDBTableService,
                       StructuredTableAdmin structuredTableAdmin,
                       StructuredTableRegistry structuredTableRegistry) {
    this.messagingService = messagingService;
    this.dsOpExecService = dsOpExecService;
    this.datasetService = datasetService;
    this.logAppenderInitializer = logAppenderInitializer;
    this.applicationLifecycleService = applicationLifecycleService;
    this.programRuntimeService = programRuntimeService;
    this.programLifecycleService = programLifecycleService;
    this.previewStore = previewStore;
    this.dataTracerFactory = dataTracerFactory;
    this.namespaceAdmin = namespaceAdmin;
    this.metricsCollectionService = metricsCollectionService;
    this.programNotificationSubscriberService = programNotificationSubscriberService;
    this.levelDBTableService = levelDBTableService;
    this.structuredTableAdmin = structuredTableAdmin;
    this.structuredTableRegistry = structuredTableRegistry;
  }

  @Override
  public Future<PreviewRequest> startPreview(PreviewRequest previewRequest) throws Exception {
    ProgramId programId = previewRequest.getProgram();

    // Set the status to INIT to prepare for preview run.
    setStatus(programId, new PreviewStatus(PreviewStatus.Status.INIT, null, System.currentTimeMillis(), null));

    AppRequest<?> request = previewRequest.getAppRequest();

    if (request == null) {
      // This shouldn't happen
      throw new IllegalStateException("Preview request shouldn't have an empty application request");
    }

    ArtifactSummary artifactSummary = request.getArtifact();
    ApplicationId preview = programId.getParent();

    try {
      namespaceAdmin.create(new NamespaceMeta.Builder().setName(programId.getNamespaceId()).build());
    } catch (NamespaceAlreadyExistsException e) {
      LOG.debug("Namespace {} already exists.", programId.getNamespaceId());
    }
    DataTracerFactoryProvider.setDataTracerFactory(preview, dataTracerFactory);

    String config = request.getConfig() == null ? null : GSON.toJson(request.getConfig());

    try {
      LOG.debug("Deploying preview application for {}", programId);
      applicationLifecycleService.deployApp(preview.getParent(), preview.getApplication(), preview.getVersion(),
                                            artifactSummary, config, NOOP_PROGRAM_TERMINATOR, null,
                                            request.canUpdateSchedules());
    } catch (Exception e) {
      setStatus(programId, new PreviewStatus(PreviewStatus.Status.DEPLOY_FAILED, new BasicThrowable(e),
                                             null, null));
      throw e;
    }

    LOG.debug("Starting preview for {}", programId);
    final PreviewConfig previewConfig = previewRequest.getAppRequest().getPreview();
    ProgramController controller = programLifecycleService.start(
      programId, previewConfig == null ? Collections.emptyMap() : previewConfig.getRuntimeArgs(), false);

    long startTimeMillis = System.currentTimeMillis();
    AtomicBoolean timeout = new AtomicBoolean();
    CompletableFuture<PreviewRequest> resultFuture = new CompletableFuture<>();
    controller.addListener(new AbstractListener() {

      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        switch (currentState) {
          case STARTING:
          case ALIVE:
          case STOPPING:
            setStatus(programId, new PreviewStatus(PreviewStatus.Status.RUNNING, null, startTimeMillis, null));
            break;
          case COMPLETED:
            terminated(PreviewStatus.Status.COMPLETED, null);
            break;
          case KILLED:
            terminated(PreviewStatus.Status.KILLED, null);
            break;
          case ERROR:
            terminated(PreviewStatus.Status.RUN_FAILED, cause);
            break;
        }
      }

      @Override
      public void completed() {
        terminated(PreviewStatus.Status.COMPLETED, null);
      }

      @Override
      public void killed() {
        terminated(timeout.get() ? PreviewStatus.Status.KILLED_BY_TIMER : PreviewStatus.Status.KILLED, null);
      }

      @Override
      public void error(Throwable cause) {
        terminated(PreviewStatus.Status.RUN_FAILED, cause);
      }

      /**
       * Handle termination of program run.
       *
       * @param status the termination status
       * @param failureCause if the program was terminated due to error, this carries the failure cause
       */
      private void terminated(PreviewStatus.Status status, @Nullable Throwable failureCause) {
        setStatus(programId, new PreviewStatus(status, failureCause == null ? null : new BasicThrowable(failureCause),
                                               startTimeMillis, System.currentTimeMillis()));
        if (failureCause == null) {
          resultFuture.complete(previewRequest);
        } else {
          resultFuture.completeExceptionally(failureCause);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    trackPreviewTimeout(previewRequest, timeout, resultFuture);
    previewStore.setProgramId(controller.getProgramRunId());
    return resultFuture;
  }

  private void trackPreviewTimeout(PreviewRequest previewRequest, AtomicBoolean timeout,
                                   CompletableFuture<PreviewRequest> resultFuture) {
    ProgramId programId = previewRequest.getProgram();
    long timeoutMins = Optional.ofNullable(previewRequest.getAppRequest().getPreview())
      .map(PreviewConfig::getTimeout)
      .map(Integer::longValue).orElse(PREVIEW_TIMEOUT);
    Thread timeoutThread = Threads.createDaemonThreadFactory("preview-timeout-" + programId).newThread(() -> {
      try {
        Uninterruptibles.getUninterruptibly(resultFuture, timeoutMins, TimeUnit.MINUTES);
      } catch (ExecutionException e) {
        // Ignore. This means the preview completed with failure.
      } catch (TimeoutException e) {
        // Timeout, kill the preview
        timeout.set(true);
        try {
          stopPreview(programId);
        } catch (Exception ex) {
          LOG.warn("Failed to stop preview upon timeout of {} minutes", timeoutMins, ex);
        }
      }
    });
    timeoutThread.start();
  }

  private void setStatus(ProgramId programId, PreviewStatus previewStatus) {
    LOG.debug("Setting preview status for {} to {}", programId, previewStatus.getStatus());
    previewStore.setPreviewStatus(programId.getParent(), previewStatus);
  }

  @Override
  public void stopPreview(ProgramId programId) throws Exception {
    programLifecycleService.stop(programId);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting preview runner service");
    StoreDefinition.createAllTables(structuredTableAdmin, structuredTableRegistry, false);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    dsOpExecService.startAndWait();
    datasetService.startAndWait();

    // It is recommended to initialize log appender after datasetService is started,
    // since log appender instantiates a dataset.
    logAppenderInitializer.initialize();

    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.PREVIEW_HTTP));
    Futures.allAsList(
      applicationLifecycleService.start(),
      programRuntimeService.start(),
      metricsCollectionService.start(),
      programNotificationSubscriberService.start()
    ).get();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Stopping preview runner service");
    programRuntimeService.stopAndWait();
    applicationLifecycleService.stopAndWait();
    logAppenderInitializer.close();
    metricsCollectionService.stopAndWait();
    programNotificationSubscriberService.stopAndWait();
    datasetService.stopAndWait();
    dsOpExecService.stopAndWait();
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
    levelDBTableService.close();
  }
}
