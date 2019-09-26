/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.preview.DataTracerFactory;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.app.preview.PreviewRunnerModule;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.store.preview.PreviewStore;
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
import io.cdap.cdap.internal.app.store.RunRecordMeta;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.gateway.handlers.store.ProgramStore;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.metrics.query.MetricsQueryHelper;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.preview.PreviewConfig;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
  private final ProgramStore programStore;
  private final MetricsCollectionService metricsCollectionService;
  private final MetricsQueryHelper metricsQueryHelper;
  private final ProgramNotificationSubscriberService programNotificationSubscriberService;
  private final LevelDBTableService levelDBTableService;
  private final StructuredTableAdmin structuredTableAdmin;
  private final StructuredTableRegistry structuredTableRegistry;
  private final ProgramId programId;
  private final CountDownLatch countDownLatch;

  private volatile boolean killedByTimer;
  private Timer timer;
  private volatile long startTimeMillis;

  @Inject
  DefaultPreviewRunner(MessagingService messagingService,
                       DatasetOpExecutorService dsOpExecService,
                       DatasetService datasetService,
                       LogAppenderInitializer logAppenderInitializer,
                       ApplicationLifecycleService applicationLifecycleService,
                       ProgramRuntimeService programRuntimeService,
                       ProgramLifecycleService programLifecycleService,
                       PreviewStore previewStore, DataTracerFactory dataTracerFactory,
                       NamespaceAdmin namespaceAdmin, ProgramStore programStore,
                       MetricsCollectionService metricsCollectionService, MetricsQueryHelper metricsQueryHelper,
                       ProgramNotificationSubscriberService programNotificationSubscriberService,
                       LevelDBTableService levelDBTableService,
                       StructuredTableAdmin structuredTableAdmin,
                       StructuredTableRegistry structuredTableRegistry,
                       @Named(PreviewRunnerModule.PREVIEW_PROGRAM_ID) ProgramId programId) {
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
    this.programStore = programStore;
    this.metricsCollectionService = metricsCollectionService;
    this.metricsQueryHelper = metricsQueryHelper;
    this.programNotificationSubscriberService = programNotificationSubscriberService;
    this.levelDBTableService = levelDBTableService;
    this.structuredTableAdmin = structuredTableAdmin;
    this.structuredTableRegistry = structuredTableRegistry;
    this.programId = programId;
    // if the preview store already has the status information, this means this preview already finished, and it
    // is a restoration, set the count to 0 to allow shut down immediately.
    this.countDownLatch =
      previewStore.getPreviewStatus(programId.getParent()) == null ? new CountDownLatch(1) : new CountDownLatch(0);
  }

  @Override
  public void startPreview(PreviewRequest<?> previewRequest) throws Exception {
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(previewRequest.getProgram().getNamespaceId()).build());
    AppRequest<?> request = previewRequest.getAppRequest();
    ArtifactSummary artifactSummary = request.getArtifact();
    ApplicationId preview = programId.getParent();
    DataTracerFactoryProvider.setDataTracerFactory(preview, dataTracerFactory);

    String config = request.getConfig() == null ? null : GSON.toJson(request.getConfig());

    try {
      applicationLifecycleService.deployApp(preview.getParent(), preview.getApplication(), preview.getVersion(),
                                            artifactSummary, config, NOOP_PROGRAM_TERMINATOR, null,
                                            request.canUpdateSchedules());
    } catch (Exception e) {
      setStatus(new PreviewStatus(PreviewStatus.Status.DEPLOY_FAILED, new BasicThrowable(e), null, null));
      throw e;
    }

    final PreviewConfig previewConfig = previewRequest.getAppRequest().getPreview();
    ProgramController controller = programLifecycleService.start(
      programId, previewConfig == null ? Collections.emptyMap() : previewConfig.getRuntimeArgs(), false);

    startTimeMillis = System.currentTimeMillis();
    CountDownLatch statusLatch = new CountDownLatch(1);
    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        try {
          switch (currentState) {
            case STARTING:
            case ALIVE:
            case STOPPING:
              setStatus(new PreviewStatus(PreviewStatus.Status.RUNNING, null, startTimeMillis, null));
              break;
            case COMPLETED:
              terminated(PreviewStatus.Status.COMPLETED, null);
              return;
            case KILLED:
              terminated(PreviewStatus.Status.KILLED, null);
              return;
            case ERROR:
              terminated(PreviewStatus.Status.RUN_FAILED, cause);
              return;
          }

          long timeOutMinutes = previewConfig != null && previewConfig.getTimeout() != null ?
            previewConfig.getTimeout() : PREVIEW_TIMEOUT;

          timer.schedule(new TimerTask() {
            @Override
            public void run() {
              try {
                LOG.info("Stopping the preview since it has reached running time: {} mins.", timeOutMinutes);
                killedByTimer = true;
                stopPreview();
              } catch (Exception e) {
                killedByTimer = false;
                LOG.debug("Error shutting down the preview run with id: {}", programId);
              }
            }
          }, timeOutMinutes * 60 * 1000);
        } finally {
          statusLatch.countDown();
        }
      }

      @Override
      public void completed() {
        terminated(PreviewStatus.Status.COMPLETED, null);
      }

      @Override
      public void killed() {
        terminated(killedByTimer ? PreviewStatus.Status.KILLED_BY_TIMER : PreviewStatus.Status.KILLED, null);
      }

      @Override
      public void error(Throwable cause) {
        terminated(PreviewStatus.Status.RUN_FAILED, cause);;
      }

      /**
       * Handle termination of program run.
       *
       * @param status the termination status
       * @param failureCause if the program was terminated due to error, this carries the failure cause
       */
      private void terminated(PreviewStatus.Status status, @Nullable Throwable failureCause) {
        setStatus(new PreviewStatus(status, failureCause == null ? null : new BasicThrowable(failureCause),
                                    startTimeMillis, System.currentTimeMillis()));
        shutDownUnrequiredServices();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    previewStore.setProgramId(controller.getProgramRunId());

    // Block until the controller.init is called and have preview status set.
    statusLatch.await();
  }

  private void setStatus(PreviewStatus status) {
    previewStore.setPreviewStatus(programId.getParent(), status);
    if (!status.getStatus().equals(PreviewStatus.Status.RUNNING)) {
      countDownLatch.countDown();
    }
  }

  @Override
  public PreviewStatus getStatus() {
    return previewStore.getPreviewStatus(programId.getParent());
  }

  @Override
  public void stopPreview() throws Exception {
    programLifecycleService.stop(programId);
  }

  @Override
  public Set<String> getTracers() {
    return new HashSet<>();
  }

  @Override
  public Map<String, List<JsonElement>> getData(String tracerName) {
    return previewStore.get(programId.getParent(), tracerName);
  }

  @Override
  public ProgramRunId getProgramRunId() {
    return previewStore.getProgramRunId(programId.getParent());
  }

  @Override
  public RunRecordMeta getRunRecord() {
    return programStore.getRun(previewStore.getProgramRunId(programId.getParent()));
  }

  @Override
  public MetricsQueryHelper getMetricsQueryHelper() {
    return metricsQueryHelper;
  }

  @Override
  protected void startUp() throws Exception {
    StoreDefinition.createAllTables(structuredTableAdmin, structuredTableRegistry, false);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    dsOpExecService.startAndWait();
    datasetService.startAndWait();
    timer = new Timer(programId.getApplication());

    // if there is a preview status in the store, that means this preview already has a run so do not need
    // to start other services
    if (previewStore.getPreviewStatus(programId.getParent()) != null) {
      return;
    }

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
    countDownLatch.await(PREVIEW_TIMEOUT - (System.currentTimeMillis() - startTimeMillis), TimeUnit.MILLISECONDS);
    shutDownUnrequiredServices();
    datasetService.stopAndWait();
    dsOpExecService.stopAndWait();
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
    levelDBTableService.close();
  }

  private void shutDownUnrequiredServices() {
    if (timer != null) {
      timer.cancel();
    }
    programRuntimeService.stopAndWait();
    applicationLifecycleService.stopAndWait();
    logAppenderInitializer.close();
    metricsCollectionService.stopAndWait();
    programNotificationSubscriberService.stopAndWait();
  }
}
