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

import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.preview.DataTracerFactory;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRunner;
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
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.inject.Inject;
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
import javax.annotation.Nullable;

/**
 * Default implementation of the {@link PreviewRunner}.
 */
public class DefaultPreviewRunner extends AbstractIdleService implements PreviewRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultPreviewRunner.class);
  private static final Gson GSON = new Gson();

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

  private volatile PreviewStatus status;
  private volatile boolean killedByTimer;
  private ProgramId programId;
  private ProgramRunId runId;
  private Timer timer;

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
                       StructuredTableRegistry structuredTableRegistry) {
    this.messagingService = messagingService;
    this.dsOpExecService = dsOpExecService;
    this.datasetService = datasetService;
    this.logAppenderInitializer = logAppenderInitializer;
    this.applicationLifecycleService = applicationLifecycleService;
    this.programRuntimeService = programRuntimeService;
    this.programLifecycleService = programLifecycleService;
    this.previewStore = previewStore;
    this.status = null;
    this.dataTracerFactory = dataTracerFactory;
    this.namespaceAdmin = namespaceAdmin;
    this.programStore = programStore;
    this.metricsCollectionService = metricsCollectionService;
    this.metricsQueryHelper = metricsQueryHelper;
    this.programNotificationSubscriberService = programNotificationSubscriberService;
    this.levelDBTableService = levelDBTableService;
    this.structuredTableAdmin = structuredTableAdmin;
    this.structuredTableRegistry = structuredTableRegistry;
  }

  @Override
  public void startPreview(PreviewRequest<?> previewRequest) throws Exception {
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(previewRequest.getProgram().getNamespaceId()).build());
    programId = previewRequest.getProgram();
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
      this.status = new PreviewStatus(PreviewStatus.Status.DEPLOY_FAILED, new BasicThrowable(e), null, null);
      throw e;
    }

    final PreviewConfig previewConfig = previewRequest.getAppRequest().getPreview();
    ProgramController controller = programLifecycleService.start(
      programId, previewConfig == null ? Collections.<String, String>emptyMap() : previewConfig.getRuntimeArgs(),
      false);

    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        setStatus(new PreviewStatus(PreviewStatus.Status.RUNNING, null, System.currentTimeMillis(), null));
        // Only have timer if there is a timeout setting.
        if (previewConfig.getTimeout() != null) {
          timer = new Timer();
          final int timeOutMinutes =  previewConfig.getTimeout();
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
        }
      }

      @Override
      public void completed() {
        setStatus(new PreviewStatus(PreviewStatus.Status.COMPLETED, null, status.getStartTime(),
                                    System.currentTimeMillis()));
        shutDownUnrequiredServices();
      }

      @Override
      public void killed() {
        if (!killedByTimer) {
          setStatus(new PreviewStatus(PreviewStatus.Status.KILLED, null, status.getStartTime(),
                                      System.currentTimeMillis()));
        } else {
          setStatus(new PreviewStatus(PreviewStatus.Status.KILLED_BY_TIMER, null, status.getStartTime(),
                                      System.currentTimeMillis()));
        }
        shutDownUnrequiredServices();
      }

      @Override
      public void error(Throwable cause) {
        setStatus(new PreviewStatus(PreviewStatus.Status.RUN_FAILED, new BasicThrowable(cause), status.getStartTime(),
                                    System.currentTimeMillis()));
        shutDownUnrequiredServices();
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    runId = controller.getProgramRunId();
  }

  private void setStatus(PreviewStatus status) {
    this.status = status;
  }

  @Override
  public PreviewStatus getStatus() {
    return status;
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
    return runId;
  }

  @Override
  public RunRecordMeta getRunRecord() {
    return programStore.getRun(runId);
  }

  @Override
  public MetricsQueryHelper getMetricsQueryHelper() {
    return metricsQueryHelper;
  }

  @Override
  protected void startUp() throws Exception {
    // TODO: CDAP-14838 Ensure preivew has it's own copy of the schema mapping.
    StoreDefinition.createAllTables(structuredTableAdmin, structuredTableRegistry, true);
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
