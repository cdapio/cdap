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
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.preview.DataTracerFactory;
import io.cdap.cdap.app.preview.PreviewDataPublisher;
import io.cdap.cdap.app.preview.PreviewMessage;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.NamespaceAlreadyExistsException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
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
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.preview.PreviewConfig;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
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
  private final PreviewDataPublisher previewDataPublisher;
  private final DataTracerFactory dataTracerFactory;
  private final NamespaceAdmin namespaceAdmin;
  private final MetricsCollectionService metricsCollectionService;
  private final ProgramNotificationSubscriberService programNotificationSubscriberService;
  private final LevelDBTableService levelDBTableService;
  private final StructuredTableAdmin structuredTableAdmin;
  private final Path previewIdDirPath;
  private final PreferencesFetcher preferencesFetcher;

  @Inject
  DefaultPreviewRunner(MessagingService messagingService,
                       DatasetOpExecutorService dsOpExecService,
                       DatasetService datasetService,
                       LogAppenderInitializer logAppenderInitializer,
                       ApplicationLifecycleService applicationLifecycleService,
                       ProgramRuntimeService programRuntimeService,
                       ProgramLifecycleService programLifecycleService,
                       PreviewDataPublisher previewDataPublisher,
                       DataTracerFactory dataTracerFactory,
                       NamespaceAdmin namespaceAdmin,
                       MetricsCollectionService metricsCollectionService,
                       ProgramNotificationSubscriberService programNotificationSubscriberService,
                       LevelDBTableService levelDBTableService,
                       StructuredTableAdmin structuredTableAdmin,
                       CConfiguration cConf,
                       PreferencesFetcher preferencesFetcher) {
    this.messagingService = messagingService;
    this.dsOpExecService = dsOpExecService;
    this.datasetService = datasetService;
    this.logAppenderInitializer = logAppenderInitializer;
    this.applicationLifecycleService = applicationLifecycleService;
    this.programRuntimeService = programRuntimeService;
    this.programLifecycleService = programLifecycleService;
    this.previewDataPublisher = previewDataPublisher;
    this.dataTracerFactory = dataTracerFactory;
    this.namespaceAdmin = namespaceAdmin;
    this.metricsCollectionService = metricsCollectionService;
    this.programNotificationSubscriberService = programNotificationSubscriberService;
    this.levelDBTableService = levelDBTableService;
    this.structuredTableAdmin = structuredTableAdmin;
    this.previewIdDirPath = Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR), "previewid").toAbsolutePath();
    this.preferencesFetcher = preferencesFetcher;
  }

  @Override
  public Future<PreviewRequest> startPreview(PreviewRequest previewRequest) throws Exception {
    ProgramId programId = previewRequest.getProgram();
    long submitTimeMillis = RunIds.getTime(programId.getApplication(), TimeUnit.MILLISECONDS);
    previewStarted(programId);

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
    PreviewConfig previewConfig = previewRequest.getAppRequest().getPreview();

    PreferencesDetail preferences = preferencesFetcher.get(programId, true);
    Map<String, String> userProps = new HashMap<>(preferences.getProperties());
    if (previewConfig != null) {
      userProps.putAll(previewConfig.getRuntimeArgs());
    }

    try {
      LOG.debug("Deploying preview application for {}", programId);
      applicationLifecycleService.deployApp(preview.getParent(), preview.getApplication(), preview.getVersion(),
                                            artifactSummary, config, NOOP_PROGRAM_TERMINATOR, null,
                                            request.canUpdateSchedules(), true, userProps);
    } catch (Exception e) {
      PreviewStatus previewStatus = new PreviewStatus(PreviewStatus.Status.DEPLOY_FAILED, submitTimeMillis,
                                                      new BasicThrowable(e), null, null);
      previewTerminated(programId, previewStatus);
      throw e;
    }

    LOG.debug("Starting preview for {}", programId);
    ProgramController controller = programLifecycleService.start(programId, userProps, false, true);

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
            setStatus(programId, new PreviewStatus(PreviewStatus.Status.RUNNING, submitTimeMillis, null,
                                                   startTimeMillis, null));
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
        PreviewStatus previewStatus = new PreviewStatus(status, submitTimeMillis,
                                                        failureCause == null ? null : new BasicThrowable(failureCause),
                                                        startTimeMillis, System.currentTimeMillis());
        previewTerminated(programId, previewStatus);
        if (failureCause == null) {
          resultFuture.complete(previewRequest);
        } else {
          resultFuture.completeExceptionally(failureCause);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    trackPreviewTimeout(previewRequest, timeout, resultFuture);
    PreviewMessage message = new PreviewMessage(PreviewMessage.Type.PROGRAM_RUN_ID, programId.getParent(),
                                                GSON.toJsonTree(controller.getProgramRunId()));
    previewDataPublisher.publish(programId.getParent(), message);
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
    PreviewMessage message = new PreviewMessage(PreviewMessage.Type.STATUS, programId.getParent(),
                                                GSON.toJsonTree(previewStatus));
    previewDataPublisher.publish(programId.getParent(), message);
  }

  @Override
  public void stopPreview(ProgramId programId) throws Exception {
    programLifecycleService.stop(programId);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting preview runner service");
    StoreDefinition.createAllTables(structuredTableAdmin);
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

    Files.createDirectories(previewIdDirPath);

    // Reconcile status for abruptly terminated preview runs
    try (Stream<Path> paths = Files.walk(Paths.get(previewIdDirPath.toString()))) {
      paths.filter(Files::isRegularFile).forEach(path -> {
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
          ProgramId programId = GSON.fromJson(reader, ProgramId.class);
          long submitTimeMillis = RunIds.getTime(programId.getApplication(), TimeUnit.MILLISECONDS);
          PreviewStatus status = new PreviewStatus(
            PreviewStatus.Status.KILLED_BY_EXCEEDING_MEMORY_LIMIT, submitTimeMillis,
            new BasicThrowable(new Exception("Preview runner container killed possibly because of out of memory. " +
                                               "Please try running preview again.")),
            null, null);
          previewTerminated(programId, status);
        } catch (IOException e) {
          LOG.warn("Error reading file {}. Ignoring", path, e);
        }
      });
    }
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

  private void previewStarted(ProgramId programId) {
    Path pid = Paths.get(previewIdDirPath.toString(), programId.getApplication());
    // write to temp file
    try {
      Files.write(pid, Bytes.toBytes(GSON.toJson(programId)));
    } catch (IOException e) {
      LOG.warn("Error saving the program id to file {}.", pid, e);
    }
    long submitTimeMillis = RunIds.getTime(programId.getApplication(), TimeUnit.MILLISECONDS);
    // Set the status to INIT to prepare for preview run.
    setStatus(programId, new PreviewStatus(PreviewStatus.Status.INIT, submitTimeMillis, null,
                                           System.currentTimeMillis(), null));
  }

  private void previewTerminated(ProgramId programId, PreviewStatus previewStatus) {
    Path pid = Paths.get(previewIdDirPath.toString(), programId.getApplication());
    // delete the temp file
    try {
      Files.delete(pid);
    } catch (IOException e) {
      LOG.warn("Error deleting file {} containing preview program id.", pid, e);
    }
    setStatus(programId, previewStatus);
  }
}
