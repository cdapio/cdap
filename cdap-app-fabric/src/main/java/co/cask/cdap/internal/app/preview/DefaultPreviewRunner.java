/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.preview;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.preview.DataTracerFactory;
import co.cask.cdap.app.preview.PreviewRequest;
import co.cask.cdap.app.preview.PreviewRunner;
import co.cask.cdap.app.preview.PreviewStatus;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.preview.PreviewStore;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.artifact.SystemArtifactLoader;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.gateway.handlers.store.ProgramStore;
import co.cask.cdap.metrics.query.MetricsQueryHelper;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.artifact.ArtifactVersionRange;
import co.cask.cdap.proto.artifact.preview.PreviewConfig;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
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

  private static final ProgramTerminator NOOP_PROGRAM_TERMINATOR = new ProgramTerminator() {
    @Override
    public void stop(ProgramId programId) throws Exception {
      // no-op
    }
  };

  private final DatasetService datasetService;
  private final LogAppenderInitializer logAppenderInitializer;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final SystemArtifactLoader systemArtifactLoader;
  private final ProgramRuntimeService programRuntimeService;
  private final ProgramLifecycleService programLifecycleService;
  private final PreviewStore previewStore;
  private final DataTracerFactory dataTracerFactory;
  private final NamespaceAdmin namespaceAdmin;
  private final ProgramStore programStore;
  private final MetricsCollectionService metricsCollectionService;
  private final MetricsQueryHelper metricsQueryHelper;

  private volatile PreviewStatus status;
  private volatile boolean killedByTimer;
  private ProgramId programId;
  private ProgramRunId runId;
  private Timer timer;

  @Inject
  DefaultPreviewRunner(DatasetService datasetService, LogAppenderInitializer logAppenderInitializer,
                       ApplicationLifecycleService applicationLifecycleService,
                       SystemArtifactLoader systemArtifactLoader, ProgramRuntimeService programRuntimeService,
                       ProgramLifecycleService programLifecycleService,
                       PreviewStore previewStore, DataTracerFactory dataTracerFactory,
                       NamespaceAdmin namespaceAdmin, ProgramStore programStore,
                       MetricsCollectionService metricsCollectionService, MetricsQueryHelper metricsQueryHelper) {
    this.datasetService = datasetService;
    this.logAppenderInitializer = logAppenderInitializer;
    this.applicationLifecycleService = applicationLifecycleService;
    this.systemArtifactLoader = systemArtifactLoader;
    this.programRuntimeService = programRuntimeService;
    this.programLifecycleService = programLifecycleService;
    this.previewStore = previewStore;
    this.status = null;
    this.dataTracerFactory = dataTracerFactory;
    this.namespaceAdmin = namespaceAdmin;
    this.programStore = programStore;
    this.metricsCollectionService = metricsCollectionService;
    this.metricsQueryHelper = metricsQueryHelper;
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
                stopPreview();
                killedByTimer = true;
              } catch (Exception e) {
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
    return programStore.getRun(programId, runId.getRun());
  }

  @Override
  public MetricsQueryHelper getMetricsQueryHelper() {
    return metricsQueryHelper;
  }

  @Override
  protected void startUp() throws Exception {
    datasetService.startAndWait();

    // It is recommended to initialize log appender after datasetService is started,
    // since log appender instantiates a dataset.
    logAppenderInitializer.initialize();

    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.PREVIEW_HTTP));
    Futures.allAsList(
      applicationLifecycleService.start(),
      systemArtifactLoader.start(),
      programRuntimeService.start(),
      programLifecycleService.start(),
      metricsCollectionService.start()
    ).get();
  }

  @Override
  protected void shutDown() throws Exception {
    shutDownUnrequiredServices();
    datasetService.stopAndWait();
  }

  private void shutDownUnrequiredServices() {
    if (timer != null) {
      timer.cancel();
    }
    programRuntimeService.stopAndWait();
    applicationLifecycleService.stopAndWait();
    systemArtifactLoader.stopAndWait();
    logAppenderInitializer.close();
    metricsCollectionService.stopAndWait();
    programLifecycleService.stopAndWait();
  }
}
