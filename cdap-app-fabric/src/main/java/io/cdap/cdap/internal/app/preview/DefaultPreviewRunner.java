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
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.inject.Inject;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.preview.DataTracerFactory;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.internal.app.deploy.ProgramTerminator;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.ProgramNotificationSubscriberService;
import io.cdap.cdap.internal.app.services.PropertiesResolver;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.metadata.ApplicationSpecificationFetcher;
import io.cdap.cdap.metrics.query.MetricsQueryHelper;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.preview.PreviewConfig;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;

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
  private final ProgramRuntimeService programRuntimeService;
  private final PreviewStore previewStore;
  private final DataTracerFactory dataTracerFactory;
  private final NamespaceAdmin namespaceAdmin;
  private final MetricsCollectionService metricsCollectionService;
  private final MetricsQueryHelper metricsQueryHelper;
  private final ProgramNotificationSubscriberService programNotificationSubscriberService;
  private final LevelDBTableService levelDBTableService; // ???
  private final StructuredTableAdmin structuredTableAdmin; // ???
  private final StructuredTableRegistry structuredTableRegistry;
  private final PreviewRequest previewRequest;
  private final CompletableFuture<PreviewStatus> completion;
  private final RemoteClient remoteClient;
  private final ProgramStateWriter programStateWriter;
  private final PropertiesResolver propertiesResolver;
  private final ApplicationSpecificationFetcher appSpecFetcher;

  private volatile boolean killedByTimer;
  private Timer timer;
  private volatile long startTimeMillis;

  @Inject
  DefaultPreviewRunner(MessagingService messagingService,
                       DatasetOpExecutorService dsOpExecService,
                       DatasetService datasetService,
                       LogAppenderInitializer logAppenderInitializer,
                       ProgramRuntimeService programRuntimeService,
                       PreviewStore previewStore, DataTracerFactory dataTracerFactory,
                       NamespaceAdmin namespaceAdmin,
                       MetricsCollectionService metricsCollectionService, MetricsQueryHelper metricsQueryHelper,
                       ProgramNotificationSubscriberService programNotificationSubscriberService,
                       LevelDBTableService levelDBTableService,
                       StructuredTableAdmin structuredTableAdmin,
                       StructuredTableRegistry structuredTableRegistry,
                       DiscoveryServiceClient discoveryClient,
                       ProgramStateWriter programStateWriter,
                       PropertiesResolver propertiesResolver,
                       ApplicationSpecificationFetcher applicationDetailFetcher,
                       PreviewRequest previewRequest) {
    this.messagingService = messagingService;
    this.dsOpExecService = dsOpExecService;
    this.datasetService = datasetService;
    this.logAppenderInitializer = logAppenderInitializer;
    this.programRuntimeService = programRuntimeService;
    this.previewStore = previewStore;
    this.dataTracerFactory = dataTracerFactory;
    this.namespaceAdmin = namespaceAdmin;
    this.metricsCollectionService = metricsCollectionService;
    this.metricsQueryHelper = metricsQueryHelper;
    this.programNotificationSubscriberService = programNotificationSubscriberService;
    this.levelDBTableService = levelDBTableService;
    this.structuredTableAdmin = structuredTableAdmin;
    this.structuredTableRegistry = structuredTableRegistry;
    this.previewRequest = previewRequest;
    this.completion = new CompletableFuture<>();
    this.remoteClient = new RemoteClient(discoveryClient,
                                         Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false),
                                         Constants.Gateway.API_VERSION_3);
    this.programStateWriter = programStateWriter;
    this.propertiesResolver = propertiesResolver;
    this.appSpecFetcher = applicationDetailFetcher;
  }

  @Override
  public PreviewRequest getPreviewRequest() {
    return previewRequest;
  }

  @Override
  public void startPreview() throws Exception {
    LOG.debug("wyzhang: startPreview");
    ProgramId programId = previewRequest.getProgram();
    AppRequest<?> request = previewRequest.getAppRequest();

    if (request == null) {
      // This shouldn't happen
      throw new IllegalStateException("Preview request shouldn't have an empty application request");
    }

    ArtifactSummary artifactSummary = request.getArtifact();
    ApplicationId preview = programId.getParent();

    namespaceAdmin.create(new NamespaceMeta.Builder().setName(programId.getNamespaceId()).build());
    DataTracerFactoryProvider.setDataTracerFactory(preview, dataTracerFactory);

    String config = request.getConfig() == null ? null : GSON.toJson(request.getConfig());

    try {
      LOG.debug("Deploying preview application for {}", programId);

      String namespace = preview.getParent().getNamespace();
      String appName = preview.getApplication();
      String appVersion = preview.getVersion();
      String url = String.format("namespaces/%s/apps/%s/versions/%s/create", namespace, appName, appVersion);

      HttpRequest httpRequest = remoteClient.requestBuilder(HttpMethod.POST, url)
        .addHeader(HttpHeaderNames.CONTENT_TYPE.toString(), MediaType.APPLICATION_JSON)
        .withBody(GSON.toJson(request))
        .build();
      HttpResponse httpResponse = remoteClient.execute(httpRequest);
      if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
        LOG.debug("wyzhang: deploying preview application failed req: " + httpRequest.toString());
        LOG.debug("wyzhang: deploying preview application failed resp:" + httpResponse.getResponseBodyAsString());
        throw new Exception(httpResponse.getResponseBodyAsString());
      }
    } catch (Exception e) {
      setStatus(new PreviewStatus(PreviewStatus.Status.DEPLOY_FAILED, new BasicThrowable(e), null, null));
      throw e;
    }

    LOG.debug("Starting preview for {}", programId);
    final PreviewConfig previewConfig = previewRequest.getAppRequest().getPreview();
    ProgramController controller = startProgram(
      programId, previewConfig == null ? Collections.emptyMap() : previewConfig.getRuntimeArgs(), false);

    startTimeMillis = System.currentTimeMillis();
    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
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
              LOG.info("Stopping the preview for {} since it has reached running time: {} mins.",
                       programId, timeOutMinutes);
              killedByTimer = true;
              stopPreview();
            } catch (Exception e) {
              killedByTimer = false;
              LOG.debug("Error shutting down the preview run with id: {}", programId, e);
            }
          }
        }, timeOutMinutes * 60 * 1000);
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
        terminated(PreviewStatus.Status.RUN_FAILED, cause);
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
  }

  private void setStatus(PreviewStatus previewStatus) {
    LOG.debug("Setting preview status for {} to {}", previewRequest.getProgram(), previewStatus.getStatus());
    previewStore.setPreviewStatus(previewRequest.getProgram().getParent(), previewStatus);
    if (previewStatus.getStatus().isEndState()) {
      completion.complete(previewStatus);
    }
  }

  @Override
  public PreviewStatus getStatus() {
    return previewStore.getPreviewStatus(previewRequest.getProgram().getParent());
  }

  @Override
  public void stopPreview() throws Exception {
    if (!completion.isDone()) {
      // TODO: fix nothing to do?
    }
  }

  @Override
  public Set<String> getTracers() {
    return new HashSet<>();
  }

  @Override
  public Map<String, List<JsonElement>> getData(String tracerName) {
    return previewStore.get(previewRequest.getProgram().getParent(), tracerName);
  }

  @Override
  public ProgramRunId getProgramRunId() {
    return previewStore.getProgramRunId(previewRequest.getProgram().getParent());
  }

  @Override
  public RunRecordDetail getRunRecord() {
    try {
      ProgramRunId runId = getProgramRunId();
      if (runId == null) {
        return null;
      }
      return getRunRecordMeta(runId);
    } catch (Exception e) {
      return null;
    }
  }

  RunRecordDetail getRunRecordMeta(ProgramRunId runId) {
    // TODO:
    return null;
  }

  @Override
  public MetricsQueryHelper getMetricsQueryHelper() {
    return metricsQueryHelper;
  }

  @Override
  protected void startUp() throws Exception {
    ProgramId programId = previewRequest.getProgram();

    LOG.debug("Starting preview runner for {}", programId);
    StoreDefinition.createAllTables(structuredTableAdmin, structuredTableRegistry, false);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    dsOpExecService.startAndWait();
    datasetService.startAndWait();
    timer = new Timer(programId.getApplication());

    // if there is a preview status in the store, that means this preview already has a run so do not need
    // to start other services. If the status is running, change it to killed since we are not going to start the
    // preview again.
    ApplicationId appId = programId.getParent();
    PreviewStatus previewStatus = previewStore.getPreviewStatus(appId);
    if (previewStatus != null) {
      if (!previewStatus.getStatus().isEndState()) {
        setStatus(new PreviewStatus(PreviewStatus.Status.KILLED, null,
                                    previewStatus.getStartTime(), System.currentTimeMillis()));
      }
      completion.complete(previewStatus);
      return;
    }

    // Set the status to INIT to prepare for preview run.
    setStatus(new PreviewStatus(PreviewStatus.Status.INIT, null, System.currentTimeMillis(), null));

    // It is recommended to initialize log appender after datasetService is started,
    // since log appender instantiates a dataset.
    logAppenderInitializer.initialize();

    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.PREVIEW_HTTP));
    Futures.allAsList(
      programRuntimeService.start(),
      metricsCollectionService.start(),
      programNotificationSubscriberService.start()
    ).get();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Stopping preview runner for {}", previewRequest.getProgram());
    try {
      // The future won't be completed with exception or cancelled, hence we only catch the timeout exception.
      completion.get(PREVIEW_TIMEOUT - (System.currentTimeMillis() - startTimeMillis), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      // Ignore
    }
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
    logAppenderInitializer.close();
    metricsCollectionService.stopAndWait();
    programNotificationSubscriberService.stopAndWait();
  }

  private ProgramController startProgram(ProgramId programId, Map<String, String> overrides, boolean debug) throws IOException, NotFoundException {
//    checkConcurrentExecution(programId); // TODO
    Map<String, String> sysArgs;
    sysArgs = propertiesResolver.getSystemProperties(Id.Program.fromEntityId(programId));
    sysArgs.put(ProgramOptionConstants.SKIP_PROVISIONING, "true");
    sysArgs.put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());
    Map<String, String> userArgs = propertiesResolver.getUserProperties(Id.Program.fromEntityId(programId));
    if (overrides != null) {
      userArgs.putAll(overrides);
    }
//    authorizePipelineRuntimeImpersonation(userArgs); // TODO
    BasicArguments systemArguments = new BasicArguments(sysArgs);
    BasicArguments userArguments = new BasicArguments(userArgs);
    ProgramOptions options = new SimpleProgramOptions(programId, systemArguments, userArguments, debug);

    ApplicationSpecification appSpec = appSpecFetcher.get(programId.getParent());
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, appSpec);
    ProgramRunId programRunId = programId.run(RunIds.generate());
    programStateWriter.start(programRunId, options, null, programDescriptor);

    RunId runId = RunIds.fromString(programRunId.getRun());

    synchronized (this) {
      ProgramRuntimeService.RuntimeInfo runtimeInfo = programRuntimeService.lookup(programRunId.getParent(), runId);
      if (runtimeInfo != null) {
        return runtimeInfo.getController();
      }
      return programRuntimeService.run(programDescriptor, options, runId).getController();
    }
  }
}
