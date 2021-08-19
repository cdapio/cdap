/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Launches an HTTP server for receiving and handling {@link RunnableTask}
 */
public class TaskWorkerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerService.class);
  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private final ArtifactManagerFactory artifactManagerFactory;
  private final RunnableTaskLauncher taskLauncher;
  private Cancellable cancelDiscovery;
  private InetSocketAddress bindAddress;
  private MetricsCollectionService metricsCollectionService;

  @Inject
  TaskWorkerService(CConfiguration cConf,
                    SConfiguration sConf,
                    DiscoveryService discoveryService,
                    ArtifactManagerFactory artifactManagerFactory, MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    this.discoveryService = discoveryService;
    this.artifactManagerFactory = artifactManagerFactory;
    this.taskLauncher = new RunnableTaskLauncher(cConf);
    this.metricsCollectionService = metricsCollectionService;

    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.TASK_WORKER)
      .setHost(cConf.get(Constants.TaskWorker.ADDRESS))
      .setPort(cConf.getInt(Constants.TaskWorker.PORT))
      .setExecThreadPoolSize(cConf.getInt(Constants.TaskWorker.EXEC_THREADS))
      .setBossThreadPoolSize(cConf.getInt(Constants.TaskWorker.BOSS_THREADS))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.TaskWorker.WORKER_THREADS))
      .setChannelPipelineModifier(new ChannelPipelineModifier() {
        @Override
        public void modify(ChannelPipeline pipeline) {
          pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
        }
      })
      .setHttpHandlers(new TaskWorkerHttpHandlerInternal(cConf, this::stopService, metricsCollectionService));

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }
    this.httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    preloadArtifacts();

    LOG.debug("Starting TaskWorkerService");
    httpService.start();
    bindAddress = httpService.getBindAddress();
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.TASK_WORKER, httpService)));
    LOG.debug("Starting TaskWorkerService has completed");
  }

  /**
   * Preloading artifacts by running a {@link SystemAppTask} for each of the artifact.
   */
  private void preloadArtifacts() {
    Set<String> artifacts = new HashSet<>(cConf.getTrimmedStringCollection(Constants.TaskWorker.PRELOAD_ARTIFACTS));
    if (artifacts.isEmpty()) {
      return;
    }

    try {
      ArtifactManager artifactManager = artifactManagerFactory.create(
        NamespaceId.SYSTEM, RetryStrategies.fromConfiguration(cConf, Constants.Service.TASK_WORKER + "."));

      for (ArtifactInfo info : artifactManager.listArtifacts()) {
        if (artifacts.contains(info.getName()) && info.getParents().isEmpty()) {
          String className = info.getClasses().getApps().stream()
            .findFirst()
            .map(ApplicationClass::getClassName)
            .orElse(null);

          LOG.debug("Preloading artifact {}:{}-{}", info.getScope(), info.getName(), info.getVersion());

          // Launch a dummy system app task with invalid RunnableTask (since we don't know the task class name)
          // We'll catch the Exception and ignore it.
          String systemAppClassName = SystemAppTask.class.getName();
          RunnableTaskRequest taskRequest = RunnableTaskRequest.getBuilder(systemAppClassName)
            .withParam(GSON.toJson(RunnableTaskRequest.getBuilder(className).build()))
            .withNamespace(NamespaceId.SYSTEM.getNamespace())
            .withArtifact(NamespaceId.SYSTEM.artifact(info.getName(), info.getVersion()).toApiArtifactId())
            .build();

          try {
            taskLauncher.launchRunnableTask(taskRequest);
          } catch (Exception e) {
            // Ignored
            LOG.trace("Expected exception", e);
          }
        }
      }
    } catch (Exception e) {
      // It is non-fatal for the service to start, hence we just log.
      LOG.warn("Failed to preload artifacts {}", artifacts, e);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down TaskWorkerService");
    httpService.stop(1, 2, TimeUnit.SECONDS);
    cancelDiscovery.cancel();
    LOG.debug("Shutting down TaskWorkerService has completed");
  }

  private void stopService(String className) {
    /** TODO: Expand this logic such that
     * based on number of requests per particular class,
     * the service gets stopped.
     */
    stop();
  }

  @VisibleForTesting
  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }
}
