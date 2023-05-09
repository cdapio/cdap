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

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.http.NettyHttpService;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launches an HTTP server for receiving and unpacking and caching artifacts.
 */
public class ArtifactLocalizerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizerService.class);

  private final CConfiguration cConf;
  private final ArtifactLocalizer artifactLocalizer;
  private final NettyHttpService httpService;
  private final ArtifactLocalizerCleaner cleaner;
  private final int cacheCleanupInterval;
  private ScheduledExecutorService scheduledExecutorService;

  @Inject
  ArtifactLocalizerService(CConfiguration cConf,
      ArtifactLocalizer artifactLocalizer,
      CommonNettyHttpServiceFactory commonNettyHttpServiceFactory) {
    this.cConf = cConf;
    this.artifactLocalizer = artifactLocalizer;
    this.httpService = commonNettyHttpServiceFactory.builder(Constants.Service.TASK_WORKER)
        .setHost(InetAddress.getLoopbackAddress().getHostName())
        .setPort(cConf.getInt(Constants.ArtifactLocalizer.PORT))
        .setBossThreadPoolSize(cConf.getInt(Constants.ArtifactLocalizer.BOSS_THREADS))
        .setWorkerThreadPoolSize(cConf.getInt(Constants.ArtifactLocalizer.WORKER_THREADS))
        .setHttpHandlers(new ArtifactLocalizerHttpHandlerInternal(artifactLocalizer),
            new GCPMetadataHttpHandlerInternal(cConf))
        .build();

    this.cacheCleanupInterval = cConf.getInt(
        Constants.ArtifactLocalizer.CACHE_CLEANUP_INTERVAL_MIN);
    String cacheDir = cConf.get(Constants.CFG_LOCAL_DATA_DIR);
    this.cleaner = new ArtifactLocalizerCleaner(Paths.get(cacheDir).resolve("artifacts"),
        cacheCleanupInterval);
  }

  @VisibleForTesting
  public int getPort() {
    return httpService.getBindAddress().getPort();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting ArtifactLocalizerService");
    httpService.start();
    scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(
            Threads.createDaemonThreadFactory("artifact-localizer-cleaner"));
    scheduledExecutorService.scheduleAtFixedRate(cleaner, cacheCleanupInterval,
        cacheCleanupInterval, TimeUnit.MINUTES);

    artifactLocalizer.preloadArtifacts(
        new HashSet<>(cConf.getTrimmedStringCollection(Constants.TaskWorker.PRELOAD_ARTIFACTS)));

    LOG.debug("Starting ArtifactLocalizerService has completed");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down ArtifactLocalizerService");
    httpService.stop(5, 5, TimeUnit.SECONDS);
    scheduledExecutorService.shutdownNow();
    LOG.debug("Shutting down ArtifactLocalizerService has completed");
  }

  @VisibleForTesting
  void forceCleanup() {
    try {
      scheduledExecutorService.submit(cleaner).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
