/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerCleaner;
import io.cdap.http.NettyHttpService;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Launches an HTTP server for fetching and cache artifacts from a remote CDAP instance.
 */
public class ArtifactCacheService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactCacheService.class);

  private final NettyHttpService httpService;
  private final ArtifactLocalizerCleaner cleaner;
  private final int cacheCleanupInterval;
  private ScheduledExecutorService scheduledExecutorService;

  @Inject
  public ArtifactCacheService(CConfiguration cConf, ArtifactCache cache) {
    httpService = new CommonNettyHttpServiceBuilder(cConf, "artifact.cache")
      .setHttpHandlers(new ArtifactCacheHttpHandlerInternal(cache))
      .setHost(cConf.get(Constants.ArtifactCache.ADDRESS))
      .setPort(cConf.getInt(Constants.ArtifactCache.PORT))
      .setBossThreadPoolSize(cConf.getInt(Constants.ArtifactCache.BOSS_THREADS))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.ArtifactCache.WORKER_THREADS))
      .build();
    cacheCleanupInterval = cConf.getInt(Constants.ArtifactCache.CACHE_CLEANUP_INTERVAL_MIN);
    String cacheDir = cConf.get(Constants.ArtifactCache.LOCAL_DATA_DIR);
    cleaner = new ArtifactLocalizerCleaner(Paths.get(cacheDir).resolve("peers"), cacheCleanupInterval);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting ArtifactCacheService");
    httpService.start();
    scheduledExecutorService = Executors
      .newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("remote-artifact-cache-cleaner"));
    scheduledExecutorService.scheduleAtFixedRate(cleaner, cacheCleanupInterval, cacheCleanupInterval, TimeUnit.MINUTES);
    LOG.info("ArtifactCacheService started");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping ArtifactCacheService");
    httpService.stop(1, 2, TimeUnit.SECONDS);
    scheduledExecutorService.shutdownNow();
    LOG.info("ArtifactCacheService stopped");
  }
}
