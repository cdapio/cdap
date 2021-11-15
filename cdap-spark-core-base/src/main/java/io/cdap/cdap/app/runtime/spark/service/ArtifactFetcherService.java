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

package io.cdap.cdap.app.runtime.spark.service;

import com.google.common.util.concurrent.AbstractIdleService;
import io.cdap.cdap.app.runtime.spark.Constant;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.http.NettyHttpService;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Launches an HTTP server for fetching artifacts.
 */
public class ArtifactFetcherService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactFetcherService.class);
  private static final String BIND_ADDRESS = "0.0.0.0";

  private final NettyHttpService httpService;

  public ArtifactFetcherService(CConfiguration cConf, Location bundleLocation) {
    httpService = new CommonNettyHttpServiceBuilder(cConf, "artifact.fetcher")
      .setHttpHandlers(
        new ArtifactFetcherHttpHandler(bundleLocation)
      )
      .setHost(BIND_ADDRESS)
      .setPort(cConf.getInt(Constant.Spark.ArtifactFetcher.PORT))
      .setExecThreadPoolSize(cConf.getInt(Constant.Spark.Driver.EXEC_THREADS))
      .setBossThreadPoolSize(cConf.getInt(Constant.Spark.Driver.BOSS_THREADS))
      .setWorkerThreadPoolSize(cConf.getInt(Constant.Spark.Driver.WORKER_THREADS))
      .build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting ArtifactFetcherService");
    httpService.start();
    LOG.info("ArtifactFetcherService started");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping ArtifactFetcherService");
    httpService.stop(1, 2, TimeUnit.SECONDS);
    LOG.info("ArtifactFetcherService stopped");
  }

}
