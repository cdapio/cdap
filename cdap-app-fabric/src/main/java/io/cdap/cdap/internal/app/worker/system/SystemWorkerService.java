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

package io.cdap.cdap.internal.app.worker.system;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.security.auth.KeyManager;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Launches an HTTP server for receiving and handling {@link RunnableTask}
 */
public class SystemWorkerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(SystemWorkerService.class);

  private final DiscoveryService discoveryService;
  private final KeyManager keyManager;
  private final TwillRunnerService twillRunnerService;
  private final ProvisioningService provisioningService;
  private final NettyHttpService httpService;
  private Cancellable cancelDiscovery;
  private InetSocketAddress bindAddress;

  @Inject
  SystemWorkerService(CConfiguration cConf, SConfiguration sConf, DiscoveryService discoveryService,
      MetricsCollectionService metricsCollectionService, KeyManager keyManager,
      TwillRunnerService twillRunnerService, ProvisioningService provisioningService,
      Injector injector) {
    this.discoveryService = discoveryService;
    this.keyManager = keyManager;
    this.twillRunnerService = twillRunnerService;
    this.provisioningService = provisioningService;

    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf,
        Constants.Service.SYSTEM_WORKER)
        .setHost(cConf.get(Constants.SystemWorker.ADDRESS))
        .setPort(cConf.getInt(Constants.SystemWorker.PORT))
        .setExecThreadPoolSize(cConf.getInt(Constants.TaskWorker.EXEC_THREADS))
        .setBossThreadPoolSize(cConf.getInt(Constants.TaskWorker.BOSS_THREADS))
        .setWorkerThreadPoolSize(cConf.getInt(Constants.TaskWorker.WORKER_THREADS))
        .setChannelPipelineModifier(new ChannelPipelineModifier() {
          @Override
          public void modify(ChannelPipeline pipeline) {
            pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
          }
        })
        .setHttpHandlers(new SystemWorkerHttpHandlerInternal(cConf, metricsCollectionService,
            injector));

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }
    this.httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting SystemWorkerService");
    keyManager.startAndWait();
    provisioningService.initializeProvisionersAndExecutors();
    twillRunnerService.start();
    httpService.start();
    bindAddress = httpService.getBindAddress();
    cancelDiscovery = discoveryService.register(
        ResolvingDiscoverable.of(
            URIScheme.createDiscoverable(Constants.Service.SYSTEM_WORKER, httpService)));
    LOG.debug("Starting SystemWorkerService has completed");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down SystemWorkerService");
    httpService.stop(1, 2, TimeUnit.SECONDS);
    cancelDiscovery.cancel();
    LOG.debug("Shutting down SystemWorkerService has completed");
  }

  @VisibleForTesting
  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }
}
