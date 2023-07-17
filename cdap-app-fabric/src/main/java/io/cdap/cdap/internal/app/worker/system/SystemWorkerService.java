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
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.master.spi.autoscaler.MetricsEmitter;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.authorization.DefaultAccessEnforcer;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.inject.Named;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launches an HTTP server for receiving and handling {@link RunnableTask}.
 */
public class SystemWorkerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(SystemWorkerService.class);

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private final TokenManager tokenManager;
  private final TwillRunnerService twillRunnerService;
  private final TwillRunnerService remoteTwillRunnerService;
  private final ProvisioningService provisioningService;
  private Cancellable cancelDiscovery;
  private InetSocketAddress bindAddress;

  @Inject
  SystemWorkerService(CConfiguration cConf,
      SConfiguration sConf,
      DiscoveryService discoveryService,
      MetricsCollectionService metricsCollectionService,
      CommonNettyHttpServiceFactory commonNettyHttpServiceFactory,
      TokenManager tokenManager, TwillRunnerService twillRunnerService,
      @Constants.AppFabric.RemoteExecution TwillRunnerService remoteTwillRunnerService,
      ProvisioningService provisioningService,
      Injector injector, MetricsEmitter metricsEmitter,
      AuthenticationContext authenticationContext,
      @Named(DefaultAccessEnforcer.INTERNAL_ACCESS_ENFORCER) AccessEnforcer internalAccessEnforcer) {
    this.discoveryService = discoveryService;
    this.tokenManager = tokenManager;
    this.twillRunnerService = twillRunnerService;
    this.remoteTwillRunnerService = remoteTwillRunnerService;
    this.provisioningService = provisioningService;

    NettyHttpService.Builder builder = commonNettyHttpServiceFactory.builder(
            Constants.Service.SYSTEM_WORKER)
        .setHost(cConf.get(Constants.SystemWorker.ADDRESS))
        .setPort(cConf.getInt(Constants.SystemWorker.PORT))
        .setExecThreadPoolSize(cConf.getInt(Constants.SystemWorker.EXEC_THREADS))
        .setBossThreadPoolSize(cConf.getInt(Constants.SystemWorker.BOSS_THREADS))
        .setWorkerThreadPoolSize(cConf.getInt(Constants.SystemWorker.WORKER_THREADS))
        .setChannelPipelineModifier(new ChannelPipelineModifier() {
          @Override
          public void modify(ChannelPipeline pipeline) {
            pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
          }
        })
        .setHttpHandlers(
            new SystemWorkerHttpHandlerInternal(cConf, metricsCollectionService, injector,
                authenticationContext, internalAccessEnforcer, metricsEmitter));

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }
    this.httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting SystemWorkerService");
    tokenManager.startAndWait();
    provisioningService.initializeProvisionersAndExecutors();
    twillRunnerService.start();
    remoteTwillRunnerService.start();
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
    tokenManager.stop();
    twillRunnerService.stop();
    remoteTwillRunnerService.stop();
    httpService.stop(1, 2, TimeUnit.SECONDS);
    cancelDiscovery.cancel();
    LOG.debug("Shutting down SystemWorkerService has completed");
  }

  @VisibleForTesting
  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }
}
