/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.service;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.metrics.MetricsReporterHook;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.data2.metrics.DatasetMetricsReporter;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpRequest;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * DatasetService implemented using the common http netty framework.
 */
public class DatasetService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetService.class);

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final Set<DatasetMetricsReporter> metricReporters;
  private final DatasetTypeService typeService;
  private final CConfiguration cConf;

  private Cancellable cancelDiscovery;
  private Cancellable opExecutorServiceWatch;
  private SettableFuture<ServiceDiscovered> opExecutorDiscovered;
  private volatile boolean stopping;

  @Inject
  public DatasetService(CConfiguration cConf, SConfiguration sConf,
                        DiscoveryService discoveryService,
                        DiscoveryServiceClient discoveryServiceClient,
                        MetricsCollectionService metricsCollectionService,
                        Set<DatasetMetricsReporter> metricReporters,
                        DatasetTypeService datasetTypeService,
                        DatasetInstanceService datasetInstanceService) {
    this.cConf = cConf;
    this.typeService = datasetTypeService;
    DatasetTypeHandler datasetTypeHandler = new DatasetTypeHandler(datasetTypeService);
    DatasetInstanceHandler datasetInstanceHandler = new DatasetInstanceHandler(datasetInstanceService);
    CommonNettyHttpServiceBuilder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.DATASET_MANAGER);
    if (LOG.isTraceEnabled()) {
      builder.addChannelPipelineModifier(new ChannelPipelineModifier() {
        @Override
        public void modify(ChannelPipeline channelPipeline) {
          channelPipeline.addBefore("router", "logger", new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
              if (msg instanceof HttpRequest) {
                HttpRequest req = (HttpRequest) msg;
                LOG.trace("Received {} for {} on channel {}", req.method(), req.uri(), ctx.channel());
              }
              super.channelRead(ctx, msg);
            }
          });
        }
      });
    }

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }

    this.httpService = builder
      .setHttpHandlers(datasetTypeHandler, datasetInstanceHandler)
      .setHandlerHooks(Collections.singleton(new MetricsReporterHook(metricsCollectionService,
                                                                     Constants.Service.DATASET_MANAGER)))
      .setHost(cConf.get(Constants.Service.MASTER_SERVICES_BIND_ADDRESS))
      .setPort(cConf.getInt(Constants.Dataset.Manager.PORT))
      .setConnectionBacklog(cConf.getInt(Constants.Dataset.Manager.BACKLOG_CONNECTIONS))
      .setExecThreadPoolSize(cConf.getInt(Constants.Dataset.Manager.EXEC_THREADS))
      .setBossThreadPoolSize(cConf.getInt(Constants.Dataset.Manager.BOSS_THREADS))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.Dataset.Manager.WORKER_THREADS))
      .build();
    this.discoveryService = discoveryService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.metricReporters = metricReporters;
  }

  @Override
  protected void doStart() {
    Thread thread = new Thread(this::startUp);
    thread.setName(Constants.Service.DATASET_MANAGER + "-startup");
    thread.start();
  }

  @Override
  protected void doStop() {
    stopping = true;

    Thread thread = new Thread(this::shutDown);
    thread.setName(Constants.Service.DATASET_MANAGER + "-shutdown");
    thread.start();
  }

  /**
   * Starts this service by starting all dependencies and wait for operation executor to be ready.
   */
  private void startUp() {
    try {
      LOG.info("Starting DatasetService...");
      typeService.startAndWait();
      httpService.start();

      // setting watch for ops executor service that we need to be running to operate correctly
      ServiceDiscovered discover = discoveryServiceClient.discover(Constants.Service.DATASET_EXECUTOR);
      opExecutorDiscovered = SettableFuture.create();
      opExecutorServiceWatch = discover.watchChanges(
        serviceDiscovered -> {
          if (!Iterables.isEmpty(serviceDiscovered)) {
            LOG.info("Discovered {} service", Constants.Service.DATASET_EXECUTOR);
            opExecutorDiscovered.set(serviceDiscovered);
          }
        }, MoreExecutors.sameThreadExecutor());

      for (DatasetMetricsReporter metricsReporter : metricReporters) {
        metricsReporter.start();
      }
    } catch (Throwable t) {
      notifyFailed(t);
      return;
    }

    // Notify that the Service has been started to unblock all startAndWait call on this service
    notifyStarted();

    try {
      waitForOpExecutorToStart();
      String announceAddress = cConf.get(Constants.Service.MASTER_SERVICES_ANNOUNCE_ADDRESS,
                                         httpService.getBindAddress().getHostName());
      int announcePort = cConf.getInt(Constants.Dataset.Manager.ANNOUNCE_PORT,
                                      httpService.getBindAddress().getPort());

      InetSocketAddress socketAddress = new InetSocketAddress(announceAddress, announcePort);
      URIScheme scheme = httpService.isSSLEnabled() ? URIScheme.HTTPS : URIScheme.HTTP;
      LOG.info("Announcing DatasetService for discovery...");
      // Register the service
      cancelDiscovery = discoveryService.register(
        ResolvingDiscoverable.of(scheme.createDiscoverable(Constants.Service.DATASET_MANAGER, socketAddress)));

      LOG.info("DatasetService started successfully on {}", socketAddress);
    } catch (Throwable t) {
      try {
        doShutdown();
      } catch (Throwable shutdownError) {
        t.addSuppressed(shutdownError);
      }
      notifyFailed(t);
    }
  }

  private void waitForOpExecutorToStart() throws Exception {
    LOG.info("Waiting for {} service to be discoverable", Constants.Service.DATASET_EXECUTOR);
    while (!stopping) {
      try {
        opExecutorDiscovered.get(1, TimeUnit.SECONDS);
        opExecutorServiceWatch.cancel();
        break;
      } catch (TimeoutException e) {
        // re-try
      } catch (InterruptedException e) {
        LOG.warn("Got interrupted while waiting for service {}", Constants.Service.DATASET_EXECUTOR);
        Thread.currentThread().interrupt();
        opExecutorServiceWatch.cancel();
        break;
      } catch (ExecutionException e) {
        LOG.error("Error during discovering service {}, DatasetService start failed",
                  Constants.Service.DATASET_EXECUTOR);
        opExecutorServiceWatch.cancel();
        throw e;
      }
    }
  }

  protected void shutDown() {
    try {
      doShutdown();
      notifyStopped();
    } catch (Throwable t) {
      notifyFailed(t);
    }
  }

  private void doShutdown() throws Exception {
    LOG.info("Stopping DatasetService...");
    if (cancelDiscovery != null) {
      cancelDiscovery.cancel();
    }

    for (DatasetMetricsReporter metricsReporter : metricReporters) {
      metricsReporter.stop();
    }

    if (opExecutorServiceWatch != null) {
      opExecutorServiceWatch.cancel();
    }

    typeService.stopAndWait();

    // Wait for a few seconds for requests to stop
    httpService.stop();
    LOG.info("DatasetService stopped");
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("bindAddress", httpService.getBindAddress())
      .toString();
  }
}
