/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.hooks.MetricsReporterHook;
import co.cask.cdap.common.http.BaseNettyHttpServiceTemplate;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerService;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * AppFabric Server.
 */
public final class AppFabricServer extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricServer.class);

  private final DiscoveryService discoveryService;
  private final InetAddress hostname;
  private final SchedulerService schedulerService;
  private final ProgramRuntimeService programRuntimeService;
  private final BaseNettyHttpServiceTemplate baseNettyHttpServiceTemplate;

  private NettyHttpService httpService;
  private Set<HttpHandler> handlers;
  private MetricsCollectionService metricsCollectionService;
  private CConfiguration configuration;

  /**
   * Construct the AppFabricServer with service factory and configuration coming from guice injection.
   */
  @Inject
  public AppFabricServer(CConfiguration configuration, DiscoveryService discoveryService,
                         SchedulerService schedulerService,
                         @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname,
                         @Named("appfabric.http.handler") Set<HttpHandler> handlers,
                         @Nullable MetricsCollectionService metricsCollectionService,
                         BaseNettyHttpServiceTemplate baseNettyHttpServiceTemplate,
                         ProgramRuntimeService programRuntimeService) {
    this.hostname = hostname;
    this.discoveryService = discoveryService;
    this.schedulerService = schedulerService;
    this.handlers = handlers;
    this.configuration = configuration;
    this.metricsCollectionService = metricsCollectionService;
    this.programRuntimeService = programRuntimeService;
    this.baseNettyHttpServiceTemplate = baseNettyHttpServiceTemplate;
  }

  /**
   * Configures the AppFabricService pre-start.
   */
  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.APP_FABRIC_HTTP));
    schedulerService.start();
    programRuntimeService.start();

    // Run http service on random port
    httpService = baseNettyHttpServiceTemplate.get()
      .setHost(hostname.getCanonicalHostName())
      .setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                Constants.Service.APP_FABRIC_HTTP)))
      .addHttpHandlers(handlers)
      .setConnectionBacklog(configuration.getInt(Constants.Gateway.BACKLOG_CONNECTIONS,
                                                 Constants.Gateway.DEFAULT_BACKLOG))
      .setExecThreadPoolSize(configuration.getInt(Constants.Gateway.EXEC_THREADS,
                                                  Constants.Gateway.DEFAULT_EXEC_THREADS))
      .setBossThreadPoolSize(configuration.getInt(Constants.Gateway.BOSS_THREADS,
                                                  Constants.Gateway.DEFAULT_BOSS_THREADS))
      .setWorkerThreadPoolSize(configuration.getInt(Constants.Gateway.WORKER_THREADS,
                                                    Constants.Gateway.DEFAULT_WORKER_THREADS))
      .build();

    // Add a listener so that when the service started, register with service discovery.
    // Remove from service discovery when it is stopped.
    httpService.addListener(new ServiceListenerAdapter() {

      private Cancellable cancellable;

      @Override
      public void running() {
        final InetSocketAddress socketAddress = httpService.getBindAddress();
        LOG.info("AppFabric HTTP Service started at {}", socketAddress);

        // When it is running, register it with service discovery
        cancellable = discoveryService.register(new Discoverable() {

          @Override
          public String getName() {
            return Constants.Service.APP_FABRIC_HTTP;
          }

          @Override
          public InetSocketAddress getSocketAddress() {
            return socketAddress;
          }
        });
      }

      @Override
      public void terminated(State from) {
        LOG.info("AppFabric HTTP service stopped.");
        if (cancellable != null) {
          cancellable.cancel();
        }
      }

      @Override
      public void failed(State from, Throwable failure) {
        LOG.info("AppFabric HTTP service stopped with failure.", failure);
        if (cancellable != null) {
          cancellable.cancel();
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    httpService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    httpService.stopAndWait();
    programRuntimeService.stopAndWait();
    schedulerService.stopAndWait();
  }
}
