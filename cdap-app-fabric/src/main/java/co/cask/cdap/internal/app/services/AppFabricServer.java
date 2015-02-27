/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.common.exception.HttpExceptionHandler;
import co.cask.cdap.common.hooks.MetricsReporterHook;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.internal.app.AppLifecycleService;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerService;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.http.HandlerHook;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.io.FileUtils;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
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
  private final AdapterService adapterService;
  private final AppLifecycleService appLifecycleService;
  private final NotificationService notificationService;
  private final Set<String> servicesNames;
  private final Set<String> handlerHookNames;

  private NettyHttpService httpService;
  private Set<HttpHandler> handlers;
  private MetricsCollectionService metricsCollectionService;
  private CConfiguration configuration;

  /**
   * Construct the AppFabricServer with service factory and configuration coming from guice injection.
   */
  @Inject
  public AppFabricServer(CConfiguration configuration, DiscoveryService discoveryService,
                         SchedulerService schedulerService, NotificationService notificationService,
                         @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname,
                         @Named("appfabric.http.handler") Set<HttpHandler> handlers,
                         @Nullable MetricsCollectionService metricsCollectionService,
                         ProgramRuntimeService programRuntimeService, AdapterService adapterService,
                         AppLifecycleService appLifecycleService,
                         @Named("appfabric.services.names") Set<String> servicesNames,
                         @Named("appfabric.handler.hooks") Set<String> handlerHookNames) {
    this.hostname = hostname;
    this.discoveryService = discoveryService;
    this.schedulerService = schedulerService;
    this.handlers = handlers;
    this.configuration = configuration;
    this.metricsCollectionService = metricsCollectionService;
    this.programRuntimeService = programRuntimeService;
    this.appLifecycleService = appLifecycleService;
    this.adapterService = adapterService;
    this.notificationService = notificationService;
    this.servicesNames = servicesNames;
    this.handlerHookNames = handlerHookNames;
  }

  /**
   * Configures the AppFabricService pre-start.
   */
  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.APP_FABRIC_HTTP));
    // Delete app fabric temp directory
    File tmpDir = new File(configuration.get(Constants.CFG_LOCAL_DATA_DIR),
                           configuration.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    FileUtils.deleteDirectory(tmpDir);

    notificationService.start();
    schedulerService.start();
    adapterService.start();
    programRuntimeService.start();

    // Create handler hooks
    ImmutableList.Builder<HandlerHook> builder = ImmutableList.builder();
    for (String hook : handlerHookNames) {
      builder.add(new MetricsReporterHook(metricsCollectionService, hook));
    }

    // Run http service on random port
    httpService = new CommonNettyHttpServiceBuilder(configuration)
      .setHost(hostname.getCanonicalHostName())
      .setHandlerHooks(builder.build())
      .addHttpHandlers(handlers)
      .setExceptionHandler(new HttpExceptionHandler())
      .setConnectionBacklog(configuration.getInt(Constants.AppFabric.BACKLOG_CONNECTIONS,
                                                 Constants.AppFabric.DEFAULT_BACKLOG))
      .setExecThreadPoolSize(configuration.getInt(Constants.AppFabric.EXEC_THREADS,
                                                  Constants.AppFabric.DEFAULT_EXEC_THREADS))
      .setBossThreadPoolSize(configuration.getInt(Constants.AppFabric.BOSS_THREADS,
                                                  Constants.AppFabric.DEFAULT_BOSS_THREADS))
      .setWorkerThreadPoolSize(configuration.getInt(Constants.AppFabric.WORKER_THREADS,
                                                    Constants.AppFabric.DEFAULT_WORKER_THREADS))
      .build();

    // Add a listener so that when the service started, register with service discovery.
    // Remove from service discovery when it is stopped.
    httpService.addListener(new ServiceListenerAdapter() {

      private List<Cancellable> cancellables = Lists.newArrayList();

      @Override
      public void running() {
        final InetSocketAddress socketAddress = httpService.getBindAddress();
        LOG.info("AppFabric HTTP Service started at {}", socketAddress);

        // TODO accept a list of services, and start them here
        // When it is running, register it with service discovery
        for (final String serviceName : servicesNames) {
          cancellables.add(discoveryService.register(new Discoverable() {
            @Override
            public String getName() {
              return serviceName;
            }

            @Override
            public InetSocketAddress getSocketAddress() {
              return socketAddress;
            }
          }));
        }
      }

      @Override
      public void terminated(State from) {
        LOG.info("AppFabric HTTP service stopped.");
        for (Cancellable cancellable : cancellables) {
          if (cancellable != null) {
            cancellable.cancel();
          }
        }
      }

      @Override
      public void failed(State from, Throwable failure) {
        LOG.info("AppFabric HTTP service stopped with failure.", failure);
        for (Cancellable cancellable : cancellables) {
          if (cancellable != null) {
            cancellable.cancel();
          }
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
    adapterService.stopAndWait();
    notificationService.stopAndWait();
  }
}
