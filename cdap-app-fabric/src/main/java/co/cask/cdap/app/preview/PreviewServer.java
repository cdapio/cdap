/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.preview;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.MetricsReporterHook;
import co.cask.cdap.internal.app.runtime.artifact.SystemArtifactLoader;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.proto.Id;
import co.cask.http.HandlerHook;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Preview Server.
 */
public class PreviewServer extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewServer.class);

  private final DiscoveryService discoveryService;
  private final InetAddress hostname;
  private final ProgramRuntimeService programRuntimeService;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ProgramLifecycleService programLifecycleService;
  private final SystemArtifactLoader systemArtifactLoader;

  private NettyHttpService httpService;
  private Set<HttpHandler> handlers;
  private MetricsCollectionService metricsCollectionService;
  private CConfiguration configuration;

  /**
   * Construct the PreviewServer with configuration coming from guice injection.
   */
  @Inject
  public PreviewServer(CConfiguration configuration,
                       @Named("shared-discovery-service") DiscoveryService discoveryService,
                       @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname,
                       @Named(Constants.Preview.HANDLERS_BINDING) Set<HttpHandler> handlers,
                       @Nullable MetricsCollectionService metricsCollectionService,
                       ProgramRuntimeService programRuntimeService,
                       ApplicationLifecycleService applicationLifecycleService,
                       ProgramLifecycleService programLifecycleService,
                       SystemArtifactLoader systemArtifactLoader) {
    this.hostname = hostname;
    this.discoveryService = discoveryService;
    this.configuration = configuration;
    this.handlers = handlers;
    this.metricsCollectionService = metricsCollectionService;
    this.programRuntimeService = programRuntimeService;
    this.applicationLifecycleService = applicationLifecycleService;
    this.programLifecycleService = programLifecycleService;
    this.systemArtifactLoader = systemArtifactLoader;
  }

  /**
   * Configures the Preview pre-start.
   */
  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Id.Namespace.SYSTEM.getId(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.PREVIEW_HTTP));
    Futures.allAsList(
      ImmutableList.of(
        applicationLifecycleService.start(),
        systemArtifactLoader.start(),
        programRuntimeService.start(),
        programLifecycleService.start()
      )
    ).get();

    // Create handler hooks
    ImmutableList.Builder<HandlerHook> builder = ImmutableList.builder();
    builder.add(new MetricsReporterHook(metricsCollectionService, Constants.Service.PREVIEW_HTTP));


    // Run http service on random port
    httpService = new CommonNettyHttpServiceBuilder(configuration)
      .setHost(hostname.getCanonicalHostName())
      .setHandlerHooks(builder.build())
      .addHttpHandlers(handlers)
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
        LOG.info("Preview HTTP Service started at {}", socketAddress);
        // When it is running, register it with service discovery

        cancellables.add(discoveryService.register(ResolvingDiscoverable.of(new Discoverable() {
          @Override
          public String getName() {
            return Constants.Service.PREVIEW_HTTP;
          }

          @Override
          public InetSocketAddress getSocketAddress() {
            return socketAddress;
          }
        })));

      }

      @Override
      public void terminated(State from) {
        LOG.info("Preview HTTP service stopped.");
        for (Cancellable cancellable : cancellables) {
          if (cancellable != null) {
            cancellable.cancel();
          }
        }
      }

      @Override
      public void failed(State from, Throwable failure) {
        LOG.info("Preview HTTP service stopped with failure.", failure);
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
    applicationLifecycleService.stopAndWait();
    systemArtifactLoader.stopAndWait();
    programLifecycleService.stopAndWait();
  }
}
