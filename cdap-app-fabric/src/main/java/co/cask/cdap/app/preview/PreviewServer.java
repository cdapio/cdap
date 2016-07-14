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
import co.cask.cdap.common.startup.ConfigurationLogger;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.internal.app.runtime.artifact.SystemArtifactLoader;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.proto.Id;
import co.cask.http.HandlerHook;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Preview Server.
 */
public class PreviewServer extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewServer.class);

  private final CConfiguration cConf;
  private final DiscoveryService discoveryService;
  private final ProgramRuntimeService programRuntimeService;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ProgramLifecycleService programLifecycleService;
  private final SystemArtifactLoader systemArtifactLoader;

  private final MetricsCollectionService metricsCollectionService;
  private final LogAppenderInitializer logAppenderInitializer;
  private final DatasetService datasetService;

  private final NettyHttpService httpService;
  private Cancellable cancellable;

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
                       SystemArtifactLoader systemArtifactLoader,
                       LogAppenderInitializer logAppenderInitializer,
                       DatasetService datasetService) {
    this.cConf = configuration;
    this.discoveryService = discoveryService;
    this.metricsCollectionService = metricsCollectionService;
    this.programRuntimeService = programRuntimeService;
    this.applicationLifecycleService = applicationLifecycleService;
    this.programLifecycleService = programLifecycleService;
    this.systemArtifactLoader = systemArtifactLoader;
    // Create handler hooks
    ImmutableList.Builder<HandlerHook> builder = ImmutableList.builder();
    builder.add(new MetricsReporterHook(metricsCollectionService, Constants.Service.PREVIEW_HTTP));
    this.httpService = new CommonNettyHttpServiceBuilder(configuration)
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

    this.logAppenderInitializer = logAppenderInitializer;
    this.datasetService = datasetService;
  }

  /**
   * starts preview services
   */
  @Override
  protected void startUp() throws Exception {
    ConfigurationLogger.logImportantConfig(cConf);

    metricsCollectionService.startAndWait();
    datasetService.startAndWait();

    // It is recommended to initialize log appender after datasetService is started,
    // since log appender instantiates a dataset.
    logAppenderInitializer.initialize();

    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Id.Namespace.SYSTEM.getId(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.PREVIEW_HTTP));
    Futures.allAsList(
      applicationLifecycleService.start(),
      systemArtifactLoader.start(),
      programRuntimeService.start(),
      programLifecycleService.start()
    ).get();

    // Run http service on random port
    httpService.startAndWait();

    // cancel service discovery on shutdown
    cancellable = discoveryService.register(ResolvingDiscoverable.of(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.PREVIEW_HTTP;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    }));
  }

  @Override
  protected void shutDown() throws Exception {
    cancellable.cancel();
    httpService.stopAndWait();
    programRuntimeService.stopAndWait();
    applicationLifecycleService.stopAndWait();
    systemArtifactLoader.stopAndWait();
    programLifecycleService.stopAndWait();
    logAppenderInitializer.close();
    datasetService.stopAndWait();
    metricsCollectionService.stopAndWait();
  }
}
