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
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.preview.PreviewDiscoveryRuntimeModule;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.MetricsReporterHook;
import co.cask.cdap.config.guice.ConfigStoreModule;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.preview.PreviewModules;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.data2.transaction.stream.inmemory.InMemoryStreamConsumerFactory;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.client.MockExploreClient;
import co.cask.cdap.gateway.handlers.meta.RemoteSystemOperationsServiceModule;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactStore;
import co.cask.cdap.internal.app.runtime.artifact.SystemArtifactLoader;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.proto.Id;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.security.guice.SecurityModules;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import co.cask.http.HandlerHook;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

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
   * Creates Preview Injector based on shared bindings passed from Standalone injector and
   * constructs the PreviewServer with configuration coming from guice injection.
   */
  @Inject
  public PreviewServer(CConfiguration cConf, Configuration hConf,
                       DiscoveryService discoveryService, TransactionManager transactionManager,
                       DatasetFramework datasetFramework, final ArtifactRepository artifactRepository,
                       final ArtifactStore artifactStore, final AuthorizerInstantiator authorizerInstantiator,
                       final StreamAdmin streamAdmin, final StreamCoordinatorClient streamCoordinatorClient,
                       final PrivilegesManager privilegesManager,
                       final AuthorizationEnforcer authorizationEnforcer) {
    Injector injector = createPreviewInjector(cConf, hConf, discoveryService, transactionManager,
                                              datasetFramework, artifactRepository,
                                              artifactStore, authorizerInstantiator, streamAdmin,
                                              streamCoordinatorClient, privilegesManager,
                                              authorizationEnforcer);
    this.cConf = injector.getInstance(CConfiguration.class);
    this.discoveryService = injector.getInstance(Key.get(DiscoveryService.class,
                                                         Names.named("shared-discovery-service")));
    this.metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    this.programRuntimeService = injector.getInstance(ProgramRuntimeService.class);
    this.applicationLifecycleService = injector.getInstance(ApplicationLifecycleService.class);
    this.programLifecycleService = injector.getInstance(ProgramLifecycleService.class);
    this.systemArtifactLoader = injector.getInstance(SystemArtifactLoader.class);
    InetAddress hostname = injector.getInstance(Key.get(InetAddress.class,
                                                        Names.named(Constants.AppFabric.SERVER_ADDRESS)));
    Set<HttpHandler> handlers =
      injector.getInstance(Key.get(new TypeLiteral<Set<HttpHandler>>() { },
                                   Names.named(Constants.Preview.HANDLERS_BINDING)));
    // Create handler hooks
    ImmutableList.Builder<HandlerHook> builder = ImmutableList.builder();
    builder.add(new MetricsReporterHook(metricsCollectionService, Constants.Service.PREVIEW_HTTP));
    this.httpService = new CommonNettyHttpServiceBuilder(this.cConf)
      .setHost(hostname.getCanonicalHostName())
      .setHandlerHooks(builder.build())
      .addHttpHandlers(handlers)
      .setConnectionBacklog(cConf.getInt(Constants.AppFabric.BACKLOG_CONNECTIONS,
                                         Constants.AppFabric.DEFAULT_BACKLOG))
      .setExecThreadPoolSize(cConf.getInt(Constants.AppFabric.EXEC_THREADS,
                                          Constants.AppFabric.DEFAULT_EXEC_THREADS))
      .setBossThreadPoolSize(cConf.getInt(Constants.AppFabric.BOSS_THREADS,
                                          Constants.AppFabric.DEFAULT_BOSS_THREADS))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.AppFabric.WORKER_THREADS,
                                            Constants.AppFabric.DEFAULT_WORKER_THREADS))
      .build();

    this.logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    this.datasetService = injector.getInstance(DatasetService.class);
  }

  private Injector createPreviewInjector(CConfiguration cConf, Configuration hConf,
                                         DiscoveryService discoveryService,
                                         TransactionManager transactionManager,
                                         DatasetFramework datasetFramework,
                                         final ArtifactRepository artifactRepository,
                                         final ArtifactStore artifactStore,
                                         final AuthorizerInstantiator authorizerInstantiator,
                                         final StreamAdmin streamAdmin,
                                         final StreamCoordinatorClient streamCoordinatorClient,
                                         final PrivilegesManager privilegesManager,
                                         final AuthorizationEnforcer authorizationEnforcer) {
    // create cConf and hConf and set appropriate settings for preview
    CConfiguration previewcConf = CConfiguration.copy(cConf);
    Path tmpDir = Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).toAbsolutePath();
    try {
      File previewDir = Files.createTempDirectory(tmpDir, "preview").toFile();
      previewcConf.set(Constants.CFG_LOCAL_DATA_DIR, previewDir.getAbsolutePath());
      previewcConf.set(Constants.Dataset.DATA_DIR, previewDir.getAbsolutePath());
      Configuration previewhConf = new Configuration(hConf);

      previewhConf.set(Constants.CFG_LOCAL_DATA_DIR, previewDir.getAbsolutePath());
      previewcConf.setIfUnset(Constants.CFG_DATA_LEVELDB_DIR, previewDir.getPath());
      return Guice.createInjector(
        new ConfigModule(previewcConf, previewhConf),
        new IOModule(),
        new AuthenticationContextModules().getMasterModule(),
        // todo : figure if we should we share secure store modules to allow reading the secure data during preview
        new SecurityModules().getStandaloneModules(),
        new SecureStoreModules().getStandaloneModules(),
        new PreviewDiscoveryRuntimeModule(discoveryService),
        new LocationRuntimeModule().getStandaloneModules(),
        new ConfigStoreModule().getStandaloneModule(),
        new PreviewServerModule(),
        new ProgramRunnerRuntimeModule().getStandaloneModules(),
        new PreviewModules().getDataFabricModule(transactionManager),
        new PreviewModules().getDataSetsModule(datasetFramework),
        new DataSetServiceModules().getStandaloneModules(),
        new MetricsClientRuntimeModule().getStandaloneModules(),
        new LoggingModules().getStandaloneModules(),
        new NamespaceStoreModule().getStandaloneModules(),
        new RemoteSystemOperationsServiceModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(ArtifactRepository.class).toInstance(artifactRepository);
            bind(ArtifactStore.class).toInstance(artifactStore);
            bind(AuthorizerInstantiator.class).toInstance(authorizerInstantiator);
            bind(AuthorizationEnforcer.class).toInstance(authorizationEnforcer);
            bind(PrivilegesManager.class).toInstance(privilegesManager);
            bind(StreamAdmin.class).toInstance(streamAdmin);
            bind(StreamConsumerFactory.class).to(InMemoryStreamConsumerFactory.class).in(Singleton.class);
            bind(StreamCoordinatorClient.class).toInstance(streamCoordinatorClient);
            // bind explore client to mock.
            bind(ExploreClient.class).to(MockExploreClient.class);
          }
        });
    } catch (IOException e) {
      LOG.error("Error while creating preview directory ", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * starts preview services
   */
  @Override
  protected void startUp() throws Exception {
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
