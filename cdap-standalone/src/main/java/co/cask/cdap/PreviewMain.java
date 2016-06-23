/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package co.cask.cdap;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.guice.PreviewServerModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.preview.PreviewServer;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.io.URLConnections;
import co.cask.cdap.common.startup.ConfigurationLogger;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.OSDetector;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.client.MockExploreClient;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactStore;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.inmemory.InMemoryTransactionService;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

/**
 * Preview Main.
 */
public class PreviewMain {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewMain.class);

  private final Injector injector;
  private final PreviewServer previewServer;
  private final MetricsCollectionService metricsCollectionService;
  private final LogAppenderInitializer logAppenderInitializer;

  private final CConfiguration cConf;
  private final DatasetService datasetService;


  PreviewMain(List<Module> modules, CConfiguration cConf) {
    this.cConf = cConf;

    injector = Guice.createInjector(modules);

    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);

    previewServer = injector.getInstance(PreviewServer.class);
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    datasetService = injector.getInstance(DatasetService.class);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          shutDown();
        } catch (Throwable e) {
          LOG.error("Failed to shutdown", e);
          // Because shutdown hooks execute concurrently, the logger may be closed already: thus also print it.
          System.err.println("Failed to shutdown: " + e.getMessage());
          e.printStackTrace(System.err);
        }
      }
    });
  }

  /**
   * This is called by {@link StandaloneMain} with instances that are shared between standalone and preview.
   * we share DatasetFramework, DiscoveryService (to register preview service), ArtifactRepository.
   * //TODO : Figure if we have to share StreamAdmin for reading from streams.
   * @param remoteDatasetFramework
   * @return
   */
  public static PreviewMain createPreviewMain(DatasetFramework remoteDatasetFramework,
                                              InMemoryDiscoveryService discoveryService,
                                              ArtifactRepository artifactRepository,
                                              ArtifactStore artifactStore,
                                              AuthorizerInstantiator authorizerInstantiator,
                                              StreamAdmin streamAdmin, StreamCoordinatorClient streamCoordinatorClient,
                                              StreamConsumerFactory streamConsumerFactory,
                                              InMemoryTransactionService transactionService,
                                              TransactionManager transactionManager) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, cConf.get(Constants.CFG_LOCAL_PREVIEW_DIR));
    cConf.set(Constants.Dataset.DATA_DIR, cConf.get(Constants.CFG_LOCAL_PREVIEW_DIR));
    Configuration hConf = new Configuration();
    setConfigurations(cConf, hConf);
    return new PreviewMain(createPreviewModules(cConf, hConf, remoteDatasetFramework,
                                                discoveryService, artifactRepository, artifactStore,
                                                authorizerInstantiator,
                                                streamAdmin, streamCoordinatorClient, streamConsumerFactory,
                                                transactionService, transactionManager), cConf);
  }

  private static void setConfigurations(CConfiguration cConf, Configuration hConf) {
    // This is needed to use LocalJobRunner with fixes (we have it in app-fabric).
    // For the modified local job runner
    hConf.addResource("mapred-site-local.xml");
    hConf.reloadConfiguration();
    // Due to incredibly stupid design of Limits class, once it is initialized, it keeps its settings. We
    // want to make sure it uses our settings in this hConf, so we have to force it initialize here before
    // someone else initializes it.
    Limits.init(hConf);

    File localDataDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR));
    hConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    hConf.set(Constants.AppFabric.OUTPUT_DIR, cConf.get(Constants.AppFabric.OUTPUT_DIR));
    hConf.set("hadoop.tmp.dir", new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsolutePath());

    // Windows specific requirements
    if (OSDetector.isWindows()) {
      // not set anywhere by the project, expected to be set from IDEs if running from the project instead of sdk
      // hadoop.dll is at cdap-unit-test\src\main\resources\hadoop.dll for some reason
      String hadoopDLLPath = System.getProperty("hadoop.dll.path");
      if (hadoopDLLPath != null) {
        System.load(hadoopDLLPath);
      } else {
        // this is where it is when the standalone sdk is built
        String userDir = System.getProperty("user.dir");
        System.load(Joiner.on(File.separator).join(userDir, "lib", "native", "hadoop.dll"));
      }
    }
  }

  private void cleanupTempDir() {
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_PREVIEW_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();

    if (!tmpDir.isDirectory()) {
      return;
    }

    try {
      DirUtils.deleteDirectoryContents(tmpDir, true);
    } catch (IOException e) {
      // It's ok not able to cleanup temp directory.
      LOG.debug("Failed to cleanup temp directory {}", tmpDir, e);
    }
  }

  /**
   * Start the service.
   */
  public void startUp() throws Exception {
    // Workaround for release of file descriptors opened by URLClassLoader - https://issues.cask.co/browse/CDAP-2841
    URLConnections.setDefaultUseCaches(false);

    cleanupTempDir();

    ConfigurationLogger.logImportantConfig(cConf);


    metricsCollectionService.startAndWait();
    datasetService.startAndWait();


    // It is recommended to initialize log appender after datasetService is started,
    // since log appender instantiates a dataset.
    logAppenderInitializer.initialize();

    Service.State state = previewServer.startAndWait();
    if (state != Service.State.RUNNING) {
      throw new Exception("Failed to start Application Fabric");
    }


    System.out.println("CDAP Preview started successfully");
  }

  /**
   * Shutdown the service.
   */
  public void shutDown() {
    LOG.info("Shutting down Preview CDAP");
    try {
      // preview will also stop all programs
      previewServer.stopAndWait();
      // stop dataset service
      datasetService.stopAndWait();

      logAppenderInitializer.close();
    } catch (Throwable e) {

      LOG.error("Exception during shutdown of PreviewMain", e);
    } finally {
      cleanupTempDir();
    }
  }

  public static List<Module> createPreviewModules(CConfiguration cConf, Configuration hConf,
                                                  DatasetFramework remoteDsFramework,
                                                  InMemoryDiscoveryService discoveryService,
                                                  final ArtifactRepository artifactRepository,
                                                  final ArtifactStore artifactStore,
                                                  final AuthorizerInstantiator authorizerInstantiator,
                                                  final StreamAdmin streamAdmin,
                                                  final StreamCoordinatorClient streamCoordinatorClient,
                                                  final StreamConsumerFactory streamConsumerFactory,
                                                  final InMemoryTransactionService transactionService,
                                                  final TransactionManager transactionManager) {
    cConf.setIfUnset(Constants.CFG_DATA_LEVELDB_DIR, "preview");

    cConf.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.LEVELDB.name());

    // configure all services except for router to bind to 127.0.0.1
    String localhost = InetAddress.getLoopbackAddress().getHostAddress();
    cConf.set(Constants.AppFabric.SERVER_ADDRESS, localhost);
    cConf.set(Constants.Dataset.Manager.ADDRESS, localhost);
    cConf.set(Constants.Dataset.Executor.ADDRESS, localhost);
    cConf.set(Constants.Stream.ADDRESS, localhost);
    cConf.set(Constants.Metrics.ADDRESS, localhost);
    cConf.set(Constants.Metrics.SERVER_ADDRESS, localhost);
    cConf.set(Constants.MetricsProcessor.ADDRESS, localhost);
    cConf.set(Constants.LogSaver.ADDRESS, localhost);

    return ImmutableList.of(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new DiscoveryRuntimeModule().getPreviewModules(discoveryService),
      new LocationRuntimeModule().getStandaloneModules(),
      new PreviewServerModule().getPreviewModules(),
      new ProgramRunnerRuntimeModule().getStandaloneModules(),
      new DataFabricModules().getPreviewModules(transactionManager),
      new DataSetServiceModules().getStandaloneModules(),
      new DataSetsModules().getPreviewModules(remoteDsFramework),

      new MetricsClientRuntimeModule().getStandaloneModules(),
      new LoggingModules().getStandaloneModules(),
      new NamespaceStoreModule().getStandaloneModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(ArtifactRepository.class).toInstance(artifactRepository);
          bind(ArtifactStore.class).toInstance(artifactStore);
          bind(AuthorizerInstantiator.class).toInstance(authorizerInstantiator);
          bind(StreamAdmin.class).toInstance(streamAdmin);
          bind(StreamConsumerFactory.class).toInstance(streamConsumerFactory);
          bind(StreamCoordinatorClient.class).toInstance(streamCoordinatorClient);
          bind(InMemoryTransactionService.class).toInstance(transactionService);
          // bind explore client to mock.
          bind(ExploreClient.class).to(MockExploreClient.class);
        }
      }
    );
  }
}

