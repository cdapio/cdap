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
package co.cask.cdap.internal;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.AuthorizationModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.app.store.ServiceStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.io.URLConnections;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.startup.ConfigurationLogger;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.OSDetector;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.service.StreamService;
import co.cask.cdap.data.stream.service.StreamServiceRuntimeModule;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metadata.MetadataServiceModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsHandlerModule;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.proto.Id;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.guice.SecurityModules;
import co.cask.cdap.security.server.ExternalAuthenticationServer;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import co.cask.tephra.inmemory.InMemoryTransactionService;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.counters.Limits;
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
  private final AppFabricServer appFabricServer;
  private final ServiceStore serviceStore;
  private final StreamService streamService;
  private final MetricsCollectionService metricsCollectionService;
  private final LogAppenderInitializer logAppenderInitializer;
  private final InMemoryTransactionService txService;
  //  private final MetadataService metadataService;
  private final boolean securityEnabled;
  private final boolean sslEnabled;
  private final CConfiguration cConf;
  private final DatasetService datasetService;
  //  private final ExploreClient exploreClient;
  private final AuthorizerInstantiator authorizerInstantiator;

  private ExternalAuthenticationServer externalAuthenticationServer;

  PreviewMain(List<Module> modules, CConfiguration cConf) {
    this.cConf = cConf;

    injector = Guice.createInjector(modules);
    txService = injector.getInstance(InMemoryTransactionService.class);
    appFabricServer = injector.getInstance(AppFabricServer.class);
    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);

    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    datasetService = injector.getInstance(DatasetService.class);
    serviceStore = injector.getInstance(ServiceStore.class);
    streamService = injector.getInstance(StreamService.class);


    sslEnabled = cConf.getBoolean(Constants.Security.SSL_ENABLED);
    securityEnabled = cConf.getBoolean(Constants.Security.ENABLED);
    if (securityEnabled) {
      externalAuthenticationServer = injector.getInstance(ExternalAuthenticationServer.class);
    }

//    exploreClient = injector.getInstance(ExploreClient.class);
//    metadataService = injector.getInstance(MetadataService.class);
    authorizerInstantiator = injector.getInstance(AuthorizerInstantiator.class);

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

  public static PreviewMain createPreviewMain(DatasetFramework remoteDatasetFramework) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, cConf.get(Constants.CFG_LOCAL_PREVIEW_DIR));
    cConf.set(Constants.Dataset.DATA_DIR, cConf.get(Constants.CFG_LOCAL_PREVIEW_DIR));
    Configuration hConf = new Configuration();
    setConfigurations(cConf, hConf);
    return new PreviewMain(createPreviewModules(cConf, hConf, remoteDatasetFramework), cConf);
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


    txService.startAndWait();
    metricsCollectionService.startAndWait();
    datasetService.startAndWait();
    serviceStore.startAndWait();
    streamService.startAndWait();

    // It is recommended to initialize log appender after datasetService is started,
    // since log appender instantiates a dataset.
    logAppenderInitializer.initialize();

    Service.State state = appFabricServer.startAndWait();
    if (state != Service.State.RUNNING) {
      throw new Exception("Failed to start Application Fabric");
    }


    if (securityEnabled) {
      externalAuthenticationServer.startAndWait();
    }




    System.out.println("Standalone CDAP started successfully.");


//     Get purchase table and verify if its not null.

    Id.DatasetInstance purchaseInstance =
      Id.DatasetInstance.from(Id.Namespace.DEFAULT, "purchases");

    ObjectMappedTable table = DatasetsUtil.getOrCreateDataset(injector.getInstance(DatasetFramework.class),
                                                              purchaseInstance,
                                                              "table", DatasetProperties.EMPTY,
                                                              DatasetDefinition.NO_ARGUMENTS, null);

    System.out.println("Table is not null ? : " + table != null);
  }

  /**
   * Shutdown the service.
   */
  public void shutDown() {
    LOG.info("Shutting down Standalone CDAP");
    boolean halt = false;
    try {

      // now the stream writer and the explore service (they need tx)
      streamService.stopAndWait();

//      exploreClient.close();
//      metadataService.stopAndWait();
      serviceStore.stopAndWait();
      // app fabric will also stop all programs
      appFabricServer.stopAndWait();
      // all programs are stopped: dataset service, metrics, transactions can stop now
      datasetService.stopAndWait();

      txService.stopAndWait();

      if (securityEnabled) {
        // auth service is on the side anyway
        externalAuthenticationServer.stopAndWait();
      }
      logAppenderInitializer.close();


      authorizerInstantiator.close();
    } catch (Throwable e) {
      halt = true;
      LOG.error("Exception during shutdown", e);
    } finally {
      cleanupTempDir();
    }

    // We can't do much but exit. Because there was an exception, some non-daemon threads may still be running.
    // Therefore System.exit() won't do it, we need to force a halt.
    if (halt) {
      Runtime.getRuntime().halt(1);
    }
  }

  public static List<Module> createPreviewModules(CConfiguration cConf, Configuration hConf,
                                                  DatasetFramework remoteDsFramework) {
    cConf.setIfUnset(Constants.CFG_DATA_LEVELDB_DIR, "data/preview");

    cConf.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.LEVELDB.name());

    // configure all services except for router to bind to 127.0.0.1
    String localhost = InetAddress.getLoopbackAddress().getHostAddress();
    cConf.set(Constants.AppFabric.SERVER_ADDRESS, localhost);
    cConf.set(Constants.Transaction.Container.ADDRESS, localhost);
    cConf.set(Constants.Dataset.Manager.ADDRESS, localhost);
    cConf.set(Constants.Dataset.Executor.ADDRESS, localhost);
    cConf.set(Constants.Stream.ADDRESS, localhost);
    cConf.set(Constants.Metrics.ADDRESS, localhost);
    cConf.set(Constants.Metrics.SERVER_ADDRESS, localhost);
    cConf.set(Constants.MetricsProcessor.ADDRESS, localhost);
    cConf.set(Constants.LogSaver.ADDRESS, localhost);
    cConf.set(Constants.Security.AUTH_SERVER_BIND_ADDRESS, localhost);
    cConf.set(Constants.Explore.SERVER_ADDRESS, localhost);
    cConf.set(Constants.Metadata.SERVICE_BIND_ADDRESS, localhost);

    return ImmutableList.of(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new MetricsHandlerModule(),
      new DiscoveryRuntimeModule().getStandaloneModules(),
      new LocationRuntimeModule().getStandaloneModules(),
      new AppFabricServiceRuntimeModule().getStandaloneModules(),
      new ProgramRunnerRuntimeModule().getStandaloneModules(),
      new DataFabricModules().getStandaloneModules(),
      new DataSetServiceModules().getStandaloneModules(),
      new DataSetsModules().getPreviewModules(remoteDsFramework),
      new MetricsClientRuntimeModule().getStandaloneModules(),
      new LoggingModules().getStandaloneModules(),
      new SecurityModules().getStandaloneModules(),
      new StreamServiceRuntimeModule().getStandaloneModules(),
      new ServiceStoreModules().getStandaloneModules(),
      new ExploreClientModule(),
      new NotificationFeedServiceRuntimeModule().getStandaloneModules(),
      new NotificationServiceRuntimeModule().getStandaloneModules(),
      new ViewAdminModules().getStandaloneModules(),
      new StreamAdminModules().getStandaloneModules(),
      new NamespaceClientRuntimeModule().getStandaloneModules(),
      new NamespaceStoreModule().getStandaloneModules(),
      new MetadataServiceModule(),
      new AuditModule().getStandaloneModules(),
      new AuthorizationModule()
    );
  }
}

