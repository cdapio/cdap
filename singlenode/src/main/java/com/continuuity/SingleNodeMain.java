/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity;

import com.continuuity.app.guice.AppFabricServiceRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.stream.service.StreamHttpModule;
import com.continuuity.data.stream.service.StreamHttpService;
import com.continuuity.data2.datafabric.dataset.service.DatasetManagerService;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.Gateway;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.gateway.collector.NettyFlumeCollector;
import com.continuuity.gateway.router.NettyRouter;
import com.continuuity.gateway.router.RouterModules;
import com.continuuity.gateway.runtime.GatewayModule;
import com.continuuity.hive.client.guice.HiveClientModule;
import com.continuuity.hive.server.HiveServer;
import com.continuuity.hive.guice.HiveRuntimeModule;
import com.continuuity.hive.inmemory.InMemoryHiveMetastore;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.metrics.guice.MetricsHandlerModule;
import com.continuuity.metrics.query.MetricsQueryService;
import com.continuuity.passport.http.client.PassportClient;
import com.continuuity.security.guice.SecurityModules;
import com.continuuity.security.server.ExternalAuthenticationServer;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintStream;
import java.net.InetAddress;
import java.util.List;

/**
 * Singlenode Main.
 * NOTE: Use AbstractIdleService
 */
public class SingleNodeMain {
  private static final Logger LOG = LoggerFactory.getLogger(SingleNodeMain.class);

  private final WebCloudAppService webCloudAppService;
  private final CConfiguration configuration;
  private final NettyRouter router;
  private final Gateway gatewayV2;
  private final MetricsQueryService metricsQueryService;
  private final NettyFlumeCollector flumeCollector;
  private final AppFabricServer appFabricServer;
  private final StreamHttpService streamHttpService;

  private final MetricsCollectionService metricsCollectionService;

  private final LogAppenderInitializer logAppenderInitializer;
  private final InMemoryTransactionManager transactionManager;

  private final InMemoryHiveMetastore hiveMetastore;
  private final HiveServer hiveServer;

  private ExternalAuthenticationServer externalAuthenticationServer;
  private final DatasetManagerService datasetService;

  private InMemoryZKServer zookeeper;

  public SingleNodeMain(List<Module> modules, CConfiguration configuration, String webAppPath) {
    this.configuration = configuration;
    this.webCloudAppService = new WebCloudAppService(webAppPath);

    Injector injector = Guice.createInjector(modules);
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    router = injector.getInstance(NettyRouter.class);
    gatewayV2 = injector.getInstance(Gateway.class);
    metricsQueryService = injector.getInstance(MetricsQueryService.class);
    flumeCollector = injector.getInstance(NettyFlumeCollector.class);
    appFabricServer = injector.getInstance(AppFabricServer.class);
    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);

    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    datasetService = injector.getInstance(DatasetManagerService.class);

    streamHttpService = injector.getInstance(StreamHttpService.class);

    hiveMetastore = injector.getInstance(InMemoryHiveMetastore.class);
    hiveServer = injector.getInstance(HiveServer.class);

    boolean securityEnabled = configuration.getBoolean(Constants.Security.CFG_SECURITY_ENABLED);
    if (securityEnabled) {
      externalAuthenticationServer = injector.getInstance(ExternalAuthenticationServer.class);
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          shutDown();
        } catch (Throwable e) {
          LOG.error("Failed to shutdown", e);
          // because shutdown hooks execute concurrently, the logger may be closed already: thus also print it.
          System.err.println("Failed to shutdown: " + e.getMessage());
          e.printStackTrace(System.err);
        }
      }
    });
  }

  /**
   * Start the service.
   */
  protected void startUp(String[] args) throws Exception {
    logAppenderInitializer.initialize();

    File zkDir = new File(configuration.get(Constants.CFG_LOCAL_DATA_DIR) + "/zookeeper");
    //noinspection ResultOfMethodCallIgnored
    zkDir.mkdir();
    zookeeper = InMemoryZKServer.builder().setDataDir(zkDir).build();
    zookeeper.startAndWait();

    configuration.set(Constants.Zookeeper.QUORUM, zookeeper.getConnectionStr());

    // Start all the services.
    transactionManager.startAndWait();
    metricsCollectionService.startAndWait();
    datasetService.startAndWait();

    Service.State state = appFabricServer.startAndWait();
    if (state != Service.State.RUNNING) {
      throw new Exception("Failed to start Application Fabric.");
    }

    gatewayV2.startAndWait();
    metricsQueryService.startAndWait();
    router.startAndWait();
    flumeCollector.startAndWait();
    webCloudAppService.startAndWait();
    streamHttpService.startAndWait();
    hiveMetastore.startAndWait();  // in that order
    hiveServer.startAndWait();
    if (externalAuthenticationServer != null) {
      externalAuthenticationServer.startAndWait();
    }

    String hostname = InetAddress.getLocalHost().getHostName();
    System.out.println("Continuuity Reactor started successfully");
    System.out.println("Connect to dashboard at http://" + hostname + ":9999");
  }

  /**
   * Shutdown the service.
   */
  public void shutDown() {
    LOG.info("Shutting down reactor...");

    streamHttpService.stopAndWait();
    webCloudAppService.stopAndWait();
    flumeCollector.stopAndWait();
    router.stopAndWait();
    gatewayV2.stopAndWait();
    metricsQueryService.stopAndWait();
    appFabricServer.stopAndWait();
    transactionManager.stopAndWait();
    datasetService.stopAndWait();
    if (externalAuthenticationServer != null) {
      externalAuthenticationServer.stopAndWait();
    }
    zookeeper.stopAndWait();
    logAppenderInitializer.close();
    hiveServer.stopAndWait();
    hiveMetastore.stopAndWait();
  }

  /**
   * Print the usage statement and return null.
   *
   * @param error indicates whether this was invoked as the result of an error
   * @throws IllegalArgumentException in case of error
   */
  static void usage(boolean error) {

    // Which output stream should we use?
    PrintStream out = (error ? System.err : System.out);

    // And our requirements and usage
    out.println("Requirements: ");
    out.println("  Java:    JDK 1.6+ must be installed and JAVA_HOME environment variable set to the java executable");
    out.println("  Node.js: Node.js must be installed (obtain from http://nodejs.org/#download).  ");
    out.println("           The \"node\" executable must be in the system $PATH environment variable");
    out.println("");
    out.println("Usage: ");
    if (System.getProperty("os.name").startsWith("Windows")) {
      out.println("  reactor.bat [options]");
    } else {
      out.println("  ./reactor.sh [options]");
    }
    out.println("");
    out.println("Additional options:");
    out.println("  --web-app-path  Path to web-app");
    out.println("  --in-memory     To run everything in memory");
    out.println("  --help          To print this message");
    out.println("");

    if (error) {
      throw new IllegalArgumentException();
    }
  }

  /**
   * The root of all goodness!
   *
   * @param args Our cmdline arguments
   */
  public static void main(String[] args) {
    CConfiguration configuration = CConfiguration.create();

    // Single node use persistent data fabric by default
    boolean inMemory = false;
    String webAppPath = WebCloudAppService.WEB_APP;

    if (args.length > 0) {
      if ("--help".equals(args[0]) || "-h".equals(args[0])) {
        usage(false);
        return;
      } else if ("--in-memory".equals(args[0])) {
        inMemory = true;
      } else if ("--web-app-path".equals(args[0])) {
        webAppPath = args[1];
      } else {
        usage(true);
      }
    }


    // This is needed to use LocalJobRunner with fixes (we have it in app-fabric).
    // For the modified local job runner
    Configuration hConf = new Configuration();
    hConf.addResource("mapred-site-local.xml");
    hConf.reloadConfiguration();
    String localDataDir = configuration.get(Constants.CFG_LOCAL_DATA_DIR);
    hConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir);
    hConf.set(Constants.AppFabric.OUTPUT_DIR, configuration.get(Constants.AppFabric.OUTPUT_DIR));

    // Windows specific requirements
    if (System.getProperty("os.name").startsWith("Windows")) {
      String userDir = System.getProperty("user.dir");
      System.load(userDir + "/lib/native/hadoop.dll");
      hConf.set("hadoop.tmp.dir", userDir + "/" + localDataDir + "/temp");
    }

    //Run gateway on random port and forward using router.
    configuration.setInt(Constants.Gateway.PORT, 0);

    //Run dataset service on random port
    configuration.setInt(Constants.Dataset.Manager.PORT, Networks.getRandomPort());

    List<Module> modules = inMemory ? createInMemoryModules(configuration, hConf)
                                    : createPersistentModules(configuration, hConf);

    SingleNodeMain main = new SingleNodeMain(modules, configuration, webAppPath);
    try {
      main.startUp(args);
    } catch (Throwable e) {
      System.err.println("Failed to start server. " + e.getMessage());
      LOG.error("Failed to start server", e);
      main.shutDown();
      System.exit(-2);
    }
  }

  private static List<Module> createInMemoryModules(CConfiguration configuration, Configuration hConf) {

    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());

    return ImmutableList.of(
      new ConfigModule(configuration, hConf),
      new IOModule(),
      new MetricsHandlerModule(),
      new AuthModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new AppFabricServiceRuntimeModule().getInMemoryModules(),
      new ProgramRunnerRuntimeModule().getInMemoryModules(),
      new GatewayModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new LoggingModules().getInMemoryModules(),
      new RouterModules().getInMemoryModules(),
      new StreamHttpModule(),
      new HiveRuntimeModule().getInMemoryModules(),
      new HiveClientModule(),
      new SecurityModules().getSingleNodeModules(),
      new StreamHttpModule()
    );
  }

  private static List<Module> createPersistentModules(CConfiguration configuration, Configuration hConf) {
    configuration.setIfUnset(Constants.CFG_DATA_LEVELDB_DIR, Constants.DEFAULT_DATA_LEVELDB_DIR);

    String environment =
      configuration.get(Constants.CFG_APPFABRIC_ENVIRONMENT, Constants.DEFAULT_APPFABRIC_ENVIRONMENT);
    if (environment.equals("vpc")) {
      System.err.println("Reactor Environment : " + environment);
    }

    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.LEVELDB.name());

    String passportUri = configuration.get(Constants.Gateway.CFG_PASSPORT_SERVER_URI);
    final PassportClient client = passportUri == null || passportUri.isEmpty() ? new PassportClient()
                                                                               : PassportClient.create(passportUri);

    return ImmutableList.of(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(PassportClient.class).toProvider(new Provider<PassportClient>() {
            @Override
            public PassportClient get() {
              return client;
            }
          });
        }
      },
      new ConfigModule(configuration, hConf),
      new IOModule(),
      new MetricsHandlerModule(),
      new AuthModule(),
      new DiscoveryRuntimeModule().getSingleNodeModules(),
      new LocationRuntimeModule().getSingleNodeModules(),
      new AppFabricServiceRuntimeModule().getSingleNodeModules(),
      new ProgramRunnerRuntimeModule().getSingleNodeModules(),
      new GatewayModule().getSingleNodeModules(),
      new DataFabricModules(configuration).getSingleNodeModules(),
      new MetricsClientRuntimeModule().getSingleNodeModules(),
      new LoggingModules().getSingleNodeModules(),
      new RouterModules().getSingleNodeModules(),
      new SecurityModules().getSingleNodeModules(),
      new StreamHttpModule(),
      new HiveRuntimeModule(configuration).getSingleNodeModules(),
      new HiveClientModule()
    );
  }
}
