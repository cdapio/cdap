/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity;

import com.continuuity.app.guice.AppFabricServiceRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.app.guice.ServiceStoreModules;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.utils.OSDetector;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetServiceModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data.stream.service.StreamHttpService;
import com.continuuity.data.stream.service.StreamServiceRuntimeModule;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionService;
import com.continuuity.explore.executor.ExploreExecutorService;
import com.continuuity.explore.guice.ExploreRuntimeModule;
import com.continuuity.explore.service.ExploreServiceUtils;
import com.continuuity.gateway.Gateway;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.gateway.collector.NettyFlumeCollector;
import com.continuuity.gateway.router.NettyRouter;
import com.continuuity.gateway.router.RouterModules;
import com.continuuity.gateway.runtime.GatewayModule;
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
import org.apache.hadoop.mapreduce.counters.Limits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final NettyRouter router;
  private final Gateway gatewayV2;
  private final MetricsQueryService metricsQueryService;
  private final NettyFlumeCollector flumeCollector;
  private final AppFabricServer appFabricServer;
  private final StreamHttpService streamHttpService;

  private final MetricsCollectionService metricsCollectionService;

  private final LogAppenderInitializer logAppenderInitializer;
  private final InMemoryTransactionService txService;

  private ExternalAuthenticationServer externalAuthenticationServer;
  private final DatasetService datasetService;

  private ExploreExecutorService exploreExecutorService;

  public SingleNodeMain(List<Module> modules, CConfiguration configuration, String webAppPath) {
    this.webCloudAppService = new WebCloudAppService(webAppPath);

    Injector injector = Guice.createInjector(modules);
    txService = injector.getInstance(InMemoryTransactionService.class);
    router = injector.getInstance(NettyRouter.class);
    gatewayV2 = injector.getInstance(Gateway.class);
    metricsQueryService = injector.getInstance(MetricsQueryService.class);
    flumeCollector = injector.getInstance(NettyFlumeCollector.class);
    appFabricServer = injector.getInstance(AppFabricServer.class);
    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);

    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    datasetService = injector.getInstance(DatasetService.class);

    streamHttpService = injector.getInstance(StreamHttpService.class);

    boolean securityEnabled = configuration.getBoolean(Constants.Security.CFG_SECURITY_ENABLED);
    if (securityEnabled) {
      externalAuthenticationServer = injector.getInstance(ExternalAuthenticationServer.class);
    }

    boolean exploreEnabled = configuration.getBoolean(Constants.Explore.CFG_EXPLORE_ENABLED);
    if (exploreEnabled) {
      ExploreServiceUtils.checkHiveSupportWithoutSecurity(this.getClass().getClassLoader());
      exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
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

    // Start all the services.
    txService.startAndWait();
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

    if (externalAuthenticationServer != null) {
      externalAuthenticationServer.startAndWait();
    }

    if (exploreExecutorService != null) {
      exploreExecutorService.startAndWait();
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

    try {
      // order matters: first shut down web app 'cause it will stop working after router is down
      webCloudAppService.stopAndWait();
      //  shut down router, gateway and flume, to stop all incoming traffic
      router.stopAndWait();
      gatewayV2.stopAndWait();
      flumeCollector.stopAndWait();
      // now the stream writer and the explore service (they need tx)
      streamHttpService.stopAndWait();
      if (exploreExecutorService != null) {
        exploreExecutorService.stopAndWait();
      }
      // app fabric will also stop all programs
      appFabricServer.stopAndWait();
      // all programs are stopped: dataset service, metrics, transactions can stop now
      datasetService.stopAndWait();
      metricsQueryService.stopAndWait();
      txService.stopAndWait();
      // auth service is on the side anyway
      if (externalAuthenticationServer != null) {
        externalAuthenticationServer.stopAndWait();
      }
      logAppenderInitializer.close();

    } catch (Throwable e) {
      LOG.error("Exception during shutdown", e);
      // we can't do much but exit. Because there was an exception, some non-daemon threads may still be running.
      // therefore System.exit() won't do it, we need to farce a halt.
      Runtime.getRuntime().halt(1);
    }
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
    if (OSDetector.isWindows()) {
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
    // Due to incredibly stupid design of Limits class, once it is initialized, it keeps its settings. We
    // want to make sure it uses our settings in this hConf, so we have to force it initialize here before
    // someone else initializes it.
    Limits.init(hConf);

    String localDataDir = configuration.get(Constants.CFG_LOCAL_DATA_DIR);
    hConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir);
    hConf.set(Constants.AppFabric.OUTPUT_DIR, configuration.get(Constants.AppFabric.OUTPUT_DIR));

    // Windows specific requirements
    if (OSDetector.isWindows()) {
      String userDir = System.getProperty("user.dir");
      System.load(userDir + "/lib/native/hadoop.dll");
      hConf.set("hadoop.tmp.dir", userDir + "/" + localDataDir + "/temp");
    }

    //Run gateway on random port and forward using router.
    configuration.setInt(Constants.Gateway.PORT, 0);

    //Run dataset service on random port
    List<Module> modules = inMemory ? createInMemoryModules(configuration, hConf)
                                    : createPersistentModules(configuration, hConf);

    SingleNodeMain main = null;
    try {
      main = new SingleNodeMain(modules, configuration, webAppPath);
      main.startUp(args);
    } catch (Throwable e) {
      System.err.println("Failed to start server. " + e.getMessage());
      LOG.error("Failed to start server", e);
      if (main != null) {
        main.shutDown();
      }
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
      new DataSetsModules().getInMemoryModule(),
      new DataSetServiceModules().getInMemoryModule(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new LoggingModules().getInMemoryModules(),
      new RouterModules().getInMemoryModules(),
      new SecurityModules().getInMemoryModules(),
      new StreamServiceRuntimeModule().getInMemoryModules(),
      new ExploreRuntimeModule().getInMemoryModules(),
      new ServiceStoreModules().getInMemoryModule()
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
      new DataFabricModules().getSingleNodeModules(),
      new DataSetsModules().getLocalModule(),
      new DataSetServiceModules().getLocalModule(),
      new MetricsClientRuntimeModule().getSingleNodeModules(),
      new LoggingModules().getSingleNodeModules(),
      new RouterModules().getSingleNodeModules(),
      new SecurityModules().getSingleNodeModules(),
      new StreamServiceRuntimeModule().getSingleNodeModules(),
      new ExploreRuntimeModule().getSingleNodeModules(),
      new ServiceStoreModules().getSingleNodeModule()
    );
  }
}
