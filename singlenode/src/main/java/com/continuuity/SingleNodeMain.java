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
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.collector.NettyFlumeCollector;
import com.continuuity.gateway.router.NettyRouter;
import com.continuuity.gateway.router.RouterModules;
import com.continuuity.gateway.v2.Gateway;
import com.continuuity.gateway.v2.runtime.GatewayModules;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
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
  private final NettyFlumeCollector flumeCollector;
  private final AppFabricServer appFabricServer;

  private final MetricsCollectionService metricsCollectionService;

  private final LogAppenderInitializer logAppenderInitializer;
  private final InMemoryTransactionManager transactionManager;

  private InMemoryZKServer zookeeper;

  public SingleNodeMain(List<Module> modules, CConfiguration configuration, String webAppPath) {
    this.configuration = configuration;
    this.webCloudAppService = new WebCloudAppService(webAppPath);

    Injector injector = Guice.createInjector(modules);
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    router = injector.getInstance(NettyRouter.class);
    gatewayV2 = injector.getInstance(Gateway.class);
    flumeCollector = injector.getInstance(NettyFlumeCollector.class);
    appFabricServer = injector.getInstance(AppFabricServer.class);
    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);

    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);

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

    Service.State state = appFabricServer.startAndWait();
    if (state != Service.State.RUNNING) {
      throw new Exception("Failed to start Application Fabric.");
    }

    gatewayV2.startAndWait();
    router.startAndWait();
    flumeCollector.startAndWait();
    webCloudAppService.startAndWait();

    String hostname = InetAddress.getLocalHost().getHostName();
    System.out.println("Continuuity Reactor (tm) started successfully");
    System.out.println("Connect to dashboard at http://" + hostname + ":9999");
  }

  /**
   * Shutdown the service.
   */
  public void shutDown() {
    LOG.info("Shutting down reactor...");

    webCloudAppService.stopAndWait();
    flumeCollector.stopAndWait();
    router.stopAndWait();
    gatewayV2.stopAndWait();
    appFabricServer.stopAndWait();
    transactionManager.stopAndWait();
    zookeeper.stopAndWait();
    logAppenderInitializer.close();
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
    out.println("  ./continuuity-reactor [options]");
    out.println("");
    out.println("Additional options:");
    out.println("  --web-app-path  Path to web-app");
    out.println("  --help          To print this message");
    out.println("  --in-memory     To run everything in memory");
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
      } else if ("--leveldb-disable".equals(args[0])) {
        // this option overrides a setting that tells if level db can be used for persistence
        configuration.setBoolean(Constants.CFG_DATA_LEVELDB_ENABLED, false);
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
    hConf.set(Constants.CFG_LOCAL_DATA_DIR, configuration.get(Constants.CFG_LOCAL_DATA_DIR));
    hConf.set(Constants.AppFabric.OUTPUT_DIR, configuration.get(Constants.AppFabric.OUTPUT_DIR));

    //Run gateway on random port and forward using router.
    configuration.setInt(Constants.Gateway.PORT, 0);

    List<Module> modules = inMemory ? createInMemoryModules(configuration, hConf)
                                    : createPersistentModules(configuration, hConf);

    SingleNodeMain main = new SingleNodeMain(modules, configuration, webAppPath);
    try {
      main.startUp(args);
    } catch (Exception e) {
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
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new AppFabricServiceRuntimeModule().getInMemoryModules(),
      new ProgramRunnerRuntimeModule().getInMemoryModules(),
      new GatewayModules().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new LoggingModules().getInMemoryModules(),
      new RouterModules().getInMemoryModules()
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
    configuration.setBoolean(Constants.CFG_DATA_LEVELDB_ENABLED, true);

    return ImmutableList.of(
      new ConfigModule(configuration, hConf),
      new IOModule(),
      new DiscoveryRuntimeModule().getSingleNodeModules(),
      new LocationRuntimeModule().getSingleNodeModules(),
      new AppFabricServiceRuntimeModule().getSingleNodeModules(),
      new ProgramRunnerRuntimeModule().getSingleNodeModules(),
      new GatewayModules().getSingleNodeModules(),
      new DataFabricModules().getSingleNodeModules(configuration),
      new MetricsClientRuntimeModule().getSingleNodeModules(),
      new LoggingModules().getSingleNodeModules(),
      new RouterModules().getSingleNodeModules()
    );
  }
}
