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
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.Copyright;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.Gateway;
import com.continuuity.gateway.runtime.GatewayModules;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metadata.MetadataServerInterface;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.metrics.guice.MetricsQueryRuntimeModule;
import com.continuuity.metrics.query.MetricsQueryService;
import com.continuuity.metrics2.collector.MetricsCollectionServerInterface;
import com.continuuity.metrics2.frontend.MetricsFrontendServerInterface;
import com.continuuity.runtime.MetadataModules;
import com.continuuity.runtime.MetricsModules;
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
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Singlenode Main.
 * NOTE: Use AbstractIdleService
 */
public class SingleNodeMain {
  private static final Logger LOG = LoggerFactory.getLogger(SingleNodeMain.class);

  private final WebCloudAppService webCloudAppService;
  private final CConfiguration configuration;
  private final Gateway gateway;
  private final MetricsCollectionServerInterface overlordCollection;
  private final MetricsFrontendServerInterface overloadFrontend;
  private final MetadataServerInterface metaDataServer;
  private final AppFabricServer appFabricServer;

  private final MetricsCollectionService metricsCollectionService;
  private final MetricsQueryService metricsQueryService;

  private final LogAppenderInitializer logAppenderInitializer;
  private final InMemoryTransactionManager transactionManager;

  private InMemoryZKServer zookeeper;

  public SingleNodeMain(List<Module> modules, CConfiguration configuration) {
    this.configuration = configuration;
    this.webCloudAppService = new WebCloudAppService();

    Injector injector = Guice.createInjector(modules);
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    gateway = injector.getInstance(Gateway.class);
    overlordCollection = injector.getInstance(MetricsCollectionServerInterface.class);
    overloadFrontend = injector.getInstance(MetricsFrontendServerInterface.class);
    metaDataServer = injector.getInstance(MetadataServerInterface.class);
    appFabricServer = injector.getInstance(AppFabricServer.class);
    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);

    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsQueryService = injector.getInstance(MetricsQueryService.class);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          webCloudAppService.stop(true);
        } catch (ServerException e) {
          LOG.error(StackTraceUtil.toStringStackTrace(e));
          System.err.println("Failed to shutdown node web cloud app");
        }
        try {
          transactionManager.close();
        } catch (Throwable e) {
          LOG.error("Failed to shutdown transaction manager.", e);
          // because shutdown hooks execute concurrently, the logger may be closed already: thus also print it.
          System.err.println("Failed to shutdown transaction manager: " + e.getMessage()
                               + ". At " + StackTraceUtil.toStringStackTrace(e));
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
    zkDir.mkdir();
    zookeeper = InMemoryZKServer.builder().setDataDir(zkDir).build();
    zookeeper.startAndWait();

    configuration.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zookeeper.getConnectionStr());

    // Start all the services.
    transactionManager.init();
    metricsCollectionService.startAndWait();
    metricsQueryService.startAndWait();

    overlordCollection.start(args, configuration);

    Service.State state = appFabricServer.startAndWait();
    if (state != Service.State.RUNNING) {
      throw new Exception("Failed to start Application Fabric.");
    }

    metaDataServer.start(args, configuration);
    overloadFrontend.start(args, configuration);
    gateway.start(args, configuration);
    webCloudAppService.start(args, configuration);

    String hostname = InetAddress.getLocalHost().getHostName();
    System.out.println("Continuuity Reactor (tm) started successfully");
    System.out.println("Connect to dashboard at http://" + hostname + ":9999");
  }

  /**
   * Shutdown the service.
   */
  public void shutDown() {
    try {
      webCloudAppService.stop(true);
      gateway.stop(true);
      metaDataServer.stop(true);
      metaDataServer.stop(true);
      appFabricServer.stopAndWait();
      overloadFrontend.stop(true);
      overlordCollection.stop(true);
      transactionManager.close();
      zookeeper.stopAndWait();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
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
    out.println("  ./continuuity-reactor [options]");
    out.println("");
    out.println("Additional options:");
    out.println("  --help      To print this message");
    out.println("  --in-memory To run everything in memory");
    out.println("");

    if (error) {
      throw new IllegalArgumentException();
    }
  }

  /**
   * Checks if node is in path or no.
   */
  public static boolean nodeExists() {
    try {
      Process proc = Runtime.getRuntime().exec("node -v");
      TimeUnit.SECONDS.sleep(2);
      int exitValue = proc.exitValue();
      if (exitValue != 0) {
        return false;
      }
    } catch (IOException e) {
      LOG.error(StackTraceUtil.toStringStackTrace(e));
      throw new RuntimeException("Nodejs not in path. Please add it to PATH in the shell you are starting devsuite");
    } catch (InterruptedException e) {
      LOG.error(StackTraceUtil.toStringStackTrace(e));
      Thread.currentThread().interrupt();
    }
    return true;
  }

  /**
   * The root of all goodness!
   *
   * @param args Our cmdline arguments
   */
  public static void main(String[] args) {
    Copyright.print(System.out);

    // Checks if node exists.
    try {
      if (!nodeExists()) {
        System.err.println("Unable to find nodejs in path. Please add it to PATH.");
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      System.exit(-1);
    }

    CConfiguration configuration = CConfiguration.create();

    // Single node use persistent data fabric by default
    boolean inMemory = false;

    if (args.length > 0) {
      if ("--help".equals(args[0]) || "-h".equals(args[0])) {
        usage(false);
        return;
      } else if ("--in-memory".equals(args[0])) {
        inMemory = true;
      } else if ("--leveldb-disable".equals(args[0])) {
        // this option overrides a setting that tells if level db can be used for persistence
        configuration.setBoolean(Constants.CFG_DATA_LEVELDB_ENABLED, false);
      } else {
        usage(true);
      }
    }


    // This is needed to use LocalJobRunner with fixes (we have it in app-fabric).
    // For the modified local job runner
    Configuration hConf = new Configuration();
    hConf.addResource("mapred-site-local.xml");
    hConf.reloadConfiguration();

    List<Module> modules = inMemory ? createInMemoryModules(configuration, hConf)
                                    : createPersistentModules(configuration, hConf);

    SingleNodeMain main = new SingleNodeMain(modules, configuration);
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
      new MetricsModules().getInMemoryModules(),
      new GatewayModules(configuration).getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new MetadataModules().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new MetricsQueryRuntimeModule().getInMemoryModules(),
      new LoggingModules().getInMemoryModules()
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
      new MetricsModules().getSingleNodeModules(),
      new GatewayModules(configuration).getSingleNodeModules(),
      new DataFabricModules().getSingleNodeModules(configuration),
      new MetadataModules().getSingleNodeModules(),
      new MetricsClientRuntimeModule().getSingleNodeModules(),
      new MetricsQueryRuntimeModule().getSingleNodeModules(),
      new MetadataModules().getSingleNodeModules(),
      new LoggingModules().getSingleNodeModules()
    );
  }
}
