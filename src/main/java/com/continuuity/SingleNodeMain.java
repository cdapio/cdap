/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity;

import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.Copyright;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.discovery.DiscoveryService;
import com.continuuity.gateway.Gateway;
import com.continuuity.gateway.runtime.GatewayModules;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.metadata.MetadataServerInterface;
import com.continuuity.metrics2.collector.MetricsCollectionServerInterface;
import com.continuuity.metrics2.frontend.MetricsFrontendServerInterface;
import com.continuuity.runtime.MetadataModules;
import com.continuuity.runtime.MetricsModules;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Binder;
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
import java.util.concurrent.TimeUnit;

/**
 * Singlenode Main
 * NOTE: Use AbstractIdleService
 */
public class SingleNodeMain {
  private static final Logger LOG = LoggerFactory.getLogger(SingleNodeMain.class);

  private InMemoryZookeeper zookeeper;
  private final WebCloudAppService webCloudAppService;
  private Gateway gateway;
  private DiscoveryService discoveryService;
  private MetricsCollectionServerInterface overlordCollection;
  private MetricsFrontendServerInterface overloadFrontend;
  private MetadataServerInterface metaDataServer;
  private AppFabricServer appFabricServer;
  private static final String ZOOKEEPER_DATA_DIR = "data/zookeeper";
  private final CConfiguration configuration;
  private final ImmutableList<Module> modules;

  public SingleNodeMain(ImmutableList<Module> modules, CConfiguration configuration) {
    this.modules = modules;
    this.configuration = configuration;
    this.webCloudAppService = new WebCloudAppService();

    Injector injector = Guice.createInjector(modules);
    gateway = injector.getInstance(Gateway.class);
    discoveryService = injector.getInstance(DiscoveryService.class);
    overlordCollection = injector.getInstance(MetricsCollectionServerInterface.class);
    overloadFrontend = injector.getInstance(MetricsFrontendServerInterface.class);
    metaDataServer = injector.getInstance(MetadataServerInterface.class);
    appFabricServer = injector.getInstance(AppFabricServer.class);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          webCloudAppService.stop(true);
        } catch (ServerException e) {
          LOG.error(StackTraceUtil.toStringStackTrace(e));
          System.err.println("Failed to shutdown node web cloud app");
        }
      }
    });
  }

  /**
   * Start the service.
   */
  protected void startUp(String[] args) throws Exception {
    File zkDir = new File(ZOOKEEPER_DATA_DIR);
    zkDir.mkdir();
    int port = PortDetector.findFreePort();
    discoveryService.startAndWait();
    zookeeper = new InMemoryZookeeper(port, zkDir);
    configuration.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zookeeper.getConnectionString());

    // Start all the services.
    overlordCollection.start(args, configuration);

    Service.State state = appFabricServer.startAndWait();
    if(state != Service.State.RUNNING) {
      throw new Exception("Failed to start Application Fabric.");
    }

    metaDataServer.start(args, configuration);
    overloadFrontend.start(args, configuration);
    gateway.start(args, configuration);
    webCloudAppService.start(args, configuration);

    String hostname = InetAddress.getLocalHost().getHostName();
    System.out.println("Continuuity AppFabric started successfully");
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
    } catch (Exception e) {
      LOG.error(StackTraceUtil.toStringStackTrace(e));
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
    out.println("  ./continuuity-app-fabric [options]");
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
   * @return
   */
  public static boolean nodeExists() {
    try {
      Process proc = Runtime.getRuntime().exec("node -v");
      TimeUnit.SECONDS.sleep(2);
      int exitValue = proc.exitValue();
      if(exitValue != 0) {
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
      if(! nodeExists()) {
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
    // When BigMamaModule is refactored we may move it into one of the singlenode modules.
    Module hadoopConfModule = getHadoopConfModule();

    ImmutableList<Module> modules;
    if (inMemory) {
      modules = createInMemoryModules(configuration, hadoopConfModule);
    } else {
      modules = createPersistentModules(configuration, hadoopConfModule);
    }

    SingleNodeMain main = new SingleNodeMain(modules, configuration);
    try {
      main.startUp(args);
    } catch (Exception e) {
      main.shutDown();
      System.err.println("Failed to start server. " + e.getMessage());
      System.exit(-2);
    }
  }

  private static Module getHadoopConfModule() {
    return new Module() {
        @Override
        public void configure(Binder binder) {
          Configuration hConf = new Configuration();
          hConf.addResource("mapred-site-local.xml");
          hConf.reloadConfiguration();
          binder.bind(Configuration.class).toInstance(hConf);
        }
      };
  }

  private static ImmutableList<Module> createInMemoryModules(CConfiguration configuration,
                                                             Module hadoopConfModule) {

    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());

    return ImmutableList.of(
      new BigMamaModule(configuration),
      new MetricsModules().getInMemoryModules(),
      new GatewayModules().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new MetadataModules().getInMemoryModules(),
      hadoopConfModule
    );
  }

  private static ImmutableList<Module> createPersistentModules(CConfiguration configuration, Module hadoopConfModule) {
    ImmutableList<Module> modules;
    configuration.setIfUnset(Constants.CFG_DATA_LEVELDB_DIR, Constants.DEFAULT_DATA_LEVELDB_DIR);

    boolean inVPC = false;
    String environment =
      configuration.get(Constants.CFG_APPFABRIC_ENVIRONMENT, Constants.DEFAULT_APPFABRIC_ENVIRONMENT);
    if(environment.equals("vpc")) {
      System.err.println("AppFabric Environment : " + environment);
      inVPC = true;
    }

    boolean levelDBCompatibleOS = DataFabricLevelDBModule.isOsLevelDBCompatible();
    boolean levelDBEnabled =
      configuration.getBoolean(Constants.CFG_DATA_LEVELDB_ENABLED, Constants.DEFAULT_DATA_LEVELDB_ENABLED);

    boolean useLevelDB = (inVPC || levelDBCompatibleOS) && levelDBEnabled;
    if (useLevelDB) {
      configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.LEVELDB.name());
    } else {
      configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.HSQLDB.name());
    }

    configuration.setBoolean(Constants.CFG_DATA_LEVELDB_ENABLED, levelDBEnabled);

    modules = ImmutableList.of(
      new BigMamaModule(configuration),
      new MetricsModules().getSingleNodeModules(),
      new GatewayModules().getSingleNodeModules(),
      useLevelDB ? new DataFabricLevelDBModule(configuration) : new DataFabricModules().getSingleNodeModules(),
      new MetadataModules().getSingleNodeModules(),
      hadoopConfModule
    );
    return modules;
  }
}
