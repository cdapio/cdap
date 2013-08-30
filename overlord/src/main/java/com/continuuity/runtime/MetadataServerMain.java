package com.continuuity.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.metadata.MetadataServerInterface;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main server class for metadata server.
 */
public class MetadataServerMain {
  private static final Logger Log =
    LoggerFactory.getLogger(MetricsCollectionServerMain.class);

  /**
   * Metrics collection server object main.
   * @param args from command line.
   */
  public void doMain(String args[]) {
    try {
      // Create an injector that is passing is using distributed
      // modules for metadata and data-fabric.
      final Injector injector
        = Guice.createInjector(
              new MetadataModules().getDistributedModules(),
              new DataFabricModules().getDistributedModules(),
              new ConfigModule(),
              new LocationRuntimeModule().getDistributedModules()
      );

      final MetadataServerInterface serverInterface
        = injector.getInstance(MetadataServerInterface.class);
      serverInterface.start(args, CConfiguration.create());
    } catch (Exception e) {
      Log.error("Metadata server failed to start. Reason : {}",
                e.getMessage());
    }
  }

  /**
   * Metric collection server main.
   */
  public static void main(String[] args) {
    MetadataServerMain serviceMain = new MetadataServerMain();
    serviceMain.doMain(args);
  }
}
