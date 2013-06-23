package com.continuuity.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metrics2.collector.MetricsCollectionServerInterface;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main server class for metrics collection server.
 */
final class MetricsCollectionServerMain {
  private static final Logger Log =
    LoggerFactory.getLogger(MetricsCollectionServerMain.class);

  /**
   * Metrics collection server object main.
   * @param args from command line.
   */
  public void doMain(String args[]) {
    try {
      final Injector injector
        = Guice.createInjector(new MetricsModules().getDistributedModules());
      final MetricsCollectionServerInterface serverInterface
        = injector.getInstance(MetricsCollectionServerInterface.class);
      serverInterface.start(args, CConfiguration.create());
    } catch (Exception e) {
      Log.error("Metrics collection server failed to start. Reason : {}",
                e.getMessage());
    }
  }

  /**
   * Metric collection server main.
   */
  public static void main(String[] args) {
    MetricsCollectionServerMain serviceMain = new MetricsCollectionServerMain();
    serviceMain.doMain(args);
  }
}
