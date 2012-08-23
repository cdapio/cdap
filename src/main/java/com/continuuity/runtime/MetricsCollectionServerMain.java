package com.continuuity.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metrics2.collector.server.MetricsCollectionServer;
import com.continuuity.metrics2.collector.server
  .MetricsCollectionServerIoHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main server class for metrics collection server.
 */
public class MetricsCollectionServerMain {
  private static final Logger Log =
    LoggerFactory.getLogger(MetricsCollectionServerMain.class);

  public void doMain(String args[]) {

    try {
      // Create a configuration object and read in the
      // continuuity-default.xml first before continuuity-site.xml
      CConfiguration conf = CConfiguration.create();

      // Create an instance of metrics i/o handler.
      MetricsCollectionServerIoHandler handler =
        new MetricsCollectionServerIoHandler(conf);

      // Create Metrics collection server.
      MetricsCollectionServer server = new MetricsCollectionServer();

      // Set the IO handler to the server.
      server.setIoHandler(handler);

      // Start FAR Server.
      server.start(args, conf);
    } catch (Exception e) {
      Log.error("Server failed to start. Reason : {}", e.getMessage());
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
