package com.continuuity.metrics2.frontend;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.runtime.MetricsModules;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests metrics frontend server.
 */
public class MetricsFrontendServerTest {
  private static MetricsFrontendServerInterface server;
  private static CConfiguration configuration;

  @BeforeClass
  public static void beforeClass() throws Exception {
    configuration = CConfiguration.create();

    // Find the ports that's available.
    int port = PortDetector.findFreePort();

    // Configure the server to start on that port.
    configuration.setInt(Constants.CFG_METRICS_FRONTEND_SERVER_PORT, port);

    // Add the SQL connection url.
    configuration.set(Constants.CFG_METRICS_CONNECTION_URL,
                      "jdbc:hsqldb:mem:testmetricsdb?user=sa");

    // Create an injector which will inject the metric frontend server.
    Injector injector = Guice.createInjector(
      new MetricsModules().getDistributedModules()
    );

    // Create an instance of metric frontend server.
    server = injector.getInstance(MetricsFrontendServerInterface.class);

    // Starts the server.
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          server.start(new String[]{}, configuration);
        } catch (ServerException e) {
          server = null;
        }
      }
    });
  }

  @AfterClass
  public static void afterClass() throws Exception {
    // Stop the server.
    if(server != null) {
      server.stop(true);
    }
  }

  @Test
  public void foo() throws Exception {

  }

}
