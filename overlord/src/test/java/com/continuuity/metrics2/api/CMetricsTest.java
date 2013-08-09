package com.continuuity.metrics2.api;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.common.metrics.OverlordMetricsReporter;
import com.continuuity.metrics2.collector.MetricsCollectionServerInterface;
import com.continuuity.runtime.MetricsModules;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Testing of flow metric API.
 */
public class CMetricsTest {
  private static final Logger LOG = LoggerFactory.getLogger(CMetricsTest.class);
  private static CConfiguration configuration;
  private static MetricsCollectionServerInterface serverInterface;

  @BeforeClass
  public static void beforeClass() throws Exception {
    configuration = CConfiguration.create();

    // Start Zookeeper in memory.
    InMemoryZookeeper zookeeper = new InMemoryZookeeper();

    // Set the port the zookeeper was started on.
    configuration.set(Constants.CFG_ZOOKEEPER_ENSEMBLE,
      zookeeper.getConnectionString());

    // Get the port to run metrics collection server on.
    int metricCollectionServePort = PortDetector.findFreePort();

    // Set the port in the configuration.
    configuration.setInt(Constants.CFG_METRICS_COLLECTOR_SERVER_PORT,
      metricCollectionServePort);

    LOG.info("Zookeeper running on port {}, Metrics collection server on " +
      "port {}.", zookeeper.getConnectionString(), metricCollectionServePort);

    // Create a Guice injector to inject the server.
    Injector injector
      = Guice.createInjector(new MetricsModules().getDistributedModules());

    // Create a instance of service object.
    serverInterface
      = injector.getInstance(MetricsCollectionServerInterface.class);

    // Start the metrics collection service.
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          serverInterface.start(new String[]{}, configuration);
        } catch (ServerException e) {
          e.printStackTrace();
        }
      }
    }).start();
    OverlordMetricsReporter.enable(1, TimeUnit.SECONDS, configuration);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (serverInterface != null) {
      serverInterface.stop(true);
    }
  }

  @Test
  public void simpleFlowMetricTest() throws Exception {
    CMetrics systemMetrics
      = new CMetrics(MetricType.FlowSystem,
            "act.app.flow.run.let.1");

    CMetrics userMetrics
      = new CMetrics(MetricType.FlowUser,
                     "act.app.flow.run.let.2");

    for (int i = 0; i < 10; i++) {
      systemMetrics.meter(CMetricsTest.class, "stream.in", 1);
      systemMetrics.meter("stream.out", 1);
      systemMetrics.histogram("window", 1);
      userMetrics.histogram("mytest", 1);
      Thread.sleep(1000);
    }

  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyMetricGroupForFlowSystem() throws Exception {
    CMetrics metrics = new CMetrics(MetricType.FlowSystem);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyMetricGroupForFlowUser() throws Exception {
    CMetrics metrics = new CMetrics(MetricType.FlowUser);
  }
}
