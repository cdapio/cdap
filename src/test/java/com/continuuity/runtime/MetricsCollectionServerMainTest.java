package com.continuuity.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.common.metrics.OverlordMetricsReporter;
import com.continuuity.metrics2.collector.MetricsCollectionServerInterface;
import com.continuuity.metrics2.frontend.MetricsFrontendServiceImpl;
import com.continuuity.metrics2.thrift.*;
import com.google.common.io.Closeables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests metric collection server in it's entirety.
 * In here we test the metrics collection server being started,
 * it's being registered with zookeeper. The overlord reported
 * is started to send the metrics created periodically to the
 * metrics collection server. We make sure that we do not
 */
public class MetricsCollectionServerMainTest {
  private static String connectionUrl;
  private static MetricsFrontendService.Iface client;
  private static InMemoryZookeeper zookeeper;
  private static CConfiguration configuration;

  @BeforeClass
  public static void beforeClass() throws Exception {
    connectionUrl = "jdbc:hsqldb:mem:end2end?user=sa";
    configuration = CConfiguration.create();
    zookeeper = new InMemoryZookeeper();
    if(zookeeper.getConnectionString() == null) {
      throw new Exception("No ZK Connection string");
    }
    configuration.set(
      Constants.CFG_ZOOKEEPER_ENSEMBLE,
      zookeeper.getConnectionString()
    );
    configuration.set(
      Constants.CFG_METRICS_CONNECTION_URL,
      connectionUrl
    );
    // Create a client.
    client = new MetricsFrontendServiceImpl(configuration);
    Assert.assertNotNull(client);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    Closeables.closeQuietly(zookeeper);
    OverlordMetricsReporter.disable();
  }


  /**
   * FIXME: Not sure why this test is failing.
   * @throws Exception
   */
  public void end2endTest() throws Exception {
    final Injector injector
        = Guice.createInjector(new MetricsModules().getDistributedModules());

    final MetricsCollectionServerInterface serverInterface
       = injector.getInstance(MetricsCollectionServerInterface.class);

    Assert.assertNotNull(serverInterface);

    // Find the port that's available and set it in configuration
    // to be used by the server.
    final int port = PortDetector.findFreePort();
    configuration.setInt(
      Constants.CFG_METRICS_COLLECTOR_SERVER_PORT,
      port
    );

    try {
      // Start the service in a thread.
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            serverInterface.start(new String[]{}, configuration);
          } catch (ServerException e) {
            Assert.assertTrue(false);
          }
        }
      }).start();

      // This sleep is necessary. ZK takes a lot of time to register
      // the service.
      Thread.sleep(10000);

      // Start the metric reporter, configure it to send metrics every
      // few seconds. Anything less than a second during test is a problem
      // as it takes time to discover metric collection server running
      // through zk.
      OverlordMetricsReporter.enable(1L, TimeUnit.SECONDS, configuration);

      // Now create some Flow metrics.
      CMetrics cmetrics = new CMetrics(MetricType.FlowUser,
                                       "demo.myapp.myflow.myrunid");

      // create some metrics.
      for(int i = 0; i < 10; ++i) {
        cmetrics.counter("source.1.processed", 1);
        cmetrics.counter("compute.1.processed", 2);
        cmetrics.counter("sink.1.processed", 3);
        cmetrics.meter("compute.1.request", i);
        Thread.sleep(1000);
      }

      Thread.sleep(2000);

      // Now we make a call to retrieve the counters that have been
      // updated.
      List<Counter> counters = getMetric(
        new FlowArgument("demo", "myapp", "myflow"),
        null // All metrics related to flow.
      );

      // Validate that we have recorded all the metrics generated.
      Assert.assertTrue(counters.size() > 0);

      // Iterate through each counter and verify the numbers are correct.
      for(Counter counter : counters) {
        if(counter.getQualifier().equals("source")) {
          Assert.assertTrue(counter.getValue() >= 10.0f);
        } else if(counter.getQualifier().equals("compute")
          && counter.getName().equals("processed")) {
          Assert.assertTrue(counter.getValue() >= 20.0f);
        } else if(counter.getQualifier().equals("sink")) {
          Assert.assertTrue(counter.getValue() >= 30.0f);
        } else if(counter.getQualifier().equals("compute")
          && counter.getName().equals("request.meanReate")) {
          Assert.assertTrue(counter.getValue() > 2.79103);
        }
      }
    } finally {
      serverInterface.stop(true);
    }
  }

  private List<Counter> getMetric(FlowArgument argument, List<String> names)
    throws TException, MetricsServiceException {
    CounterRequest request = new CounterRequest(argument);
    if(names != null) {
      request.setName(names);
    }
    return client.getCounters(request);
  }
}
