package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.gateway.collector.NettyFlumeCollector;
import com.continuuity.gateway.consumer.PrintlnConsumer;
import com.continuuity.gateway.consumer.StreamEventWritingConsumer;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Stream;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This tests whether Flume events are properly transmitted through the Gateway.
 */
public class GatewayFlumeCollectorTest {

  // Our logger object
  private static final Logger LOG = LoggerFactory
      .getLogger(GatewayFlumeCollectorTest.class);

  // A set of constants we'll use in this test
  static final String name = "collect.flume";
  static final String destination = "foo";
  static final int batchSize = 4;
  static final int eventsToSend = 10;
  static int port = 10000;

  static final String apiKey = "SampleTestApiKey";
  static final String cluster = "SampleTestClusterName";
  
  // This is the Gateway object we'll use for these tests
  private Gateway theGateway = null;

  // This is the data fabric operations executor
  private OperationExecutor executor;

  // This is the configuration object we will use in these tests
  private CConfiguration myConfiguration;

  /**
   * Create a new Gateway instance to use in these set of tests. This method
   * is called before any of the test methods.
   *
   * @throws Exception If the Gateway can not be created.
   */
  @Before
  public void setupGateway() throws Exception {
    myConfiguration = new CConfiguration();

    // Set up our Guice injections
    Injector injector = Guice.createInjector(new GatewayTestModule(myConfiguration));
    this.executor = injector.getInstance(OperationExecutor.class);

    // Look for a free port
    port = PortDetector.findFreePort();

    // Create and populate a new config object

    myConfiguration.setBoolean(Constants.CONFIG_DO_SERVICE_DISCOVERY, false);
    myConfiguration.set(Constants.CONFIG_CONNECTORS, name);
    myConfiguration.set(
        Constants.buildConnectorPropertyName(name, Constants.CONFIG_CLASSNAME),
        NettyFlumeCollector.class.getCanonicalName());
    myConfiguration.setInt(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);

    // Now create our Gateway
    theGateway = new Gateway();
    theGateway.setExecutor(this.executor);
    theGateway.setDiscoveryServiceClient(
        injector.getInstance(DiscoveryServiceClient.class));

    // Set up a basic consumer
    Consumer theConsumer = new PrintlnConsumer();
    theGateway.setConsumer(theConsumer);

    // make sure the destination stream is defined in the meta data
    MetadataService mds = new MetadataService(this.executor);
    Stream stream = new Stream(destination);
    stream.setName(destination);
    mds.assertStream(new Account(TestUtil.DEFAULT_ACCOUNT_ID), stream);
  } // end of setupGateway

  /**
   * Test that we can send simulated Flume events to a Queue using
   * EventWritingConsumer.
   *
   * NOTE: This has been seperated out from the above test till we figure
   * out how OMID can handle multiple write format that gets on queue. No
   * Ignore is added for test on purpose.
   *
   * @throws Exception If any exceptions happen during the test
   */
  @Test
  public void testWithAuthFlumeToQueueWithStreamEventWritingConsumer() throws Exception {

    // Set up our consumer and queues
    StreamEventWritingConsumer consumer = new StreamEventWritingConsumer();
    consumer.setExecutor(this.executor);


    // Set the mocked passport client for authentication
    // Authentication configuration
    myConfiguration.setBoolean(Constants.CONFIG_AUTHENTICATION_REQUIRED, true);
    myConfiguration.set(Constants.CONFIG_CLUSTER_NAME, cluster);
    Map<String, List<String>> keysAndClusters =
      new TreeMap<String, List<String>>();
    keysAndClusters.put(apiKey, Arrays.asList(cluster));
    theGateway.setPassportClient(new MockedPassportClient(keysAndClusters));
    theGateway.setConsumer(consumer);

    // Initialize and start the Gateway
    try {
      theGateway.start(null, myConfiguration);
    } catch (ServerException e) {
      // We don't care about the reconfigure problem in this test
      LOG.debug(e.getMessage());
    }

    // Send some events
    TestUtil.enableAuth(apiKey);
    TestUtil.sendFlumeEvents(port, destination, eventsToSend, batchSize);
    Assert.assertEquals(eventsToSend, consumer.eventsReceived());
    Assert.assertEquals(eventsToSend, consumer.eventsSucceeded());
    Assert.assertEquals(0, consumer.eventsFailed());
    TestUtil.consumeQueueAsEvents(this.executor, destination, name, eventsToSend);
    TestUtil.disableAuth();

    // Stop the Gateway
    theGateway.stop(false);
  }

  /**
   * Test that we can send simulated Flume events to a Queue using
   * StreamEventWritingConsumer.
   *
   * @throws Exception If any exceptions happen during the test
   */
  @Test
  public void testWithNoAuthFlumeToQueueWithStreamEventWritingConsumer() throws Exception {

    // Set up our consumer and queues
    StreamEventWritingConsumer consumer = new StreamEventWritingConsumer();
    consumer.setExecutor(this.executor);

    // Initialize and start the Gateway
    myConfiguration.setBoolean(Constants.CONFIG_AUTHENTICATION_REQUIRED, false);
    theGateway.setConsumer(consumer);

    try {
      theGateway.start(null, myConfiguration);
    } catch (ServerException e) {
      // We don't care about the reconfigure problem in this test
      LOG.debug(e.getMessage());
    }

    // Send some events
    TestUtil.sendFlumeEvents(port, destination, eventsToSend, batchSize);
    Assert.assertEquals(eventsToSend, consumer.eventsReceived());
    Assert.assertEquals(eventsToSend, consumer.eventsSucceeded());
    Assert.assertEquals(0, consumer.eventsFailed());
    TestUtil.consumeQueueAsEvents(this.executor, destination, name,
                                  eventsToSend);

    // Stop the Gateway
    theGateway.stop(false);
  }

} // end of GatewayFlumeCollectorTest
