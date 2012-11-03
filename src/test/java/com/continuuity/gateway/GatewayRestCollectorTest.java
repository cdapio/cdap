package com.continuuity.gateway;

import com.continuuity.api.data.OperationContext;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.collector.RestCollector;
import com.continuuity.gateway.consumer.EventWritingConsumer;
import com.continuuity.gateway.consumer.PrintlnConsumer;
import com.continuuity.gateway.consumer.TupleWritingConsumer;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Stream;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tests whether REST events are properly transmitted through the Gateway
 */
public class GatewayRestCollectorTest {

  // Our logger object
  private static final Logger LOG = LoggerFactory
      .getLogger(GatewayRestCollectorTest.class);

  // A set of constants we'll use in these tests
  static String name = "collect.rest";
  static final String prefix = "";
  static final String path = "/stream/";
  static final String stream = "pfunk";
  static final int eventsToSend = 10;
  static int port = 10000;

  // This is the Gateway object we'll use for these tests
  private Gateway theGateway = null;

  // This is the configuration object we'll use in the tests
  CConfiguration myConfiguration;

  private OperationExecutor executor;

  /**
   * Create a new Gateway instance to use in these set of tests. This method
   * is called before any of the test methods.
   *
   * @throws Exception If the Gateway can not be created.
   */
  @Before
  public void setupGateway() throws Exception {

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
        new DataFabricModules().getInMemoryModules());
    this.executor = injector.getInstance(OperationExecutor.class);

    // Look for a free port
    port = PortDetector.findFreePort();

    // Create and populate a new config object
    myConfiguration = new CConfiguration();

    myConfiguration.setBoolean(Constants.CONFIG_DO_SERVICE_DISCOVERY, false);
    myConfiguration.set(Constants.CONFIG_CONNECTORS, name);
    myConfiguration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_CLASSNAME), RestCollector.class.getCanonicalName());
    myConfiguration.setInt(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PORT), port);
    myConfiguration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_PREFIX), prefix);
    myConfiguration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_MIDDLE), path);

    // Now create our Gateway
    theGateway = new Gateway();
    theGateway.setExecutor(this.executor);

    // Set up a basic consumer
    Consumer theConsumer = new PrintlnConsumer();
    theGateway.setConsumer(theConsumer);

    // make sure the destination stream is defined in the meta data
    MetadataService mds = new MetadataService(this.executor);
    Stream stream = new Stream(this.stream);
    stream.setName(this.stream);
    mds.createStream(new Account(OperationContext.DEFAULT_ACCOUNT_ID), stream);

  } // end of setupGateway


  /**
   * Test that we can send simulated REST events to a Queue using
   * TupleWriteConsumer.
   *
   * @throws Exception If any exceptions happen during the test
   */
  @Test
  public void testRestToQueueWithTupleWriteConsumer() throws Exception {
    // now switch the consumer to write the events as tuples
    TupleWritingConsumer tupleWritingConsumer = new TupleWritingConsumer();
    tupleWritingConsumer.setExecutor(this.executor);

    // and restart the gateway
    theGateway.setConsumer(tupleWritingConsumer);
    theGateway.start(null, myConfiguration);

    // Send some REST events and verify them
    TestUtil.sendRestEvents(port, prefix, path, stream, eventsToSend);
    Assert.assertEquals(eventsToSend, tupleWritingConsumer.eventsReceived());
    Assert.assertEquals(eventsToSend, tupleWritingConsumer.eventsSucceeded());
    Assert.assertEquals(0, tupleWritingConsumer.eventsFailed());
    TestUtil.consumeQueueAsTuples(this.executor, stream, name, eventsToSend);

    // Stop the Gateway
    theGateway.stop(false);
  }

  /**
   * Test that we can send simulated REST events to a Queue using
   * EventWritingConsumer.
   *
   * NOTE: This has been seperated out from the above test till we figure
   * out how OMID can handle multiple write format that gets on queue. No
   * Ignore is added for test on purpose.
   *
   * @throws Exception If any exceptions happen during the test
   */
  public void testRestToQueueWithEventWriteConsumer() throws Exception {

    // Set up our consumer and queues
    EventWritingConsumer eventWritingConsumer = new EventWritingConsumer();
    eventWritingConsumer.setExecutor(this.executor);

    // Initialize and start the Gateway
    theGateway.setConsumer(eventWritingConsumer);

    try {
      theGateway.start(null, myConfiguration);
    } catch (ServerException e) {
      // We don't care about the reconfigure problem in this test
      LOG.debug(e.getMessage());
    }

    // Send some REST events and verify them
    TestUtil.sendRestEvents(port, prefix, path, stream, eventsToSend);
    Assert.assertEquals(eventsToSend, eventWritingConsumer.eventsReceived());
    Assert.assertEquals(eventsToSend, eventWritingConsumer.eventsSucceeded());
    Assert.assertEquals(0, eventWritingConsumer.eventsFailed());
    TestUtil.consumeQueueAsEvents(this.executor, stream, name, eventsToSend);

    // Stop the Gateway
    theGateway.stop(false);
  }

} // end of GatewayRestCollectorTest
