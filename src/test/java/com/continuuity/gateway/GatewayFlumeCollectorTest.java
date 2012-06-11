package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.collector.NettyFlumeCollector;
import com.continuuity.gateway.consumer.EventWritingConsumer;
import com.continuuity.gateway.consumer.TupleWritingConsumer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tests whether Flume events are properly transmitted through the Gateway
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

  // This is the Gateway object we'll use for these tests
  private Gateway theGateway = null;

	// This is the data fabric operations executor
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
    port = Util.findFreePort();

    // Create and populate a new config object
    CConfiguration configuration = new CConfiguration();

		configuration.set(Constants.CONFIG_CONNECTORS, name);
		configuration.set(
        Constants.buildConnectorPropertyName(name, Constants.CONFIG_CLASSNAME),
        NettyFlumeCollector.class.getCanonicalName() );
	  configuration.setInt(
        Constants.buildConnectorPropertyName(name, Constants.CONFIG_PORT),port);

    // Now create our Gateway
		theGateway = new Gateway();
		theGateway.setExecutor(this.executor);
		theGateway.configure(configuration);

  } // end of setupGateway

  /**
   * Test that we can send simulated Flume events to a Queue
   *
   * @throws Exception If any exceptions happen during the test
   */
	@Test
	public void testFlumeToQueue() throws Exception {

		// Set up our consumer and queues
		EventWritingConsumer eventWritingConsumer = new EventWritingConsumer();
		eventWritingConsumer.setExecutor(this.executor);

		// Initialize and start the Gateway
		theGateway.setConsumer(eventWritingConsumer);
		theGateway.start();

		// Send some events
		Util.sendFlumeEvents(port, destination, eventsToSend, batchSize);
		Assert.assertEquals(eventsToSend, eventWritingConsumer.eventsReceived());
		Assert.assertEquals(eventsToSend, eventWritingConsumer.eventsSucceeded());
		Assert.assertEquals(0, eventWritingConsumer.eventsFailed());
		Util.consumeQueueAsEvents(this.executor, destination, name, eventsToSend);

		// Stop the Gateway
		theGateway.stop();

		// now switch the consumer to write the events as tuples
		TupleWritingConsumer tupleWritingConsumer = new TupleWritingConsumer();
		tupleWritingConsumer.setExecutor(this.executor);

		// and restart the gateway
		theGateway.setConsumer(tupleWritingConsumer);
		theGateway.start();

		// Send some events
		Util.sendFlumeEvents(port, destination, eventsToSend, batchSize);
		Assert.assertEquals(eventsToSend, tupleWritingConsumer.eventsReceived());
		Assert.assertEquals(eventsToSend, tupleWritingConsumer.eventsSucceeded());
		Assert.assertEquals(0, tupleWritingConsumer.eventsFailed());
		Util.consumeQueueAsTuples(this.executor, destination, name, eventsToSend);

		// Stop the Gateway
		theGateway.stop();
	}

} // end of GatewayFlumeCollectorTest
