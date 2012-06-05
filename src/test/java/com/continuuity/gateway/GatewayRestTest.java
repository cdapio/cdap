package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryQueueTable;
import com.continuuity.gateway.collector.RestCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tests whether REST events are properly transmitted through the Gateway
 */
public class GatewayRestTest {

  // Our logger object
	private static final Logger LOG = LoggerFactory
			.getLogger(GatewayRestTest.class);

  // A set of constants we'll use in these tests
	static  String name = "rest.RestCollect";
	static final String prefix = "";
	static final String path = "/stream/";
	static final String dest = "pfunk";
	static final int eventsToSend = 10;
  static int port = 10000;

  // This is the Gateway object we'll use for these tests
  private Gateway theGateway = null;

  /**
   * Create a new Gateway instance to use in these set of tests. This method
   * is called before any of the test methods.
   *
   * @throws Exception If the Gateway can not be created.
   */
  @Before
  public void setupGateway() throws Exception {

    // Look for a free port
    port = Util.findFreePort();

    // Create and populate a new config object
    CConfiguration configuration = new CConfiguration();

    configuration.set(Constants.CONFIG_COLLECTORS, name);
    configuration.set(Constants.buildCollectorPropertyName(name,
				Constants.CONFIG_CLASSNAME), RestCollector.class.getCanonicalName());
    configuration.setInt(Constants.buildCollectorPropertyName(name,
				Constants.CONFIG_PORT),port);
    configuration.set(Constants.buildCollectorPropertyName(name,
				Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.buildCollectorPropertyName(name,
				Constants.CONFIG_PATH_STREAM), path);

    // Now create our Gateway
    theGateway = new Gateway();
    theGateway.configure(configuration);

  } // end of setupGateway


  /**
   * Test that we can send simulated REST events to a Queue
   *
   * @throws Exception If any exceptions happen during the test
   */
	@Test
	public void testRestToQueue() throws Exception {

    // Set up our consumer and queues
    QueueWritingConsumer consumer = new QueueWritingConsumer();
    MemoryQueueTable queues = new MemoryQueueTable();
    consumer.setQueueTable(queues);

    // Initialize and start the Gateway
    theGateway.setConsumer(consumer);
    theGateway.start();

    // Send some REST events
		Util.sendRestEvents(port, prefix, path, dest, eventsToSend);

    // Stop the Gateway
    theGateway.stop();

    Assert.assertEquals(eventsToSend, consumer.eventsReceived());
		Assert.assertEquals(eventsToSend, consumer.eventsSucceeded());
		Assert.assertEquals(0, consumer.eventsFailed());

    // Clean out the queue
		Util.consumeQueue(queues, dest, name, eventsToSend);
	}

} // end of GatewayRestTest
