package com.continuuity.gateway;

import com.continuuity.data.engine.memory.MemoryQueueTable;
import com.continuuity.gateway.connector.flume.NettyFlumeConnector;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tests whether Flume events are properly transmitted through the gateway
 */
public class GatewayFlumeTest {

	private static final Logger LOG = LoggerFactory
			.getLogger(GatewayFlumeTest.class);

	static final String hostname = "localhost";
	static final String name = "fume";
	static final String stream = "foo";
	static final int port = 8765;
	static final int batchSize = 4;
	static final int eventsToSend = 10;

	Gateway setupGateway() throws Exception {
		Configuration configuration = new Configuration();
		configuration.set(Constants.CONFIG_CONNECTORS, name);
		configuration.set(Constants.connectorConfigName(name, Constants.CONFIG_CLASSNAME), NettyFlumeConnector.class.getCanonicalName());
		configuration.setInt(Constants.connectorConfigName(name, Constants.CONFIG_PORTNUMBER), port);
		Gateway gateway = new Gateway();
		gateway.configure(configuration);
		return gateway;
	}

	@Test
	public void testFlumeToQueue() throws Exception {
		Gateway gateway = setupGateway();
		MemoryQueueTable queues = new MemoryQueueTable();
		Consumer consumer = new QueueWritingConsumer(queues);
		gateway.setConsumer(consumer);

		gateway.start();
		Util.sendFlumeEvents(port, stream, eventsToSend, batchSize);
		gateway.stop();

		Assert.assertEquals(eventsToSend, consumer.eventsReceived());
		Assert.assertEquals(eventsToSend, consumer.eventsSucceeded());
		Assert.assertEquals(0, consumer.eventsFailed());

		Util.consumeQueue(queues, stream, name, eventsToSend);
	}
}
