package com.continuuity.gateway;

import com.continuuity.data.engine.memory.MemoryQueueTable;
import com.continuuity.gateway.connector.rest.RestConnector;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GatewayRestTest {

	private static final Logger LOG = LoggerFactory
			.getLogger(GatewayRestTest.class);

	static final String name = "rest";
	static final String prefix = "";
	static final String path = "/stream/";
	static final String dest = "pfunk";
	static final int eventsToSend = 10;

	static byte[] createMessage(int messageNo) {
		return ("This is message " + messageNo + ".").getBytes();
	}

	Gateway setupGateway(int port) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set(Constants.CONFIG_CONNECTORS, name);
		configuration.set(Constants.connectorConfigName(name, Constants.CONFIG_CLASSNAME), RestConnector.class.getCanonicalName());
		configuration.setInt(Constants.connectorConfigName(name, Constants.CONFIG_PORTNUMBER), port);
		configuration.set(Constants.connectorConfigName(name, Constants.CONFIG_PATH_PREFIX), prefix);
		configuration.set(Constants.connectorConfigName(name, Constants.CONFIG_PATH_STREAM), path);
		Gateway gateway = new Gateway();
		gateway.configure(configuration);
		return gateway;
	}

	@Test
	public void testRestToQueue() throws Exception {
		int port = Util.findFreePort();
		Gateway gateway = setupGateway(port);
		MemoryQueueTable queues = new MemoryQueueTable();
		Consumer consumer = new QueueWritingConsumer(queues);
		gateway.setConsumer(consumer);

		gateway.start();
		Util.sendRestEvents(port, prefix, path, dest, eventsToSend);
		gateway.stop();

		Assert.assertEquals(eventsToSend, consumer.eventsReceived());
		Assert.assertEquals(eventsToSend, consumer.eventsSucceeded());
		Assert.assertEquals(0, consumer.eventsFailed());

		Util.consumeQueue(queues, dest, name, eventsToSend);
	}

}
