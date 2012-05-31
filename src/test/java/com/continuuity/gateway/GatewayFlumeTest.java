package com.continuuity.gateway;

import com.continuuity.data.engine.memory.MemoryQueueTable;
import com.continuuity.data.operation.queue.QueueConfig;
import com.continuuity.data.operation.queue.QueueConsumer;
import com.continuuity.data.operation.queue.QueueEntry;
import com.continuuity.data.operation.queue.QueuePartitioner;
import com.continuuity.flow.flowlet.api.Event;
import com.continuuity.flow.flowlet.impl.EventSerializer;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.SimpleEvent;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This tests whether Flume events are properly transmitted through the gateway
 */
public class GatewayFlumeTest {

	private static final Logger LOG = LoggerFactory
			.getLogger(GatewayFlumeTest.class);

	static final String hostname = "localhost";
	static final int port = 8765;
	static final int batchSize = 4;
	static final int eventsToSend = 10;

	static byte[] createMessage(int messageNo) {
		return ("This is message " + messageNo + ".").getBytes();
	}

	void sendFlumeEvents () throws EventDeliveryException {
		RpcClient client = RpcClientFactory.
				getDefaultInstance(hostname, port, batchSize);

		for (int i = 0; i < eventsToSend; i++) {
			SimpleEvent event = new SimpleEvent();
			Map<String, String> headers = new HashMap<String, String>();
			headers.put("messageNumber", Integer.toString(i));
			event.setHeaders(headers);
			event.setBody(createMessage(i));
			client.append(event);
		}
		client.close();
	}

	Gateway setupGateway() throws Exception {
		Gateway gateway = new Gateway();
		FlumeConnector connector = new NettyFlumeConnector();
		connector.setHost(hostname);
		connector.setPort(port);
		connector.setName("flume");
		gateway.addConnector(connector);
		return gateway;
	}

	void consumeQueue(MemoryQueueTable queues) throws Exception {
		QueueConsumer consumer = new QueueConsumer(0, 0, 1, true, false);
		QueueConfig config = new QueueConfig(new QueuePartitioner.RandomPartitioner(), true);
		EventSerializer deserializer = new EventSerializer();
		for (int remaining = eventsToSend; remaining > 0; --remaining) {
			QueueEntry entry = queues.pop("default".getBytes(), consumer, config, false);
			Event event = deserializer.deserialize(entry.getValue());
			Assert.assertEquals("flume", event.getHeader(Constants.HEADER_FROM_CONNECTOR));
			String header = event.getHeader("messageNumber");
			Assert.assertNotNull(header);
			int messageNumber = Integer.valueOf(header);
			LOG.info("Popped one event number: " + messageNumber);
			Assert.assertTrue(messageNumber >= 0 && messageNumber < eventsToSend);
			Assert.assertArrayEquals(event.getBody(), createMessage(messageNumber));
			queues.ack("default".getBytes(), entry);
		}
	}

	@Test
	public void testFlumeToQueue() throws Exception {
		Gateway gateway = setupGateway();
		MemoryQueueTable queues = new MemoryQueueTable();
		Consumer consumer = new QueueWritingConsumer(queues);
		gateway.setConsumer(consumer);

		gateway.start();
		this.sendFlumeEvents();
		gateway.stop();

		Assert.assertEquals(eventsToSend, consumer.eventsReceived());
		Assert.assertEquals(eventsToSend, consumer.eventsSucceeded());
		Assert.assertEquals(0, consumer.eventsFailed());

		consumeQueue(queues);
	}
}
