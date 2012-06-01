package com.continuuity.gateway;

import com.continuuity.data.engine.memory.MemoryQueueTable;
import com.continuuity.data.operation.queue.QueueConfig;
import com.continuuity.data.operation.queue.QueueConsumer;
import com.continuuity.data.operation.queue.QueueEntry;
import com.continuuity.data.operation.queue.QueuePartitioner;
import com.continuuity.flow.flowlet.api.Event;
import com.continuuity.flow.flowlet.impl.EventSerializer;
import com.continuuity.gateway.connector.rest.RestConnector;
import com.continuuity.gateway.connector.rest.RestHandler;
import org.apache.flume.EventDeliveryException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class GatewayRestTest {

	private static final Logger LOG = LoggerFactory
			.getLogger(GatewayRestTest.class);

	static final int port = 4321;
	static final int eventsToSend = 10;
	static final String dest = "pfunk";

	static byte[] createMessage(int messageNo) {
		return ("This is message " + messageNo + ".").getBytes();
	}

	Gateway setupGateway() throws Exception {
		Gateway gateway = new Gateway();
		RestConnector connector = new RestConnector();
		connector.setPort(port);
		connector.setName("rest");
		gateway.addConnector(connector);
		return gateway;
	}

	void sendRestEvents () throws EventDeliveryException {
		String path = RestHandler.PATH_PREFIX + dest;
		for (int i = 0; i < eventsToSend; i++) {
			Map<String, String> headers = new HashMap<String, String>();
			headers.put(dest + ".messageNumber", Integer.toString(i));
			byte[] body = createMessage(i);
			send(port, path, headers, body);
		}
	}

	private void send(int port, String path, Map<String, String> headers, byte[] body) {
		try {
			String url = "http://localhost:" + port + path;
			HttpPost post = new HttpPost(url);
			for (String header : headers.keySet()) {
				post.setHeader(header, headers.get(header));
			}
			post.setEntity(new ByteArrayEntity(body));
			HttpClient client = new DefaultHttpClient();
			HttpResponse response = client.execute(post);
			int status = response.getStatusLine().getStatusCode();
			if (status != HttpStatus.SC_OK) {
				LOG.error("Error sending event: " + response.getStatusLine());
			}
			client.getConnectionManager().shutdown();
		} catch (Exception e) {
			LOG.error("Exception while sending event: " + e.getMessage());
		}
	}

	void consumeQueue(MemoryQueueTable queues) throws Exception {
		QueueConsumer consumer = new QueueConsumer(0, 0, 1, true, false);
		QueueConfig config = new QueueConfig(new QueuePartitioner.RandomPartitioner(), true);
		EventSerializer deserializer = new EventSerializer();
		for (int remaining = eventsToSend; remaining > 0; --remaining) {
			QueueEntry entry = queues.pop(dest.getBytes(), consumer, config, false);
			Event event = deserializer.deserialize(entry.getValue());
			Assert.assertEquals("rest", event.getHeader(Constants.HEADER_FROM_CONNECTOR));
			String header = event.getHeader("messageNumber");
			Assert.assertNotNull(header);
			int messageNumber = Integer.valueOf(header);
			LOG.info("Popped one event number: " + messageNumber);
			Assert.assertTrue(messageNumber >= 0 && messageNumber < eventsToSend);
			Assert.assertArrayEquals(event.getBody(), createMessage(messageNumber));
			queues.ack(dest.getBytes(), entry);
		}
	}

	@Test
	public void testFlumeToQueue() throws Exception {
		Gateway gateway = setupGateway();
		MemoryQueueTable queues = new MemoryQueueTable();
		Consumer consumer = new QueueWritingConsumer(queues);
		gateway.setConsumer(consumer);

		gateway.start();
		this.sendRestEvents();
		gateway.stop();

		Assert.assertEquals(eventsToSend, consumer.eventsReceived());
		Assert.assertEquals(eventsToSend, consumer.eventsSucceeded());
		Assert.assertEquals(0, consumer.eventsFailed());

		consumeQueue(queues);
	}

}
