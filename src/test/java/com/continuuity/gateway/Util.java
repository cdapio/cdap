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
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

public class Util {

	private static final Logger LOG = LoggerFactory.getLogger(Util.class);

	static int findFreePort() throws IOException {
		Socket socket = new Socket();
		socket.bind(null);
		int port = socket.getLocalPort();
		socket.close();
		return port;
	}

	static SimpleEvent createFlumeEvent(int messageNumber, String dest) {
		SimpleEvent event = new SimpleEvent();
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("messageNumber", Integer.toString(messageNumber));
		headers.put(Constants.HEADER_DESTINATION_ENDPOINT, dest);
		event.setHeaders(headers);
		event.setBody(createMessage(messageNumber));
		return event;
	}

	static byte[] createMessage(int messageNo) {
		return ("This is message " + messageNo + ".").getBytes();
	}

	static void sendFlumeEvents(int port, String dest, int numMessages, int batchSize)
      throws EventDeliveryException {

		RpcClient client = RpcClientFactory.
				getDefaultInstance("localhost", port, batchSize);
		try {
			List<org.apache.flume.Event> events = new ArrayList<org.apache.flume.Event>();
			for (int i = 0; i < numMessages; ) {
				events.clear();
				int bound = Math.min(i + batchSize, numMessages);
				for (; i < bound; i++) {
					events.add(createFlumeEvent(i, dest));
				}
				client.appendBatch(events);
			}
		} catch (EventDeliveryException e) {
			client.close();
			throw e;
		}
		client.close();
	}

	static void sendFlumeEvent(int port, SimpleEvent event) throws EventDeliveryException {
		RpcClient client = RpcClientFactory.
				getDefaultInstance("localhost", port, 1);
		try {
			client.append(event);
		} catch (EventDeliveryException e) {
			client.close();
			throw e;
		}
		client.close();
	}

	static HttpPost createHttpPost(int port, String prefix, String path, String dest, int messageNo) {
		String url = "http://localhost:" + port + prefix + path + dest;
		HttpPost post = new HttpPost(url);
		post.setHeader(dest + ".messageNumber", Integer.toString(messageNo));
		post.setEntity(new ByteArrayEntity(createMessage(messageNo)));
		return post;
	}

	static void sendRestEvent(HttpPost post) throws IOException {
		HttpClient client = new DefaultHttpClient();
		HttpResponse response = client.execute(post);
		int status = response.getStatusLine().getStatusCode();
		if (status != HttpStatus.SC_OK) {
			LOG.error("Error sending event: " + response.getStatusLine());
		}
		client.getConnectionManager().shutdown();
	}

	static void sendRestEvents(int port, String prefix, String path, String dest, int eventsToSend)
			throws IOException {
		for (int i = 0; i < eventsToSend; i++) {
			Util.sendRestEvent(createHttpPost(port, prefix, path, dest, i));
		}
	}

	static void verifyEvent(Event event, String collectorName, String destination, Integer expectedNo) {
		Assert.assertNotNull(event.getHeader("messageNumber"));
		int messageNumber = Integer.valueOf(event.getHeader("messageNumber"));
		if (expectedNo != null) Assert.assertEquals(messageNumber, expectedNo.intValue());
		if (collectorName != null) Assert.assertEquals(collectorName, event.getHeader(Constants.HEADER_FROM_COLLECTOR));
		if (destination != null) Assert.assertEquals(destination, event.getHeader(Constants.HEADER_DESTINATION_ENDPOINT));
		Assert.assertArrayEquals(createMessage(messageNumber), event.getBody());
	}

	static class NoopConsumer extends Consumer {
		@Override
		public void single(Event event) { }
	}

	static class VerifyConsumer extends Consumer {
		Integer expectedNumber = null;
		String collectorName = null, destination = null;
		VerifyConsumer(String name, String dest) {
			this.collectorName = name;
			this.destination = dest;
		};
		VerifyConsumer(int expected, String name, String dest) {
			this.expectedNumber = expected;
			this.collectorName = name;
			this.destination = dest;
		}
		@Override
		protected void single(Event event) throws Exception {
			Util.verifyEvent(event, this.collectorName, this.destination, this.expectedNumber);
		}
	}

	static void consumeQueue(MemoryQueueTable queues, String queueName, String collectorName, int eventsExpected) throws Exception {
		QueueConsumer consumer = new QueueConsumer(0, 0, 1, true, false);
		QueueConfig config = new QueueConfig(new QueuePartitioner.RandomPartitioner(), true);
		EventSerializer deserializer = new EventSerializer();
		for (int remaining = eventsExpected; remaining > 0; --remaining) {
			QueueEntry entry = queues.pop(queueName.getBytes(), consumer, config, false);
			Event event = deserializer.deserialize(entry.getValue());
			Util.verifyEvent(event, collectorName, queueName, null);
			LOG.info("Popped one event, message number: " + event.getHeader("messageNumber"));
			queues.ack(queueName.getBytes(), entry);
		}
	}
}
