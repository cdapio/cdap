package com.continuuity.gateway;

import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEntryPointer;
import com.continuuity.data.operation.type.WriteOperation;
import com.continuuity.flow.flowlet.api.Event;
import com.continuuity.flow.flowlet.impl.EventSerializer;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.SimpleEvent;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
		headers.put(Constants.HEADER_DESTINATION_STREAM, dest);
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
		if (destination != null) Assert.assertEquals(destination, event.getHeader(Constants.HEADER_DESTINATION_STREAM));
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

	static void consumeQueue(OperationExecutor executor, String queueName,
													 String collectorName, int eventsExpected) throws Exception {

		EventSerializer deserializer = new EventSerializer();
		com.continuuity.data.operation.ttqueue.QueueConsumer consumer = new com.continuuity.data.operation.ttqueue.QueueConsumer(0, 0, 1);
		com.continuuity.data.operation.ttqueue.QueueConfig config =
				new com.continuuity.data.operation.ttqueue.QueueConfig(
						new com.continuuity.data.operation.ttqueue.QueuePartitioner.RandomPartitioner(), true);
		QueueDequeue dequeue = new QueueDequeue(queueName.getBytes(), consumer, config);
		for (int remaining = eventsExpected; remaining > 0; --remaining) {
			DequeueResult result = executor.execute(dequeue);
			Assert.assertTrue(result.isSuccess());
			QueueEntryPointer ackPointer = result.getEntryPointer();
			Event event = deserializer.deserialize(result.getValue());
			Util.verifyEvent(event, collectorName, queueName, null);
			LOG.info("Popped one event, message number: " + event.getHeader("messageNumber"));
			QueueAck ack = new QueueAck(queueName.getBytes(), ackPointer, consumer);
			List<WriteOperation> operations = new ArrayList<WriteOperation>(1);
			operations.add(ack);
			Assert.assertTrue(executor.execute(operations).isSuccess());
		}
	}

	/**
	 * Verify that a given value can be retrieved for a given key via http GET request
	 * @param executor the operation executor to use for access to data fabric
	 * @param baseUri The URI for get request, without the key
	 * @param key The key
	 * @param value The value
	 * @throws Exception if an exception occurs
	 */
	static void testKeyValue(OperationExecutor executor,
										String baseUri, byte[] key, byte[] value) throws Exception {
		// add the key/value to the data fabric
		Write write = new Write(key, value);
		List<WriteOperation> operations = new ArrayList<WriteOperation>(1);
		operations.add(write);
		Assert.assertTrue(executor.execute(operations).isSuccess());

		// make a get URL
		String getUrl = baseUri + URLEncoder.encode(new String(key, "ISO8859_1"), "ISO8859_1");
		LOG.info("GET request URI for key '" + new String(key) + "' is " + getUrl);

		// and issue a GET request to the server
		HttpClient client = new DefaultHttpClient();
		HttpResponse response = client.execute(new HttpGet(getUrl));
		client.getConnectionManager().shutdown();

		// verify the response is ok
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());

		// verify the length of the return value is the same as the original value's
		int length = (int)response.getEntity().getContentLength();
		Assert.assertEquals(value.length, length);

		// verify that the value is actually the same
		InputStream content = response.getEntity().getContent();
		byte[] bytes = new byte[length];
		content.read(bytes);
		Assert.assertArrayEquals(value, bytes);
	}

	/**
	 * Verify that a given value can be retrieved for a given key via http GET request.
	 * This converts the key and value from String to bytes and calls the byte-based
	 * method testKeyValue.
	 * @param executor the operation executor to use for access to data fabric
	 * @param baseUri The URI for get request, without the key
	 * @param key The key
	 * @param value The value
	 * @throws Exception if an exception occurs
	 */
	static void testKeyValue(OperationExecutor executor,
										String baseUri, String key, String value) throws Exception {
		testKeyValue(executor, baseUri, key.getBytes(), value.getBytes());
	}


}
