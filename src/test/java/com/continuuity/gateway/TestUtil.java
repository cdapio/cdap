package com.continuuity.gateway;

import com.continuuity.api.data.*;
import com.continuuity.api.flow.flowlet.Event;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.*;
import com.continuuity.flow.definition.impl.FlowStream;
import com.continuuity.flow.flowlet.internal.EventSerializer;
import com.continuuity.flow.flowlet.internal.TupleSerializer;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.SimpleEvent;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.continuuity.data.operation.ttqueue.QueueAdmin.QueueInfo;

public class TestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(TestUtil.class);

  static final OperationContext context = OperationContext.DEFAULT;

  /**
   * Creates a string containing a number
   *
   * @param i The number to use
   * @return a message as a byte a array
   */
  static byte[] createMessage(int i) {
    return ("This is message " + i + ".").getBytes();
  }

  /**
   * Creates a flume event that has
   * <ul>
   * <li>a header named "messageNumber" with the string value of a number i</li>
   * <li>a header named "HEADER_DESTINATION_STREAM" with the name of a
   *  destination</li>
   * <li>a body with the text "This is message number i.</li>
   * </ul>
   *
   * @param i    The number to use
   * @param dest The destination name to use
   * @return a flume event
   */
  static SimpleEvent createFlumeEvent(int i, String dest) {
    SimpleEvent event = new SimpleEvent();
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("messageNumber", Integer.toString(i));
    headers.put(Constants.HEADER_DESTINATION_STREAM, dest);
    event.setHeaders(headers);
    event.setBody(createMessage(i));
    return event;
  }

  /**
   * Uses Flumes RPC client to send a number of flume events to a specified
   * port.
   *
   * @param port        The port to use
   * @param dest        The destination name to use as the destination
   * @param numMessages How many messages to send
   * @param batchSize   Batch size to use for sending
   * @throws EventDeliveryException If sending fails for any reason
   */
  static void sendFlumeEvents(int port, String dest, int numMessages,
                              int batchSize)
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

  /**
   * Uses Flume's RPC client to send a single event to a port
   *
   * @param port  The port to use
   * @param event The event
   * @throws EventDeliveryException If something went wrong while sending
   */
  static void sendFlumeEvent(int port, SimpleEvent event)
      throws EventDeliveryException {
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

  /**
   * Creates an HTTP post that has
   * <ul>
   * <li>a header named "messageNumber" with the string value of a number i</li>
   * <li>a body with the text "This is message number i.</li>
   * </ul>
   *
   * @param i The number to use
   * @return a flume event
   */
  static HttpPost createHttpPost(int port, String prefix, String path,
                                 String dest, int i) {
    String url = "http://localhost:" + port + prefix + path + dest;
    HttpPost post = new HttpPost(url);
    post.setHeader(dest + ".messageNumber", Integer.toString(i));
    post.setEntity(new ByteArrayEntity(createMessage(i)));
    return post;
  }

  /**
   * Send an event to rest collector
   *
   * @param post The event as an Http post (see createHttpPost)
   * @throws IOException if sending fails
   */
  static void sendRestEvent(HttpPost post) throws IOException {
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(post);
    int status = response.getStatusLine().getStatusCode();
    if (status != HttpStatus.SC_OK) {
      LOG.error("Error sending event: " + response.getStatusLine());
    }
    client.getConnectionManager().shutdown();
  }

  /**
   * Creates an HTTP post for an event and sends it to the rest collector
   *
   * @param port   The port as configured for the collector
   * @param prefix The path prefix as configured for the collector
   * @param path   The path as configured for the collector
   * @param dest   The destination name to use as the destination
   * @throws IOException if sending fails
   */
  static void sendRestEvents(int port, String prefix, String path,
                             String dest, int eventsToSend)
      throws IOException {
    for (int i = 0; i < eventsToSend; i++) {
      TestUtil.sendRestEvent(createHttpPost(port, prefix, path, dest, i));
    }
  }

  /**
   * Verify that an event corresponds to the form as created by
   * createFlumeEvent or createHttpPost
   *
   * @param event         The event to verify
   * @param collectorName The name of the collector that the event was sent to
   * @param destination   The name of the destination that the event was routed
   *                      to
   * @param expectedNo    The number of the event, it should be both in the
   *                      messageNumber header and in the body.
   *                      If null, then this method checks whether the number
   *                      in the header matches the body.
   */
  static void verifyEvent(Event event, String collectorName,
                          String destination, Integer expectedNo) {
    Assert.assertNotNull(event.getHeader("messageNumber"));
    int messageNumber = Integer.valueOf(event.getHeader("messageNumber"));
    if (expectedNo != null)
      Assert.assertEquals(messageNumber, expectedNo.intValue());
    if (collectorName != null)
      Assert.assertEquals(collectorName,
          event.getHeader(Constants.HEADER_FROM_COLLECTOR));
    if (destination != null)
      Assert.assertEquals(destination,
          event.getHeader(Constants.HEADER_DESTINATION_STREAM));
    Assert.assertArrayEquals(createMessage(messageNumber), event.getBody());
  }

  /**
   * Verify that an tuple corresponds to an event of the form as created by
   * createFlumeEvent or createHttpPost
   *
   * @param tuple         The tuple to verify
   * @param collectorName The name of the collector that the event was sent to
   * @param destination   The name of the destination that the event was routed
   *                      to
   * @param expectedNo    The number of the event, it should be both in the
   *                      messageNumber header and in the body.
   *                      If null, then this method checks whether the number
   *                      in the header matches the body.
   */
  static void verifyTuple(Tuple tuple, String collectorName,
                          String destination, Integer expectedNo) {
    Map<String, String> headers = tuple.get("headers");
    Assert.assertNotNull(headers.get("messageNumber"));
    int messageNumber = Integer.valueOf(headers.get("messageNumber"));
    if (expectedNo != null)
      Assert.assertEquals(messageNumber, expectedNo.intValue());
    if (collectorName != null)
      Assert.assertEquals(collectorName,
          headers.get(Constants.HEADER_FROM_COLLECTOR));
    if (destination != null)
      Assert.assertEquals(destination,
          headers.get(Constants.HEADER_DESTINATION_STREAM));
    Assert.assertArrayEquals(
        createMessage(messageNumber), (byte[]) tuple.get("body"));
  }

  public static void verifyQueueInfo(OperationExecutor executor,
                                     int port, String prefix, String path,
                                     String stream)
      throws Exception {
    // get the queue info from opex
    byte[] queueName = FlowStream.buildStreamURI(
        Constants.defaultAccount, stream).toString().getBytes();
    OperationResult<QueueInfo> info = executor.execute(
        OperationContext.DEFAULT, new QueueAdmin.GetQueueInfo(queueName));
    String json = info.isEmpty() ? null : info.getValue().getJSONString();

    // get the queue info via HTTP
    String url = "http://localhost:" + port + prefix + path + stream;
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(new HttpGet(url));
    client.getConnectionManager().shutdown();
    String json2 = null;
    if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
      int length = (int)response.getEntity().getContentLength();
      // verify that the value is actually the same
      InputStream content = response.getEntity().getContent();
      if (length > 0) {
        byte[] bytes = new byte[length];
        int bytesRead = 0;
        while (bytesRead < length) {
          int readNow = content.read(bytes, bytesRead, length - bytesRead);
          if (readNow < 0)
            Assert.fail("failed to read entire response content");
          bytesRead += readNow;
        }
        Assert.assertEquals(length, bytesRead);
        json2 = new String(bytes);
      }
    }

    // verify they match
    Assert.assertEquals(json, json2);
  }

  /**
   * A consumer that does nothing
   */
  static class NoopConsumer extends Consumer {
    @Override
    public void single(Event event) {
    }
  }

  /**
   * A consumer that verifies events, optionally for a fixed message number.
   * See verifyEvent for moire details.
   */
  static class VerifyConsumer extends Consumer {
    Integer expectedNumber = null;
    String collectorName = null, destination = null;

    VerifyConsumer(String name, String dest) {
      this.collectorName = name;
      this.destination = dest;
    }

    VerifyConsumer(int expected, String name, String dest) {
      this.expectedNumber = expected;
      this.collectorName = name;
      this.destination = dest;
    }

    @Override
    protected void single(Event event) throws Exception {
      TestUtil.verifyEvent(event, this.collectorName,
          this.destination, this.expectedNumber);
    }
  }

  /**
   * Consume the events in a queue and verify that they correspond to the
   * format as created by createHttpPost() or createFlumeEvent()
   *
   * @param executor       The executor to use for access to the data fabric
   * @param destination    The name of the flow (destination) that the events
   *                       were sent to
   * @param collectorName  The name of the collector that received the events
   * @param eventsExpected How many events should be read
   * @throws Exception
   */
  static void consumeQueueAsEvents(OperationExecutor executor,
                                   String destination,
                                   String collectorName,
                                   int eventsExpected) throws Exception {
    // address the correct queue
    byte[] queueURI = FlowStream.buildStreamURI(
        Constants.defaultAccount, destination).toString().getBytes();
    // one deserializer to reuse
    EventSerializer deserializer = new EventSerializer();
    // prepare the queue consumer
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueueConfig config =
        new QueueConfig(QueuePartitioner.PartitionerType.RANDOM, true);
    QueueDequeue dequeue = new QueueDequeue(queueURI, consumer, config);
    for (int remaining = eventsExpected; remaining > 0; --remaining) {
      // dequeue one event and remember its ack pointer
      DequeueResult result = executor.execute(context, dequeue);
      Assert.assertTrue(result.isSuccess());
      QueueEntryPointer ackPointer = result.getEntryPointer();
      // deserialize and verify the event
      Event event = deserializer.deserialize(result.getValue());
      TestUtil.verifyEvent(event, collectorName, destination, null);
      // message number should be in the header "messageNumber"
      LOG.info("Popped one event, message number: " +
          event.getHeader("messageNumber"));
      // ack the event so that it disappers from the queue
      QueueAck ack = new QueueAck(queueURI, ackPointer, consumer);
      List<WriteOperation> operations = new ArrayList<WriteOperation>(1);
      operations.add(ack);
      executor.execute(context, operations);
    }
  }

  /**
   * Consume the tuples in a queue and verify that the are events as created by
   * createHttpPost() or createFlumeEvent()
   *
   * @param executor       The executor to use for access to the data fabric
   * @param destination    The name of the flow (destination) that the events
   *                       were sent to
   * @param collectorName  The name of the collector that received the events
   * @param tuplesExpected How many tuples should be read
   * @throws Exception
   */
  static void consumeQueueAsTuples(OperationExecutor executor,
                                   String destination,
                                   String collectorName,
                                   int tuplesExpected) throws Exception {
    // address the correct queue
    byte[] queueURI = FlowStream.buildStreamURI(
        Constants.defaultAccount, destination).toString().getBytes();
    // one deserializer to reuse
    TupleSerializer deserializer = new TupleSerializer(false);
    // prepare the queue consumer
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueueConfig config =
        new QueueConfig(QueuePartitioner.PartitionerType.RANDOM, true);
    QueueDequeue dequeue = new QueueDequeue(queueURI, consumer, config);
    for (int remaining = tuplesExpected; remaining > 0; --remaining) {
      // dequeue one event and remember its ack pointer
      DequeueResult result = executor.execute(context, dequeue);
      Assert.assertTrue(result.isSuccess());
      QueueEntryPointer ackPointer = result.getEntryPointer();
      // deserialize and verify the event
      Tuple tuple = deserializer.deserialize(result.getValue());
      TestUtil.verifyTuple(tuple, collectorName, destination, null);
      // message number should be in the header "messageNumber"
      Map<String, String> headers = tuple.get("headers");
      LOG.info("Popped one event, message number: " +
          headers.get("messageNumber"));
      // ack the event so that it disappers from the queue
      QueueAck ack = new QueueAck(queueURI, ackPointer, consumer);
      List<WriteOperation> operations = new ArrayList<WriteOperation>(1);
      operations.add(ack);
      executor.execute(context, operations);
    }
  }

  /**
   * Verify that a given value can be retrieved for a given key via http GET
   * request
   *
   * @param executor the operation executor to use for access to data fabric
   * @param baseUri  The URI for get request, without the key
   * @param key      The key
   * @param value    The value
   * @throws Exception if an exception occurs
   */
  static void writeAndGet(OperationExecutor executor,
                          String baseUri, byte[] key, byte[] value)
      throws Exception {
    // add the key/value to the data fabric
    Write write = new Write(key, value);
    List<WriteOperation> operations = new ArrayList<WriteOperation>(1);
    operations.add(write);
    executor.execute(context, operations);

    // make a get URL
    String getUrl = baseUri +
        URLEncoder.encode(new String(key, "ISO8859_1"), "ISO8859_1");
    LOG.info("GET request URI for key '" + new String(key) + "' is " + getUrl);

    // and issue a GET request to the server
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(new HttpGet(getUrl));
    client.getConnectionManager().shutdown();

    // verify the response is ok
    Assert.assertEquals(HttpStatus.SC_OK,
        response.getStatusLine().getStatusCode());

    // verify the length of the return value is the same as the original value's
    int length = (int) response.getEntity().getContentLength();
    Assert.assertEquals(value.length, length);

    // verify that the value is actually the same
    InputStream content = response.getEntity().getContent();
    if (length > 0) {
      byte[] bytes = new byte[length];
      int bytesRead = content.read(bytes);
      Assert.assertEquals(length, bytesRead);
      Assert.assertArrayEquals(value, bytes);
    }
    // verify that the entire content was read
    Assert.assertEquals(-1, content.read(new byte[1]));
  }

  /**
   * Verify that a given value can be retrieved for a given key via http GET
   * request. This converts the key and value from String to bytes and calls
   * the byte-based method writeAndGet.
   *
   * @param executor the operation executor to use for access to data fabric
   * @param baseUri  The URI for get request, without the key
   * @param key      The key
   * @param value    The value
   * @throws Exception if an exception occurs
   */
  static void writeAndGet(OperationExecutor executor,
                          String baseUri, String key, String value)
      throws Exception {
    writeAndGet(executor, baseUri, key.getBytes("ISO8859_1"),
        value.getBytes("ISO8859_1"));
  }

  /**
   * Verify that a given value can be stored for a given key via http PUT
   * request
   *
   * @param executor the operation executor to use for access to data fabric
   * @param baseUri  The URI for PUT request, without the key
   * @param key      The key
   * @param value    The value
   * @throws Exception if an exception occurs
   */
  static void putAndRead(OperationExecutor executor,
                         String baseUri, byte[] key, byte[] value)
      throws Exception {

    // make a get URL
    String putUrl = baseUri +
        URLEncoder.encode(new String(key, "ISO8859_1"), "ISO8859_1");
    LOG.info("PUT request URI for key '" +
        new String(key, "ISO8859_1") + "' is " + putUrl);

    // and issue a PUT request to the server
    HttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(putUrl);
    put.setEntity(new ByteArrayEntity(value));
    HttpResponse response = client.execute(put);
    client.getConnectionManager().shutdown();

    // verify the response is ok
    Assert.assertEquals(HttpStatus.SC_OK,
        response.getStatusLine().getStatusCode());

    // read the key/value back from the data fabric
    ReadKey read = new ReadKey(key);
    OperationResult<byte[]> result = executor.execute(context, read);

    // verify the read value is the same as the original value
    Assert.assertFalse(result.isEmpty());
    Assert.assertArrayEquals(value, result.getValue());
  }

  /**
   * Verify that a given value can be stored for a given key via http PUT
   * request. This converts the key and value from String to bytes and calls
   * the byte-based method putAndRead.
   *
   * @param executor the operation executor to use for access to data fabric
   * @param baseUri  The URI for REST request, without the key
   * @param key      The key
   * @param value    The value
   * @throws Exception if an exception occurs
   */
  static void putAndRead(OperationExecutor executor,
                         String baseUri, String key, String value)
      throws Exception {
    putAndRead(executor, baseUri, key.getBytes("ISO8859_1"),
        value.getBytes("ISO8859_1"));
  }

  /**
   * Send a GET request to the given URL and return the HTTP status
   *
   * @param url the URL to get
   */
  public static int sendGetRequest(String url) throws Exception {
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(new HttpGet(url));
    client.getConnectionManager().shutdown();
    return response.getStatusLine().getStatusCode();
  }

  /**
   * Send a GET request to the given URL and return the HTTP status
   *
   * @param baseUrl the baseURL
   * @param key     the key to delete
   */
  public static int sendGetRequest(String baseUrl, String key)
      throws Exception {
    String urlKey = URLEncoder.encode(key, "ISO8859_1");
    return sendGetRequest(baseUrl + urlKey);
  }

  /**
   * Send a DELETE request to the given URL and return the HTTP status
   *
   * @param url the URL to delete
   */
  public static int sendDeleteRequest(String url) throws Exception {
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(new HttpDelete(url));
    client.getConnectionManager().shutdown();
    return response.getStatusLine().getStatusCode();
  }

  /**
   * Send a DELETE request to the given URL for the given key and return the
   * HTTP status
   *
   * @param baseUrl the baseURL
   * @param key     the key to delete
   */
  public static int sendDeleteRequest(String baseUrl, String key)
      throws Exception {
    String urlKey = URLEncoder.encode(key, "ISO8859_1");
    String url = baseUrl + urlKey;
    return sendDeleteRequest(url);
  }

  /**
   * Send a POST request to the given URL and return the HTTP status
   *
   * @param url the URL to post to
   */
  public static int sendPostRequest(String url) throws Exception {
    HttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost(url);
    post.setEntity(new ByteArrayEntity(new byte[0]));
    HttpResponse response = client.execute(post);
    client.getConnectionManager().shutdown();
    return response.getStatusLine().getStatusCode();
  }

  /**
   * Send a POST request to the given URL and return the HTTP status
   *
   * @param url the URL to post to
   */
  public static int sendPutRequest(String url) throws Exception {
    HttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(url);
    put.setEntity(new ByteArrayEntity(new byte[0]));
    HttpResponse response = client.execute(put);
    client.getConnectionManager().shutdown();
    return response.getStatusLine().getStatusCode();
  }
}
