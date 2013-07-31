package com.continuuity.gateway;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.Operation;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEntryPointer;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigure;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.streamevent.StreamEventCodec;
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
import org.apache.http.entity.StringEntity;
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

/**
 *
 */
public class TestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(TestUtil.class);

  private static String apiKey = null;

  public static final String DEFAULT_ACCOUNT_ID = com.continuuity.common.conf.Constants.DEVELOPER_ACCOUNT_ID;

  /** defaults to be used everywhere where we don't have authenticated accounts. */
  public static final OperationContext DEFAULT_CONTEXT = new OperationContext(DEFAULT_ACCOUNT_ID);

  static final OperationContext CONTEXT = TestUtil.DEFAULT_CONTEXT;

  static void enableAuth(String apiKey) {
    TestUtil.apiKey = apiKey;
  }

  static void disableAuth() {
    TestUtil.apiKey = null;
  }

  /**
   * Creates a string containing a number.
   *
   * @param i The number to use
   * @return a message as a byte a array
   */
  static byte[] createMessage(int i) {
    return ("This is message " + i + ".").getBytes();
  }

  /**
   * Creates a flume event that has.
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
    if (TestUtil.apiKey != null) {
      headers.put(GatewayAuthenticator.CONTINUUITY_API_KEY,
          TestUtil.apiKey);
      LOG.warn("Set the API key on the flume event: " + apiKey);
    }
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
   * Uses Flume's RPC client to send a single event to a port.
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
   * Creates an HTTP post that has.
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
   * Send an event to rest collector.
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
   * Creates an HTTP post for an event and sends it to the rest collector.
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
   * createFlumeEvent or createHttpPost.
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
  static void verifyEvent(StreamEvent event, String collectorName,
                          String destination, Integer expectedNo) {
    Assert.assertNotNull(event.getHeaders().get("messageNumber"));
    int messageNumber = Integer.valueOf(event.getHeaders().get("messageNumber"));
    if (expectedNo != null) {
      Assert.assertEquals(messageNumber, expectedNo.intValue());
    }
    if (collectorName != null) {
      Assert.assertEquals(collectorName,
          event.getHeaders().get(Constants.HEADER_FROM_COLLECTOR));
    }
    if (destination != null) {
      Assert.assertEquals(destination,
          event.getHeaders().get(Constants.HEADER_DESTINATION_STREAM));
    }
    Assert.assertArrayEquals(createMessage(messageNumber), Bytes.toBytes(event.getBody()));
  }

  /**
   * A consumer that does nothing.
   */
  static class NoopConsumer extends Consumer {
    @Override
    public void single(StreamEvent event, String accountId) {
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
    protected void single(StreamEvent event, String accountId) throws Exception {
      TestUtil.verifyEvent(event, this.collectorName,
          this.destination, this.expectedNumber);
    }
  }

  /**
   * Consume the events in a queue and verify that they correspond to the
   * format as created by createHttpPost() or createFlumeEvent().
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
    byte[] queueURI = QueueName.fromStream(DEFAULT_ACCOUNT_ID, destination)
                               .toString().getBytes();
    // one deserializer to reuse
    StreamEventCodec deserializer = new StreamEventCodec();
    // prepare the queue consumer
    QueueConfig config = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);
    executor.execute(CONTEXT, new QueueConfigure(queueURI, consumer));
    QueueDequeue dequeue = new QueueDequeue(queueURI, consumer, config);
    for (int remaining = eventsExpected; remaining > 0; --remaining) {
      // dequeue one event and remember its ack pointer
      DequeueResult result = executor.execute(CONTEXT, dequeue);
      Assert.assertTrue(result.isSuccess());
      QueueEntryPointer ackPointer = result.getEntryPointer();
      // deserialize and verify the event
      StreamEvent event = deserializer.decodePayload(result.getEntry().getData());
      TestUtil.verifyEvent(event, collectorName, destination, null);
      // message number should be in the header "messageNumber"
      LOG.info("Popped one event, message number: " +
          event.getHeaders().get("messageNumber"));
      // ack the event so that it disappers from the queue
      QueueAck ack = new QueueAck(queueURI, ackPointer, consumer);
      List<WriteOperation> operations = new ArrayList<WriteOperation>(1);
      operations.add(ack);
      executor.commit(CONTEXT, operations);
    }
  }

  /**
   * Verify that a given value can be retrieved for a given key via http GET
   * request.
   *
   * @param executor the operation executor to use for access to data fabric
   * @param table    the name of the table to test on
   * @param baseUri  The URI for get request, without the key
   * @param key      The key
   * @param value    The value
   * @throws Exception if an exception occurs
   */
  static void writeAndGet(OperationExecutor executor, String baseUri,
                          String table, byte[] key, byte[] value)
  throws Exception {
    // add the key/value to the data fabric
    Write write = new Write(table, key, Operation.KV_COL, value);
    List<WriteOperation> operations = new ArrayList<WriteOperation>(1);
    operations.add(write);
    executor.commit(CONTEXT, operations);

    // make a get URL
    String getUrl = baseUri + (table == null ? "default" : table) + "/" +
        URLEncoder.encode(new String(key, "ISO8859_1"), "ISO8859_1");
    LOG.info("GET request URI for key '" + new String(key) + "' is " + getUrl);

    // and issue a GET request to the server
    HttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet(getUrl);
    if (TestUtil.apiKey != null) {
      get.setHeader(GatewayAuthenticator.CONTINUUITY_API_KEY,
          TestUtil.apiKey);
    }
    HttpResponse response = client.execute(get);
    client.getConnectionManager().shutdown();

    // verify the response is ok, throw exception if 403
    if (HttpStatus.SC_FORBIDDEN == response.getStatusLine().getStatusCode()) {
      throw new SecurityException("Authentication failed, access denied");
    }
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
   * @param table    the name of the table to test on
   * @param baseUri  The URI for get request, without the key
   * @param key      The key
   * @param value    The value
   * @throws Exception if an exception occurs
   */
  static void writeAndGet(OperationExecutor executor, String baseUri,
                          String table, String key, String value)
      throws Exception {
    writeAndGet(executor, baseUri, table, key.getBytes("ISO8859_1"),
        value.getBytes("ISO8859_1"));
  }

  /**
   * Verify that a given value can be retrieved for a given key via http GET
   * request. This converts the key and value from String to bytes and calls
   * the byte-based method writeAndGet. Uses default table
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
    writeAndGet(executor, baseUri, null, key, value);
  }

  /**
   * Verify that a given value can be stored for a given key via http PUT
   * request.
   *
   * @param executor the operation executor to use for access to data fabric
   * @param baseUri  The URI for PUT request, without the key
   * @param table    the name of the table to test on
   * @param key      The key
   * @param value    The value
   * @throws Exception if an exception occurs
   */
  static void putAndRead(OperationExecutor executor, String baseUri,
                         String table, byte[] key, byte[] value)
      throws Exception {

    // make a get URL
    String putUrl = baseUri + (table == null ? "default" : table) + "/" +
        URLEncoder.encode(new String(key, "ISO8859_1"), "ISO8859_1");
    LOG.info("PUT request URI for key '" +
        new String(key, "ISO8859_1") + "' is " + putUrl);

    // and issue a PUT request to the server
    HttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(putUrl);
    if (TestUtil.apiKey != null) {
      put.setHeader(GatewayAuthenticator.CONTINUUITY_API_KEY,
          TestUtil.apiKey);
    }
    put.setEntity(new ByteArrayEntity(value));
    HttpResponse response = client.execute(put);
    client.getConnectionManager().shutdown();

    // verify the response is ok, throw exception if 403
    if (HttpStatus.SC_FORBIDDEN == response.getStatusLine().getStatusCode()) {
      throw new SecurityException("Authentication failed, access denied");
    }

    Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());

    // read the key/value back from the data fabric
    Read read = new Read(key, Operation.KV_COL);
    OperationResult<Map<byte[], byte[]>> result = executor.execute(CONTEXT, read);

    // verify the read value is the same as the original value
    Assert.assertFalse(result.isEmpty());
    Assert.assertArrayEquals(value, result.getValue().get(Operation.KV_COL));
  }

  /**
   * Verify that a given value can be stored for a given key via http PUT
   * request. This converts the key and value from String to bytes and calls
   * the byte-based method putAndRead.
   *
   * @param executor the operation executor to use for access to data fabric
   * @param baseUri  The URI for REST request, without the key
   * @param table    the name of the table to test on
   * @param key      The key
   * @param value    The value
   * @throws Exception if an exception occurs
   */
  static void putAndRead(OperationExecutor executor, String baseUri,
                         String table, String key, String value)
      throws Exception {
    putAndRead(executor, baseUri, table, key.getBytes("ISO8859_1"),
        value.getBytes("ISO8859_1"));
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
  static void putAndRead(OperationExecutor executor, String baseUri,
                         String key, String value)
      throws Exception {
    putAndRead(executor, baseUri, null, key, value);
  }

  /**
   * Send a GET request to the given URL and return the HTTP status.
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
   * Send a GET request to the given URL and return the HTTP status.
   *
   * @param baseUrl the baseURL
   * @param table    the name of the table to test on
   * @param key     the key to delete
   */
  public static int sendGetRequest(String baseUrl, String table, String key)
      throws Exception {
    String urlKey = URLEncoder.encode(key, "ISO8859_1");
    String url = baseUrl + (table == null ? "default" : table) + "/" + urlKey;
    return sendGetRequest(url);
  }

  /**
   * Send a GET request to the given URL and return the HTTP status. Use
   * default table.
   *
   * @param baseUrl the baseURL
   * @param key     the key to delete
   */
  public static int sendGetRequest(String baseUrl, String key)
      throws Exception {
    return sendGetRequest(baseUrl, null, key);
  }

  /**
   * Send a DELETE request to the given URL and return the HTTP status.
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
   * HTTP status.
   *
   * @param baseUrl the baseURL
   * @param table    the name of the table to test on
   * @param key     the key to delete
   */
  public static int sendDeleteRequest(String baseUrl, String table, String key)
      throws Exception {
    String urlKey = URLEncoder.encode(key, "ISO8859_1");
    String url = baseUrl + (table == null ? "default" : table) + "/" + urlKey;
    return sendDeleteRequest(url);
  }

  /**
   * Send a DELETE request to the given URL for the given key and return the
   * HTTP status. Uses default table
   *
   * @param baseUrl the baseURL
   * @param key     the key to delete
   */
  public static int sendDeleteRequest(String baseUrl, String key)
      throws Exception {
    return sendDeleteRequest(baseUrl, null, key);
  }

  /**
   * Send a GET request to the given URL and return the Http response.
   *
   * @param url the URL to get
   * @param headers map with header data
   */
  public static HttpResponse sendGetRequest(String url, Map<String, String> headers) throws Exception {
    HttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet(url);
    if (headers != null) {
      for (Map.Entry<String, String> header: headers.entrySet()) {
        get.setHeader(header.getKey(), header.getValue());
      }
    }
    HttpResponse response = client.execute(new HttpGet(url));
    client.getConnectionManager().shutdown();
    return response;
  }


  /**
   * Send a POST request to the given URL and return the HTTP status.
   *
   * @param url the URL to post to
   */
  public static int sendPostRequest(String url) throws Exception {
    return sendPostRequest(url, new HashMap<String, String>());
  }

  /**
   * Send a POST request to the given URL and return the HTTP status.
   *
   * @param url the URL to post to
   * @param headers map with header data
   */
  public static int sendPostRequest(String url, Map<String, String> headers) throws Exception {
    return sendPostRequest(url, new byte[0], headers);
  }

  /**
   * Send a Post request to the given URL and return the HTTP status.
   *
   * @param url the URL to post to
   * @param content binary content
   * @param headers map with header data
   */
  public static int sendPostRequest(String url, byte[] content, Map<String, String> headers) throws Exception {
    HttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost(url);
    if (headers != null) {
      for (Map.Entry<String, String> header: headers.entrySet()) {
        post.setHeader(header.getKey(), header.getValue());
      }
    }
    post.setEntity(new ByteArrayEntity(content));
    HttpResponse response = client.execute(post);
    client.getConnectionManager().shutdown();
    return response.getStatusLine().getStatusCode();
  }

  /**
   * Send a Post request to the given URL and return the HTTP status.
   *
   * @param url the URL to post to
   * @param content String content
   * @param headers map with header data
   */
  public static int sendPostRequest(String url, String content, Map<String, String> headers) throws Exception {
    HttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost(url);
    if (headers != null) {
      for (Map.Entry<String, String> header: headers.entrySet()) {
        post.setHeader(header.getKey(), header.getValue());
      }
    }
    post.setEntity(new StringEntity(content, "UTF-8"));
    HttpResponse response = client.execute(post);
    client.getConnectionManager().shutdown();
    return response.getStatusLine().getStatusCode();
  }

  /**
   * Send a PUT request to the given URL and return the HTTP status.
   *
   * @param url the URL to put to
   */
  public static int sendPutRequest(String url) throws Exception {
    return sendPutRequest(url, new HashMap<String, String>());
  }

  /**
   * Send a PUT request to the given URL and return the HTTP status.
   *
   * @param url the URL to put to
   * @param headers map with header data
   */
  public static int sendPutRequest(String url, Map<String, String> headers) throws Exception {
    return sendPutRequest(url, new byte[0], headers);
  }

  /**
   * Send a PUT request to the given URL and return the HTTP status.
   *
   * @param url the URL to put to
   * @param content binary content
   * @param headers map with header data
   */
  public static int sendPutRequest(String url, byte[] content, Map<String, String> headers) throws Exception {
    HttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(url);
    if (headers != null) {
      for (Map.Entry<String, String> header: headers.entrySet()) {
        put.setHeader(header.getKey(), header.getValue());
      }
    }
    put.setEntity(new ByteArrayEntity(content));
    HttpResponse response = client.execute(put);
    client.getConnectionManager().shutdown();
    return response.getStatusLine().getStatusCode();
  }

  /**
   * Send a PUT request to the given URL and return the HTTP status.
   *
   * @param url the URL to put to
   * @param content String content
   * @param headers map with header data
   */
  public static int sendPutRequest(String url, String content, Map<String, String> headers) throws Exception {
    HttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(url);
    if (headers != null) {
      for (Map.Entry<String, String> header: headers.entrySet()) {
        put.setHeader(header.getKey(), header.getValue());
      }
    }
    put.setEntity(new StringEntity(content, "UTF-8"));
    HttpResponse response = client.execute(put);
    client.getConnectionManager().shutdown();
    return response.getStatusLine().getStatusCode();
  }

}
