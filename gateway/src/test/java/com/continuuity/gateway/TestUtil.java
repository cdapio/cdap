package com.continuuity.gateway;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.gateway.auth.GatewayAuthenticator;
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
   * Send a doGet request to the given URL and return the HTTP status.
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
   * Send a doGet request to the given URL and return the HTTP status.
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
   * Send a doGet request to the given URL and return the HTTP status. Use
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
   * Send a doGet request to the given URL and return the Http response.
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
