package com.continuuity.gateway;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.Operation;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.data.operation.ttqueue.admin.GetGroupID;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigure;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.accessor.DataRestAccessor;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.gateway.collector.RestCollector;
import com.continuuity.gateway.consumer.StreamEventWritingConsumer;
import com.continuuity.streamevent.StreamEventCodec;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Tests Rest Accessor.
 */
public class RestAccessorTest {

  static final OperationContext CONTEXT = TestUtil.DEFAULT_CONTEXT;

  /**
   * this is the executor for all access to the data fabric.
   */
  private OperationExecutor executor;

  /**
   * the rest accessor we will use in the tests.
   */
  private DataRestAccessor accessor;

  /**
   * the rest collector we will use in the clear test.
   */
  private RestCollector collector;

  /**
   * Set up in-memory data fabric.
   */
  @Before
  public void setup() throws OperationException {

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
      new DataFabricModules().getInMemoryModules());
    this.executor = injector.getInstance(OperationExecutor.class);
    this.executor.execute(TestUtil.CONTEXT, new ClearFabric(ClearFabric.ToClear.ALL));

  } // end of setupGateway

  /**
   * Create a new rest accessor with a given name and parameters.
   *
   * @param name   The name for the accessor
   * @param prefix The path prefix for the URI
   * @param middle The path middle for the URI
   * @return the accessor's base URL for REST requests
   */
  String setupAccessor(String name, String prefix, String middle)
      throws Exception {
    // bring up a new accessor
    DataRestAccessor restAccessor = new DataRestAccessor();
    restAccessor.setName(name);
    restAccessor.setAuthenticator(new NoAuthenticator());
    // find a free port
    int port = PortDetector.findFreePort();
    // configure it
    CConfiguration configuration = new CConfiguration();
    configuration.setInt(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PORT), port);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_MIDDLE), middle);
    restAccessor.configure(configuration);
    restAccessor.setExecutor(this.executor);
    // start the accessor
    restAccessor.start();
    // all fine
    this.accessor = restAccessor;
    return "http://localhost:" + port + prefix + middle;
  }

  // we will need this to test the clear API.
  String setupCollector(String name, String prefix, String middle)
      throws Exception {
    // bring up a new collector
    RestCollector restCollector = new RestCollector();
    restCollector.setName(name);
    restCollector.setAuthenticator(new NoAuthenticator());
    // find a free port
    int port = PortDetector.findFreePort();
    // configure it
    CConfiguration configuration = new CConfiguration();
    configuration.setInt(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PORT), port);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_MIDDLE), middle);
    restCollector.configure(configuration);

    StreamEventWritingConsumer consumer = new StreamEventWritingConsumer();
    consumer.setExecutor(this.executor);
    restCollector.setConsumer(consumer);
    restCollector.setExecutor(this.executor);
    restCollector.setMetadataService(new DummyMDS());
    // start the accessor
    restCollector.start();
    // all fine
    this.collector = restCollector;
    return "http://localhost:" + port + prefix + middle;
  }

  static String[][] keyValues = {
      {"x", "y"},
      {"mt", ""},
      {"blank in the key", "some string"},
      {"cat's name?", "pfunk!"},
      {"special-/?@:%+key", "moseby"},
      {"nønäscîi", "value\u0000with\u0000nulls"},
      {"key\u0000with\u0000nulls", "foo"}
  };

  /**
   * Starts up a REST accessor, then tests retrieval of several combinations
   * of keys and values.
   * <ul>
   * <li>of ASCII letters only</li>
   * <li>of special characters</li>
   * <li>containing non-ASCII characters</li>
   * <li>empty key or value</li>
   * <li>containing null bytes</li>
   * </ul>
   *
   * @throws Exception if any exception occurs
   */
  @Test
  public void testGetAccess() throws Exception {
    // configure an accessor
    String uri = setupAccessor("restor", "/v0.1", "/table/");

    // write value via executor, then retrieve it back via REST
    for (String[] keyValue : keyValues) {
      TestUtil.writeAndGet(this.executor, uri, keyValue[0], keyValue[1]);
      TestUtil.writeAndGet(this.executor, uri, "tt", keyValue[0], keyValue[1]);
    }

    // shut it down
    this.accessor.stop();
  }

  /**
   * Starts up a REST accessor, then tests storing of several combinations
   * of keys and values.
   * <ul>
   * <li>of ASCII letters only</li>
   * <li>of special characters</li>
   * <li>containing non-ASCII characters</li>
   * <li>empty key or value</li>
   * <li>containing null bytes</li>
   * </ul>
   *
   * @throws Exception if any exception occurs
   */
  @Test
  public void testPutAccess() throws Exception {
    // configure an accessor
    String uri = setupAccessor("restor", "/v0.1", "/table/");

    // write value via REST, then retrieve it back via executor
    for (String[] keyValue : keyValues) {
      TestUtil.putAndRead(this.executor, uri, keyValue[0], keyValue[1]);
      TestUtil.putAndRead(this.executor, uri, "tt", keyValue[0], keyValue[1]);
    }

    // shut it down
    this.accessor.stop();
  }

  /**
   * Verifies that a PUT to an existing key updates the value.
   */
  @Test
  public void testRepeatedPut() throws Exception {
    // configure an accessor
    String uri = setupAccessor("rest", "/v0.1", "/table/");

    // write value via REST, then retrieve it back via executor
    TestUtil.putAndRead(this.executor, uri, "ki", "velu");
    TestUtil.putAndRead(this.executor, uri, "nn", "ki", "velu");
    // write new value for the same key via REST, then retrieve it
    // back via executor
    TestUtil.putAndRead(this.executor, uri, "ki", "nuvelu");
    TestUtil.putAndRead(this.executor, uri, "nn", "ki", "nuvelu");

    // shut it down
    this.accessor.stop();
  }

  /**
   * This tests that deletes work: A key that exists can be deleted,
   * and another attempt to delete the same key fails with 404 Not Found.
   *
   * @throws Exception if anything goes wrong
   */
  @Test
  public void testDelete() throws Exception {
    // configure an accessor
    String uri = setupAccessor("access.rest", "/", "table/");
    String table = "some-table";

    // write a value and verify it can be read
    String key = "to be deleted";
    TestUtil.writeAndGet(this.executor, uri, key, "foo");
    TestUtil.writeAndGet(this.executor, uri, table, key, "foo");

    // now delete it
    Assert.assertEquals(200, TestUtil.sendDeleteRequest(uri, key));
    Assert.assertEquals(200, TestUtil.sendDeleteRequest(uri, table, key));
    // verify that it's gone
    Assert.assertEquals(404, TestUtil.sendGetRequest(uri, key));
    Assert.assertEquals(404, TestUtil.sendGetRequest(uri, table, key));
    // and verify that a repeated delete fails
    Assert.assertEquals(404, TestUtil.sendDeleteRequest(uri, key));
    Assert.assertEquals(404, TestUtil.sendDeleteRequest(uri, table, key));

    this.accessor.stop();
  }

  /**
   * Verifies the list request is working. It first inserts a few key/values
   * and verifies that they can be read back. Then it lists all keys and
   * verifies they are all there, exactly once.
   * @param table    the name of the table to test on
   */
  public void testList(String table) throws Exception {
    // configure an accessor
    String uri = setupAccessor("access.rest", "/", "table/");

    // write some values and verify they can be read
    TestUtil.writeAndGet(this.executor, uri, table, "a", "bar");
    // a should only show once in the list!
    TestUtil.writeAndGet(this.executor, uri, table, "a", "foo");
    TestUtil.writeAndGet(this.executor, uri, table, "b", "foo");
    TestUtil.writeAndGet(this.executor, uri, table, "c", "foo");

    // now send a list request
    String requestUri = uri + (table == null ? "default" : table) + "?q=list";
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(new HttpGet(requestUri));
    client.getConnectionManager().shutdown();

    // verify the response is ok
    Assert.assertEquals(HttpStatus.SC_OK,
        response.getStatusLine().getStatusCode());

    // verify the length of the return value is greater than 0
    int length = (int) response.getEntity().getContentLength();
    Assert.assertTrue(length > 0);

    // make sure to read all the content from the response
    InputStream content = response.getEntity().getContent();
    byte[] bytes = new byte[length];
    int bytesRead = content.read(bytes);
    Assert.assertEquals(length, bytesRead);
    Assert.assertEquals(-1, content.read(new byte[1]));

    // convert the content into a string, then split it into lines
    String all = new String(bytes, "ASCII");
    List<String> keys = Arrays.asList(all.split("\n"));

    // and verify that all three keys are listed and nothing else
    Assert.assertEquals(3, keys.size());
    Assert.assertTrue(keys.contains("a"));
    Assert.assertTrue(keys.contains("b"));
    Assert.assertTrue(keys.contains("c"));
  }

  @Test
  public void testList() throws Exception {
    testList(null);
    testList("foo");
  }

  // TODO test list before write
  // TODO test list with encoding
  // TODO test list with start/limit

  /**
   * This tests that the accessor returns the correct HTTP codes for
   * invalid requests.
   *
   * @throws Exception if anything goes wrong
   */
  @Test
  public void testBadRequests() throws Exception {
    // configure an accessor
    String prefix = "/continuuity", middle = "/table/";
    String baseUrl = setupAccessor("access.rest", prefix, middle);
    int port = this.accessor.getHttpConfig().getPort();

    // test one valid key
    TestUtil.writeAndGet(this.executor, baseUrl, "x", "y");
    baseUrl += "default/";

    // test that ping works
    Assert.assertEquals(200, TestUtil.sendGetRequest(
        "http://localhost:" + port + "/ping"));

    // submit a request without prefix in the path -> 404 Not Found
    Assert.assertEquals(404, TestUtil.sendGetRequest(
        "http://localhost:" + port + "/somewhere"));
    Assert.assertEquals(404, TestUtil.sendGetRequest(
        "http://localhost:" + port + prefix + "/data"));
    // submit a request with correct prefix but no table -> 404 Not Found
    Assert.assertEquals(400, TestUtil.sendGetRequest(
        "http://localhost:" + port + prefix + middle + "x"));
    // a request with correct prefix but non-existent table -> 404 Not Found
    Assert.assertEquals(404, TestUtil.sendGetRequest(
        "http://localhost:" + port + prefix + middle + "other/x"));
    // submit a GET without key -> 404 Not Found
    Assert.assertEquals(400, TestUtil.sendGetRequest(baseUrl));
    // submit a GET with existing key -> 200 OK
    Assert.assertEquals(200, TestUtil.sendGetRequest(baseUrl + "x"));
    // submit a GET with non-existing key -> 404 Not Found
    Assert.assertEquals(404, TestUtil.sendGetRequest(
        baseUrl + "does.not.exist"));
    // GET with existing key but more after that in the path -> 404 Not Found
    Assert.assertEquals(400, TestUtil.sendGetRequest(baseUrl + "x/y/z"));
    // submit a GET with existing key but with query part -> 400 Bad Request
    Assert.assertEquals(400, TestUtil.sendGetRequest(baseUrl + "x?query=none"));

    // test some bad delete requests
    // submit a request without the correct prefix in the path -> 404 Not Found
    Assert.assertEquals(404, TestUtil.sendDeleteRequest(
        "http://localhost:" + port));
    Assert.assertEquals(404, TestUtil.sendDeleteRequest(
        "http://localhost:" + port + "/"));
    // no table
    Assert.assertEquals(404, TestUtil.sendDeleteRequest(
        "http://localhost:" + port + prefix + "/table"));
    Assert.assertEquals(404, TestUtil.sendDeleteRequest(
        "http://localhost:" + port + prefix + middle));
    // table without key
    Assert.assertEquals(400, TestUtil.sendDeleteRequest(
        "http://localhost:" + port + prefix + middle + "default"));
    Assert.assertEquals(400, TestUtil.sendDeleteRequest(
        "http://localhost:" + port + prefix + middle + "sometable"));
    // unknown table
    Assert.assertEquals(404, TestUtil.sendDeleteRequest(
        "http://localhost:" + port + prefix + middle + "sometable/x"));
    Assert.assertEquals(404, TestUtil.sendDeleteRequest(
        "http://localhost:" + port + prefix + middle + "sometable/pfunk"));
    // no key
    Assert.assertEquals(400, TestUtil.sendDeleteRequest(baseUrl));
    // non-existent key
    Assert.assertEquals(404, TestUtil.sendDeleteRequest(baseUrl + "no-exist"));
    // correct key but more in the path
    Assert.assertEquals(400, TestUtil.sendDeleteRequest(baseUrl + "x/a"));
    // correct key but unsupported query -> 501 Not Implemented
    Assert.assertEquals(501, TestUtil.sendDeleteRequest(
        baseUrl + "x?force=true"));

    // test some bad put requests
    // submit a request without the correct prefix in the path -> 404 Not Found
    Assert.assertEquals(404, TestUtil.sendPutRequest(
        "http://localhost:" + port));
    Assert.assertEquals(404, TestUtil.sendPutRequest(
        "http://localhost:" + port + "/"));
    // no table
    Assert.assertEquals(404, TestUtil.sendPutRequest(
        "http://localhost:" + port + prefix + "/table"));
    Assert.assertEquals(404, TestUtil.sendPutRequest(
        "http://localhost:" + port + prefix + middle));
    // table without key
    Assert.assertEquals(400, TestUtil.sendPutRequest(
        "http://localhost:" + port + prefix + middle + "default"));
    Assert.assertEquals(400, TestUtil.sendPutRequest(
        "http://localhost:" + port + prefix + middle + "sometable"));
    // non-default table - this now works! (after ENG-732)
    Assert.assertEquals(200, TestUtil.sendPutRequest(
        "http://localhost:" + port + prefix + middle + "sometable/x"));
    Assert.assertEquals(200, TestUtil.sendPutRequest(
        "http://localhost:" + port + prefix + middle + "sometable/pfunk"));
    // no key
    Assert.assertEquals(400, TestUtil.sendPutRequest(baseUrl));
    // correct key but more in the path
    Assert.assertEquals(400, TestUtil.sendPutRequest(baseUrl + "x/"));
    Assert.assertEquals(400, TestUtil.sendPutRequest(baseUrl + "x/a"));
    // correct key but unsupported query -> 501 Not Implemented
    Assert.assertEquals(501, TestUtil.sendPutRequest(baseUrl + "x?force=true"));

    // and shutdown
    this.accessor.stop();
  }

  /**
   * This tests that the accessor can be stopped and restarted.
   */
  @Test
  public void testStopRestart() throws Exception {
    // configure an accessor
    String baseUrl = setupAccessor("access.rest", "/continuuity", "/table/");
    // test one valid key
    TestUtil.writeAndGet(this.executor, baseUrl, "x", "y");
    // submit a GET with existing key -> 200 OK
    Assert.assertEquals(200, TestUtil.sendGetRequest(baseUrl + "default/x"));
    // stop the connector
    this.accessor.stop();
    // verify that GET fails now. Should throw an exception
    try {
      TestUtil.sendGetRequest(baseUrl + "x");
      Assert.fail("Expected HttpHostConnectException because connector was " +
          "stopped.");
    } catch (HttpHostConnectException e) {
      // this is expected
    }
    // restart the connector
    this.accessor.start();
    // submit a GET with existing key -> 200 OK
    Assert.assertEquals(200, TestUtil.sendGetRequest(baseUrl + "default/x"));
    // and finally shut down
    this.accessor.stop();
  }

  @Test
  public void testClearData() throws Exception {
    // setup accessor
    String baseUrl = setupAccessor("access.rest", "/continuuity", "/data/");
    String clearUrl = this.accessor.getHttpConfig().getBaseUrl() + "?clear=";
    // setup collector
    String collectorUrl =
        setupCollector("collect.rest", "/continuuity", "/stream/");

    // write and verify some data
    TestUtil.writeAndGet(this.executor, baseUrl, "key", "value");
    // write and verify some stream
    sendAndVerify(collectorUrl, "foo", 1);
    // write and verify some queue
    queueAndVerify("queue://foo/bar", 2);

    // clear all
    Assert.assertEquals(200, TestUtil.sendPostRequest(clearUrl + "all"));
    // verify all are gone
    verifyKeyGone("key");
    verifyQueueGone("queue://foo/bar");
    verifyStreamGone("foo");

    // write and verify some data
    TestUtil.writeAndGet(this.executor, baseUrl, "key", "value");
    // write and verify some stream
    sendAndVerify(collectorUrl, "foo", 1);
    // write and verify some queue
    queueAndVerify("queue://foo/bar", 2);

    // clear all
    Assert.assertEquals(200, TestUtil.
        sendPostRequest(clearUrl + "queues,streams,data"));
    // verify all are gone
    verifyKeyGone("key");
    verifyQueueGone("queue://foo/bar");
    verifyStreamGone("foo");

    // write and verify some data
    TestUtil.writeAndGet(this.executor, baseUrl, "key", "value");
    // write and verify some stream
    sendAndVerify(collectorUrl, "foo", 1);
    // write and verify some queue
    queueAndVerify("queue://foo/bar", 2);

    // clear data
    Assert.assertEquals(200, TestUtil.sendPostRequest(clearUrl + "data"));
    // verify data is gone, rest is still there
    verifyKeyGone("key");
    verifyEvent("foo", 1);
    verifyInteger("queue://foo/bar", 2);

    // write and verify some data
    TestUtil.writeAndGet(this.executor, baseUrl, "key", "value");

    // clear streams
    Assert.assertEquals(200, TestUtil.sendPostRequest(clearUrl + "streams"));
    // verify streams are gone, rest is still there
    // verify data is gone, rest is still there
    verifyStreamGone("foo");
    verifyKeyValue("key", "value");
    verifyInteger("queue://foo/bar", 2);

    // write and verify some stream
    sendAndVerify(collectorUrl, "foo", 1);

    // clear queues
    Assert.assertEquals(200, TestUtil.sendPostRequest(clearUrl + "queues"));
    // verify queues are gone, rest is still there
    verifyQueueGone("queue://foo/bar");
    verifyKeyValue("key", "value");
    verifyEvent("foo", 1);

    // write and verify some queue
    queueAndVerify("queue://foo/bar", 2);
  }

  void sendEvent(String baseUrl, String stream, int n) throws Exception {
    HttpPost post = new HttpPost(baseUrl + stream);
    post.addHeader(stream + ".number", Integer.toString(n));
    post.setEntity(new ByteArrayEntity(
        ("This is event number " + n).getBytes()));
    TestUtil.sendRestEvent(post);
  }

  void verifyEvent(String stream, int n) throws Exception {

    String streamUri = QueueName.fromStream(TestUtil.DEFAULT_ACCOUNT_ID, stream)
                                .toString();
    GetGroupID op = new GetGroupID(streamUri.getBytes());
    long id = this.executor.execute(CONTEXT, op);
    QueueConfig queueConfig = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true);
    QueueConsumer queueConsumer = new QueueConsumer(0, id, 1, queueConfig);
    this.executor.execute(CONTEXT, new QueueConfigure(streamUri.getBytes(), queueConsumer));
    // singleEntry = true means we must ack before we can see the next entry
    QueueDequeue dequeue = new QueueDequeue(streamUri.getBytes(), queueConsumer, queueConfig);
    DequeueResult result = this.executor.execute(CONTEXT, dequeue);
    Assert.assertFalse(result.isEmpty());
    // try to deserialize into an event
    StreamEventCodec deserializer = new StreamEventCodec();
    StreamEvent event = deserializer.decodePayload(result.getEntry().getData());
    Map<String, String> headers = event.getHeaders();
    byte[] body = Bytes.toBytes(event.getBody());
    Assert.assertEquals(Integer.toString(n), headers.get("number"));
    Assert.assertEquals(new String(body), "This is event number " + n);
    // ack the entry so that the next request can see the next entry
    QueueAck ack = new
      QueueAck(streamUri.getBytes(), result.getEntryPointer(), queueConsumer);
    this.collector.getExecutor().commit(CONTEXT, ack);
  }

  void verifyInteger(String queueUri, int n) throws Exception {
    GetGroupID op = new GetGroupID(queueUri.getBytes());
    long id = this.executor.execute(CONTEXT, op);
    QueueConfig queueConfig = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true);
    QueueConsumer queueConsumer = new QueueConsumer(0, id, 1, queueConfig);
    executor.execute(CONTEXT, new QueueConfigure(queueUri.getBytes(), queueConsumer));
    // singleEntry = true means we must ack before we can see the next entry
    QueueDequeue dequeue = new QueueDequeue(queueUri.getBytes(), queueConsumer, queueConfig);
    DequeueResult result = this.executor.execute(CONTEXT, dequeue);
    Assert.assertFalse(result.isEmpty());
    // try to deserialize into an integer
    int num = Bytes.toInt(result.getEntry().getData());
    Assert.assertEquals(n, num);
    // ack the entry so that the next request can see the next entry
    QueueAck ack = new
      QueueAck(queueUri.getBytes(), result.getEntryPointer(), queueConsumer);
    this.collector.getExecutor().commit(CONTEXT, ack);
  }

  void sendAndVerify(String baseUrl, String stream, int n) throws Exception {
    sendEvent(baseUrl, stream, n);
    verifyEvent(stream, n);
  }

  void queueAndVerify(String queueUri, int n) throws Exception {
    sendInteger(queueUri, n);
    verifyInteger(queueUri, n);
  }

  void sendInteger(String queueUri, int n) throws OperationException {
    byte[] bytes = Bytes.toBytes(n);
    QueueEnqueue enqueue = new QueueEnqueue(queueUri.getBytes(), new QueueEntry(bytes));
    this.executor.commit(CONTEXT, enqueue);
  }

  void verifyKeyGone(String key) throws Exception {
    Read read = new Read(key.getBytes(), Operation.KV_COL);
    Assert.assertTrue(this.executor.execute(CONTEXT, read).isEmpty());
  }

  void verifyKeyValue(String key, String value) throws Exception {
    Read read = new Read(key.getBytes(), Operation.KV_COL);
    OperationResult<Map<byte[], byte[]>> result = this.executor.execute(CONTEXT, read);
    Assert.assertFalse(result.isEmpty());
    Assert.assertArrayEquals(value.getBytes(), result.getValue().get(Operation.KV_COL));
  }

  void verifyQueueGone(String queueUri) throws Exception {
    GetGroupID op = new GetGroupID(queueUri.getBytes());
    long id = this.executor.execute(CONTEXT, op);
    QueueConfig queueConfig = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true);
    QueueConsumer queueConsumer = new QueueConsumer(0, id, 1, queueConfig);
    executor.execute(CONTEXT, new QueueConfigure(queueUri.getBytes(), queueConsumer));
    // singleEntry = true means we must ack before we can see the next entry
    QueueDequeue dequeue = new QueueDequeue(queueUri.getBytes(), queueConsumer, queueConfig);
    DequeueResult result = this.executor.execute(CONTEXT, dequeue);
    Assert.assertTrue(result.isEmpty());
  }

  void verifyStreamGone(String stream) throws Exception {
    String streamUri = QueueName.fromStream(TestUtil.DEFAULT_ACCOUNT_ID, stream)
      .toString();
    verifyQueueGone(streamUri);
  }
}

