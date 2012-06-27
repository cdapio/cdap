package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.accessor.RestAccessor;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

public class RestAccessorTest {

  private static final Logger LOG = LoggerFactory.getLogger(RestAccessorTest.class);

  /**
   * this is the executor for all access to the data fabric
   */
  private OperationExecutor executor;

  /**
   * the rest accessor we will use in the tests
   */
  private RestAccessor accessor;

  /**
   * Set up in-memory data fabric
   */
  @Before
  public void setup() {

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
        new DataFabricModules().getInMemoryModules());
    this.executor = injector.getInstance(OperationExecutor.class);

  } // end of setupGateway

  /**
   * Create a new rest accessor with a given name and parameters
   *
   * @param name   The name for the accessor
   * @param prefix The path prefix for the URI
   * @param middle The path middle for the URI
   * @return the accessor's base URL for REST requests
   */
  String setupAccessor(String name, String prefix, String middle) throws Exception {
    // bring up a new accessor
    RestAccessor restAccessor = new RestAccessor();
    restAccessor.setName(name);
    // find a free port
    int port = Util.findFreePort();
    // configure it
    CConfiguration configuration = new CConfiguration();
    configuration.setInt(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
    configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_MIDDLE), middle);
    restAccessor.configure(configuration);
    restAccessor.setExecutor(this.executor);
    // start the accessor
    restAccessor.start();
    // all fine
    this.accessor = restAccessor;
    return "http://localhost:" + port + prefix + middle + "default/";
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
   * Starts up a REST accessor, then tests retrieval of several combinations of keys and values
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
      Util.writeAndGet(this.executor, uri, keyValue[0], keyValue[1]);
    }

    // shut it down
    this.accessor.stop();
  }

  /**
   * Starts up a REST accessor, then tests storing of several combinations of keys and values
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
      Util.putAndRead(this.executor, uri, keyValue[0], keyValue[1]);
    }

    // shut it down
    this.accessor.stop();
  }

  /**
   * Verifies that a PUT to an existing key updates the value
   */
  @Test
  public void testRepeatedPut() throws Exception {
    // configure an accessor
    String uri = setupAccessor("rest", "/v0.1", "/table/");

    // write value via REST, then retrieve it back via executor
    Util.putAndRead(this.executor, uri, "ki", "velu");
    // write new value for the same key via REST, then retrieve it back via executor
    Util.putAndRead(this.executor, uri, "ki", "nuvelu");

    // shut it down
    this.accessor.stop();
  }

  /**
   * This tests that deletes work: A key that exists can be deleted, and another attempt
   * to delete the same key fails with 404 Not Found.
   *
   * @throws Exception if anything goes wrong
   */
  @Test
  public void testDelete() throws Exception {
    // configure an accessor
    String uri = setupAccessor("access.rest", "/", "table/");
    int port = this.accessor.getHttpConfig().getPort();

    // write a value and verify it can be read
    String key = "to be deleted";
    Util.writeAndGet(this.executor, uri, key, "foo");

    // now delete it
    Assert.assertEquals(200, Util.sendDeleteRequest(uri, key));
    // verify that it's gone
    Assert.assertEquals(404, Util.sendGetRequest(uri, key));
    // and verify that a repeated delete fails
    Assert.assertEquals(404, Util.sendDeleteRequest(uri, key));

    this.accessor.stop();
  }

  /**
   * Verifies the list request is working. It first inserts a few key/values
   * and verifies that they can be read back. Then it lists all keyts and
   * verifies they are all there, exactly once.
   * @throws Exception
   */
  @Test
  public void testList() throws Exception {
    // configure an accessor
    String uri = setupAccessor("access.rest", "/", "table/");
    int port = this.accessor.getHttpConfig().getPort();

    // write some values and verify they can be read
    Util.writeAndGet(this.executor, uri, "a", "bar");
    Util.writeAndGet(this.executor, uri, "a", "foo"); // a should only show once in the list!
    Util.writeAndGet(this.executor, uri, "b", "foo");
    Util.writeAndGet(this.executor, uri, "c", "foo");

    // now send a list request
    String requestUri = uri + "?q=list";
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(new HttpGet(requestUri));
    client.getConnectionManager().shutdown();

    // verify the response is ok
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());

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

  // TODO test list before write
  // TODO test list with encoding
  // TODO test list with start/limit

  /**
   * This tests that the accessor returns the correct HTTP codes for invalid requests
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
    Util.writeAndGet(this.executor, baseUrl, "x", "y");

    // submit a request without prefix in the path -> 404 Not Found
    Assert.assertEquals(404, Util.sendGetRequest("http://localhost:" + port + "/somewhere"));
    Assert.assertEquals(404, Util.sendGetRequest("http://localhost:" + port + prefix + "/data"));
    // submit a request with correct prefix but no table -> 404 Not Found
    Assert.assertEquals(400, Util.sendGetRequest("http://localhost:" + port + prefix + middle + "x"));
    // submit a request with correct prefix but non-existent table -> 404 Not Found
    Assert.assertEquals(404, Util.sendGetRequest("http://localhost:" + port + prefix + middle + "other/x"));
    // submit a POST to the accessor (which only supports GET) -> 405 Not Allowed
    Assert.assertEquals(400, Util.sendPostRequest(baseUrl));
    // submit a GET without key -> 404 Not Found
    Assert.assertEquals(400, Util.sendGetRequest(baseUrl));
    // submit a GET with existing key -> 200 OK
    Assert.assertEquals(200, Util.sendGetRequest(baseUrl + "x"));
    // submit a GET with non-existing key -> 404 Not Found
    Assert.assertEquals(404, Util.sendGetRequest(baseUrl + "does.not.exist"));
    // submit a GET with existing key but more after that in the path -> 404 Not Found
    Assert.assertEquals(400, Util.sendGetRequest(baseUrl + "x/y/z"));
    // submit a GET with existing key but with query part -> 400 Bad Request
    Assert.assertEquals(400, Util.sendGetRequest(baseUrl + "x?query=none"));

    // test some bad delete requests
    // submit a request without the correct prefix in the path -> 404 Not Found
    Assert.assertEquals(404, Util.sendDeleteRequest("http://localhost:" + port));
    Assert.assertEquals(404, Util.sendDeleteRequest("http://localhost:" + port + "/"));
    // no table
    Assert.assertEquals(404, Util.sendDeleteRequest("http://localhost:" + port + prefix + "/table"));
    Assert.assertEquals(400, Util.sendDeleteRequest("http://localhost:" + port + prefix + middle));
    // table without key
    Assert.assertEquals(400, Util.sendDeleteRequest("http://localhost:" + port + prefix + middle + "default"));
    Assert.assertEquals(400, Util.sendDeleteRequest("http://localhost:" + port + prefix + middle + "sometable"));
    // unknown table
    Assert.assertEquals(404, Util.sendDeleteRequest("http://localhost:" + port + prefix + middle + "sometable/x"));
    Assert.assertEquals(404, Util.sendDeleteRequest("http://localhost:" + port + prefix + middle + "sometable/pfunk"));
    // no key
    Assert.assertEquals(400, Util.sendDeleteRequest(baseUrl));
    // non-existent key
    Assert.assertEquals(404, Util.sendDeleteRequest(baseUrl + "no-exist"));
    // correct key but more in the path
    Assert.assertEquals(400, Util.sendDeleteRequest(baseUrl + "x/a"));
    // correct key but unsupported query -> 501 Not Implemented
    Assert.assertEquals(501, Util.sendDeleteRequest(baseUrl + "x?force=true"));

    // test some bad put requests
    // submit a request without the correct prefix in the path -> 404 Not Found
    Assert.assertEquals(404, Util.sendPutRequest("http://localhost:" + port));
    Assert.assertEquals(404, Util.sendPutRequest("http://localhost:" + port + "/"));
    // no table
    Assert.assertEquals(404, Util.sendPutRequest("http://localhost:" + port + prefix + "/table"));
    Assert.assertEquals(400, Util.sendPutRequest("http://localhost:" + port + prefix + middle));
    // table without key
    Assert.assertEquals(400, Util.sendPutRequest("http://localhost:" + port + prefix + middle + "default"));
    Assert.assertEquals(400, Util.sendPutRequest("http://localhost:" + port + prefix + middle + "sometable"));
    // unknown table
    Assert.assertEquals(404, Util.sendPutRequest("http://localhost:" + port + prefix + middle + "sometable/x"));
    Assert.assertEquals(404, Util.sendPutRequest("http://localhost:" + port + prefix + middle + "sometable/pfunk"));
    // no key
    Assert.assertEquals(400, Util.sendPutRequest(baseUrl));
    // correct key but more in the path
    Assert.assertEquals(400, Util.sendPutRequest(baseUrl + "x/"));
    Assert.assertEquals(400, Util.sendPutRequest(baseUrl + "x/a"));
    // correct key but unsupported query -> 501 Not Implemented
    Assert.assertEquals(501, Util.sendPutRequest(baseUrl + "x?force=true"));

    // and shutdown
    this.accessor.stop();
  }

  /**
   * This tests that the accessor can be stopped and restarted
   */
  @Test
  public void testStopRestart() throws Exception {
    // configure an accessor
    String baseUrl = setupAccessor("access.rest", "/continuuity", "/table/");
    // test one valid key
    Util.writeAndGet(this.executor, baseUrl, "x", "y");
    // submit a GET with existing key -> 200 OK
    Assert.assertEquals(200, Util.sendGetRequest(baseUrl + "x"));
    // stop the connector
    this.accessor.stop();
    // verify that GET fails now. Should throw an exception
    try {
      Util.sendGetRequest(baseUrl + "x");
      Assert.fail("Expected HttpHostConnectException because connector was stopped.");
    } catch (HttpHostConnectException e) {
      // this is expected
    }
    // restart the connector
    this.accessor.start();
    // submit a GET with existing key -> 200 OK
    Assert.assertEquals(200, Util.sendGetRequest(baseUrl + "x"));
    // and finally shut down
    this.accessor.stop();
  }
}
