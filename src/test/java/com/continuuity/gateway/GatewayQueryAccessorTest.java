package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.common.zookeeper.ZookeeperClientProvider;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.gateway.accessor.QueryRestAccessor;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test configures a gateway with a test accessor as the single connector
 * and verifies that key/values that have been persisted to the in-memory data
 * fabric can be retrieved vie HTTP get requests.
 */
public class GatewayQueryAccessorTest {

  // Our logger object
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory
      .getLogger(GatewayQueryAccessorTest.class);

  // A set of constants we'll use in these tests
  static String name = "access.query";
  static final String prefix = "/";
  static final String path = "query/";
  static int port = 10005;

  // This is the Gateway object we'll use for these tests
  static private Gateway theGateway = null;

  protected static InMemoryZookeeper zookeeper;
  protected static CuratorFramework zkclient;
  protected static CConfiguration configuration;

  /**
   * Create a new Gateway instance to use in these set of tests. This method
   * is called before any of the test methods.
   *
   * @throws Exception If the Gateway can not be created.
   */
  @BeforeClass
  public static void setup() throws Exception {

    configuration = CConfiguration.create();
    zookeeper = new InMemoryZookeeper();
    zkclient = ZookeeperClientProvider.getClient(
        zookeeper.getConnectionString(),
        1000
    );
    configuration.set(com.continuuity.common.conf.Constants.
        CFG_ZOOKEEPER_ENSEMBLE, zookeeper.getConnectionString());
    configuration.set(com.continuuity.common.conf.Constants.
        CFG_STATE_STORAGE_CONNECTION_URL, "jdbc:hsqldb:mem:InmemoryZK?user=sa");

    // Look for a free port
    port = PortDetector.findFreePort();

    // Create and populate a new config object
    configuration.set(Constants.CONFIG_CONNECTORS, name);
    configuration.set(
        Constants.buildConnectorPropertyName(name, Constants.CONFIG_CLASSNAME),
        QueryRestAccessor.class.getCanonicalName());
    configuration.setInt(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PORT), port);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_MIDDLE), path);

    // Now create our Gateway
    theGateway = new Gateway();
    theGateway.setExecutor(new NoOperationExecutor());
    theGateway.setConsumer(new TestUtil.NoopConsumer());
    theGateway.start(null, configuration);

    // TODO clean this up as soon as it is fixed in flow
    // this appears to be need for flows/queries to work in tests
    configuration.setBoolean("execution.mode.singlenode", true);

  } // end of setupGateway

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.closeQuietly(zookeeper);
    Closeables.closeQuietly(zkclient);
    // Stop the Gateway
    theGateway.stop(false);
  }

  /**
   * Hello World! QueryProvider that responds with the method
   * and arguments passed to it as string.
   */
  // TODO: PLEASE FIX ONCE THE PROCEDURE STUFF IS READY!!

//  public class HelloWorldQueryProvider extends QueryProvider {
//    @Override
//    public void configure(QuerySpecifier specifier) {
//      specifier.service("HelloWorld");
//      specifier.type(QueryProviderContentType.TEXT);
//      specifier.provider(HelloWorldQueryProvider.class);
//    }
//
//    @Override
//    public QueryProviderResponse process(
//        String method, Map<String, String> arguments) {
//      StringBuffer sb = new StringBuffer();
//      sb.append("method : ").append(method).append(" [ ");
//      for(Map.Entry<String, String> argument : arguments.entrySet()) {
//        sb.append(argument.getKey()).append("=").append(argument.getValue());
//      }
//      sb.append(" ] ");
//      return new QueryProviderResponse(sb.toString());
//    }
//  }

  // TODO: PLEASE FIX ONCE THE PROCEDURE STUFF IS READY!!
//  @Test(timeout = 30000)
//  public void testQueryThroughGateway() throws Exception {
//
//    /** This sucks, but it is what it is. */
//    FlowletExecutionContext ctx
//        = new FlowletExecutionContext("fid", "flid", false);
//    FlowIdentifier identifier =
//      new FlowIdentifier("demo", "demo", "test", -1);
//    identifier.setType(EntityType.QUERY);
//    ctx.setFlowIdentifier(identifier);
//
//    ctx.setConfiguration(configuration);
//    ctx.setInstanceId(1);
//    ctx.setOperationExecutor(new NoOperationExecutor());
//
//    // Create an instance of QueryProviderProcessor.
//    final QueryProviderProcessor queryProcessor =
//        new QueryProviderProcessor(new HelloWorldQueryProvider(), ctx);
//    // Start the query processor.
//    new Thread(new Runnable() {
//      @Override
//      public void run() {
//        try {
//          queryProcessor.start();
//        } catch (Exception e) {
//          e.printStackTrace();
//        }
//      }
//    }).start();
//
//    // After this is done, we are certain that the service has been
//    // registered.
//    while(! queryProcessor.isRunning()) {
//      Thread.sleep(10);
//    }
//
//    // now query the processor through gateway
//    String uriPrefix = "http://localhost:" + port + prefix + path;
//
//    // test that ping works
//    sendAndExpectStatus(
//        "http://localhost:" + port + "/ping", HttpStatus.SC_OK);
//
//    // test an invalid service, should return 404
//    sendAndExpectStatus(uriPrefix +
//        "HiUniverse/method?p1=v1&p2=v2", HttpStatus.SC_NOT_FOUND);
//
//    // test requests without method, should return 404
//    sendAndExpectStatus(uriPrefix +
//        "HelloWorld", HttpStatus.SC_NOT_FOUND);
//    sendAndExpectStatus(uriPrefix +
//        "HelloWorld/", HttpStatus.SC_NOT_FOUND);
//    sendAndExpectStatus(uriPrefix +
//        "HelloWorld?p1=v1", HttpStatus.SC_NOT_FOUND);
//    sendAndExpectStatus(uriPrefix +
//        "HelloWorld/p1=v1", HttpStatus.SC_OK); // no ? -> p1=v1 is the "method"
//
//    // test a request with method but incomplete args, should ignore arg
//    sendAndExpectStatus(uriPrefix +
//        "HelloWorld/method?p1=v1&p2", HttpStatus.SC_OK);
//
//    // test a request with method but without params, should be fine
//    sendAndExpectStatus(uriPrefix +
//        "HelloWorld/method", HttpStatus.SC_OK);
//    sendAndExpectStatus(uriPrefix +
//        "HelloWorld/method?", HttpStatus.SC_OK);
//    sendAndExpectStatus(uriPrefix +
//        "HelloWorld/method/", HttpStatus.SC_NOT_FOUND);
//
//    // test a valid query, should return a known value
//    HttpClient httpClient = new DefaultHttpClient();
//    HttpGet get = new HttpGet(uriPrefix + "HelloWorld?method=A&p1=v1&p2=v2");
//    HttpResponse response= httpClient.execute(get);
//    Assert.assertEquals(HttpStatus.SC_OK,
//        response.getStatusLine().getStatusCode());
//
//    // verify the response is as expected
//    String contentType = response.getEntity().getContentType().getValue();
//    // returned content type contains a charset ("text/plain; charset=...");
//    Assert.assertTrue(contentType.startsWith("text/plain"));
//    int pos = contentType.indexOf("charset=");
//    String charset = pos > 0 ? contentType.substring(pos+8) : "UTF-8";
//    int length = (int) response.getEntity().getContentLength();
//    InputStream content = response.getEntity().getContent();
//    if (length > 0) {
//      byte[] bytes = new byte[length];
//      int bytesRead = content.read(bytes);
//      // verify that the entire content was read
//      Assert.assertEquals(-1, content.read(new byte[1]));
//      Assert.assertEquals(length, bytesRead);
//      Assert.assertEquals("method : A [ p2=v2p1=v1 ] ",
//          new String(bytes, charset));
//    }
//
//    // Now stop the query processor.
//    queryProcessor.stop();
//  }

  private void sendAndExpectStatus(String uri, int expectedStatus)
      throws Exception {
    HttpClient httpClient = new DefaultHttpClient();
    HttpGet get = new HttpGet(uri);
    HttpResponse response = httpClient.execute(get);
    Assert.assertEquals(expectedStatus,
        response.getStatusLine().getStatusCode());
  }

}
