package com.continuuity.gateway;

import com.continuuity.api.common.Bytes;
import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.DeploymentServerFactory;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.gateway.collector.RestCollector;
import com.continuuity.gateway.connector.AppFabricRestConnector;
import com.continuuity.gateway.consumer.StreamEventWritingConsumer;
import com.continuuity.internal.app.authorization.PassportAuthorizationFactory;
import com.continuuity.internal.app.deploy.SyncManagerFactory;
import com.continuuity.internal.app.services.InMemoryAppFabricServerFactory;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.pipeline.PipelineFactory;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import org.apache.http.conn.HttpHostConnectException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Map;

public class AppFabricCollectorTest {


  private final static String CONTINUUITY_API_KEY = com.continuuity.common.conf.Constants.CONTINUUITY_API_KEY_HEADER;
  private final static String CONTINUUITY_JAR_FILE_NAME = "X-Continuuity-JarFileName";

  static final OperationContext context = OperationContext.DEFAULT;

  /**
   * this is the executor for all access to the data fabric
   */
  private OperationExecutor executor;

  /**
   * the rest connector we will use in the tests
   */
  private AppFabricRestConnector connector;

  /**
   * the rest collector we will use in the clear test
   */
  private RestCollector collector;

  private static AppFabricService.Iface server;
  private static LocationFactory lf;
  private static StoreFactory sFactory;
  private static CConfiguration configuration;

  private static class SimpleDeploymentServerModule extends AbstractModule {
    /**
     * Configures a {@link com.google.inject.Binder} via the exposed methods.
     */
    @Override
    protected void configure() {
      bind(DeploymentServerFactory.class).to(InMemoryAppFabricServerFactory.class);
      bind(LocationFactory.class).to(LocalLocationFactory.class);
      bind(new TypeLiteral<PipelineFactory<?>>(){}).to(new TypeLiteral<SynchronousPipelineFactory<?>>(){});
      bind(ManagerFactory.class).to(SyncManagerFactory.class);
      bind(StoreFactory.class).to(MDSStoreFactory.class);
      bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
      bind(AuthorizationFactory.class).to(PassportAuthorizationFactory.class);
      bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
    }
  }
  /**
   * Set up in-memory data fabric
   */
  @Before
  public void setup() {

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
        new DataFabricModules().getInMemoryModules(),
        new SimpleDeploymentServerModule());

    DeploymentServerFactory factory = injector.getInstance(DeploymentServerFactory.class);

    configuration = CConfiguration.create();
    configuration.set("app.output.dir", "/tmp/app");
    configuration.set("app.tmp.dir", "/tmp/temp");

    // Create the server.
    server = factory.create(configuration);

    // Create location factory.
    lf = injector.getInstance(LocationFactory.class);

    // Create store
    sFactory = injector.getInstance(StoreFactory.class);

  } // end of setupGateway

  /**
   * Create a new rest connector with a given name and parameters
   *
   * @param name   The name for the connector
   * @param prefix The path prefix for the URI
   * @param middle The path middle for the URI
   * @return the connector's base URL for REST requests
   */
  String setupConnector(String name, String prefix, String middle)
      throws Exception {
    // bring up a new connector
    AppFabricRestConnector restConnector = new AppFabricRestConnector();
    restConnector.setName(name);
    restConnector.setAuthenticator(new NoAuthenticator());
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
    restConnector.configure(configuration);
    // start the connector
    restConnector.start();
    // all fine
    this.connector = restConnector;
    return "http://localhost:" + port + prefix + middle;
  }

  // we will need this to test the clear API
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
    // start the connector
    restCollector.start();
    // all fine
    this.collector = restCollector;
    return "http://localhost:" + port + prefix + middle;
  }

  static final ByteBuffer emptyBody = ByteBuffer.wrap(new byte[0]);


  /**
   * This tests that the connector returns the correct HTTP codes for
   * invalid requests
   *
   * @throws Exception if anything goes wrong
   */
  @Test
  public void testBadRequests() throws Exception {
    // configure an connector
    String prefix = "/continuuity", middle = "/table/";
    String baseUrl = setupConnector("access.rest", prefix, middle);
    int port = this.connector.getHttpConfig().getPort();

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
    this.connector.stop();
  }

  /**
   * This tests that the connector can be stopped and restarted
   */
  @Test
  public void testStopRestart() throws Exception {
    // configure an connector
    String baseUrl = setupConnector("access.rest", "/continuuity", "/table/");
    int port = this.connector.getHttpConfig().getPort();
    // test that ping works
    Assert.assertEquals(200, TestUtil.sendGetRequest("http://localhost:" + port + "/ping"));
    // stop the connector
    this.connector.stop();
    // verify that GET fails now. Should throw an exception
    try {
      TestUtil.sendGetRequest("http://localhost:" + port + "/ping");
      Assert.fail("Expected HttpHostConnectException because connector was " +
          "stopped.");
    } catch (HttpHostConnectException e) {
      // this is expected
    }
    // restart the connector
    this.connector.start();
    // submit a GET with existing key -> 200 OK
    Assert.assertEquals(200, TestUtil.sendGetRequest("http://localhost:" + port + "/ping"));
    // and finally shut down
    this.connector.stop();
  }

  @Test
  public void testDeploy() throws Exception {
    // setup connector
    String baseUrl = setupConnector("access.rest", "/continuuity", "/rest-app/");
    String deployUrl = this.connector.getHttpConfig().getBaseUrl() + "deploy";
    // setup collector
    String collectorUrl =
        setupCollector("collect.rest", "/continuuity", "/stream/");
    int port = this.connector.getHttpConfig().getPort();

    Map<String,String> headers= Maps.newHashMap();
    headers.put(CONTINUUITY_API_KEY,"api-key-example");
    headers.put(CONTINUUITY_JAR_FILE_NAME,"jar-file-name");
    byte[] bytes=Bytes.toBytes("123456");
    Assert.assertEquals(200, TestUtil.sendPostRequest(deployUrl, bytes, headers));
  }
}

