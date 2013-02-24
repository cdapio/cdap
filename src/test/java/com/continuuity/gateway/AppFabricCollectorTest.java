package com.continuuity.gateway;

import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.gateway.collector.RestCollector;
import com.continuuity.gateway.connector.AppFabricRestConnector;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.passport.PassportConstants;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.apache.http.conn.HttpHostConnectException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class AppFabricCollectorTest {


  private final static String CONTINUUITY_API_KEY = PassportConstants.CONTINUUITY_API_KEY_HEADER;
  private final static String CONTINUUITY_JAR_FILE_NAME = "X-Continuuity-JarFileName";

  static final OperationContext context = OperationContext.DEFAULT;

  static final String apiKey = "SampleTestApiKey";
  static final String cluster = "SampleTestClusterName";

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

  private static AppFabricServer server;
  private static LocationFactory lf;
  private static StoreFactory sFactory;
  private static CConfiguration configuration;

  /**
   * Set up in-memory data fabric
   */
  @Before
  public void setup() throws InterruptedException {

    configuration = CConfiguration.create();
    configuration.setInt(com.continuuity.common.conf.Constants.CFG_APP_FABRIC_SERVER_PORT, 45000);
    configuration.set("app.output.dir", "/tmp/app");
    configuration.set("app.tmp.dir", "/tmp/temp");

    Injector injector = Guice.createInjector(
      new BigMamaModule(configuration),
      new DataFabricModules().getInMemoryModules()
    );
    server = injector.getInstance(AppFabricServer.class);

    server.start();

    ListenableFuture<Service.State> future = server.start();

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
    Map<String,List<String>> keysAndClusters =
      new TreeMap<String,List<String>>();
    keysAndClusters.put(apiKey, Arrays.asList(new String[]{cluster}));
    restConnector.setPassportClient(new MockedPassportClient(keysAndClusters));
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

  static final ByteBuffer emptyBody = ByteBuffer.wrap(new byte[0]);

  /**
   * This tests that the connector can be stopped and restarted
   */
  @Test @Ignore
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

  @Test @Ignore
  public void testDeploy() throws Exception {
    // setup connector
    String baseUrl = setupConnector("connector.rest", "/continuuity", "/rest-app/");
    String deployUrl = this.connector.getHttpConfig().getBaseUrl() + "deploy";
    // setup collector
    int port = this.connector.getHttpConfig().getPort();

    String jarFileName="WordCount.jar";
    File farFile = FileUtils.toFile(this.getClass().getResource("/"+jarFileName));

    Map<String,String> headers= Maps.newHashMap();
    headers.put(CONTINUUITY_API_KEY,"api-key-example");
    headers.put(CONTINUUITY_JAR_FILE_NAME, jarFileName);

//    BufferedInputStream is = new BufferedInputStream(new FileInputStream(jarFileName));

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      ByteStreams.copy(new FileInputStream(farFile), bos);
    } finally {
      bos.close();
    }

    Assert.assertEquals(200, TestUtil.sendPostRequest(deployUrl, bos.toByteArray(), headers));
  }
  @After
  public void stop() {
    Service.State state = server.stopAndWait();
    Assert.assertTrue(state == Service.State.TERMINATED);
  }
}
