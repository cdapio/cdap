package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.gateway.connector.AppFabricRestConnector;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.passport.PassportConstants;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests Reactor connector.
 */
public class AppFabricConnectorTest {
  private static final String CONTINUUITY_API_KEY = PassportConstants.CONTINUUITY_API_KEY_HEADER;

  private static AppFabricServer server;
  private static Connector restConnector;
  private static String baseURL;
  private static String pingURL;
  private static final String prefix = "";

  /**
   * Set up in-memory data fabric
   */
  @Before
  public void setup() throws Exception {
    CConfiguration configuration = CConfiguration.create();
    configuration.setInt(com.continuuity.common.conf.Constants.CFG_APP_FABRIC_SERVER_PORT,
                         PortDetector.findFreePort());
    configuration.set("app.output.dir", "/tmp/app");
    configuration.set("app.tmp.dir", "/tmp/temp");
    Injector injector = Guice.createInjector(new GatewayTestModule(configuration));

    // Start the discovery service. Used to find where FAR is running.
    TimeUnit.SECONDS.sleep(2);

    // Get the instance of AppFabricServer, start it.
    server = injector.getInstance(AppFabricServer.class);
    server.startAndWait();

    // Create and configure the AppFabricConnector.
    restConnector = new AppFabricRestConnector();
    DiscoveryServiceClient ds = injector.getInstance(DiscoveryServiceClient.class);
    restConnector.setDiscoveryServiceClient(ds);

    String name = "test";
    restConnector.setName(name);
    restConnector.setAuthenticator(new NoAuthenticator());

    int port = PortDetector.findFreePort();
    // configure it
    configuration.setInt(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
    configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
    String middle = "/app";
    baseURL = "http://localhost:" + port + middle;
    pingURL = "http://localhost:" + port + "/ping";
    configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_MIDDLE), middle);
    restConnector.configure(configuration);
    restConnector.start();
  } // end of setupGateway

  @Test
  public void testDeployWithHttpPut() throws Exception {
    String deployStatusUrl = baseURL + "/status";
    Assert.assertEquals(200, deploy("WordCount.jar", true));

    Map<String, String> headers = Maps.newHashMap();
    headers.put(CONTINUUITY_API_KEY, "api-key-example");
    headers.put("Content-Type", "application/json");

    HttpResponse response = TestUtil.sendGetRequest(deployStatusUrl, headers);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testDeploy() throws Exception {
    String deployStatusUrl = baseURL + "/status";
    Assert.assertEquals(200, deploy("WordCount.jar"));

    Map<String, String> headers = Maps.newHashMap();
    headers.put(CONTINUUITY_API_KEY, "api-key-example");
    headers.put("Content-Type", "application/json");

    HttpResponse response = TestUtil.sendGetRequest(deployStatusUrl, headers);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }
// TODO(ENG-3120, ENG-3119): The test doesn't pass since we need to inject metrics discovery client.
//  @Test
//  public void testDelete() throws Exception {
//    String deployStatusUrl = baseURL + "/status";
//    Assert.assertEquals(200, deploy("WordCount.jar"));
//
//    Map<String, String> headers = Maps.newHashMap();
//    headers.put(CONTINUUITY_API_KEY, "api-key-example");
//    headers.put("Content-Type", "application/json");
//
//    HttpResponse response = TestUtil.sendGetRequest(deployStatusUrl, headers);
//    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
//
//    String deleteUrl = baseURL + "/WordCountApp";
//    int responseCode = TestUtil.sendDeleteRequest(deleteUrl);
//    Assert.assertEquals(200, responseCode);
//
//  }


  @Test
  public void testDeployStatus() throws Exception {
    Assert.assertEquals(200, deploy("WordCount.jar"));

    String deployStatusUrl = baseURL + "/status";

    Map<String, String> headers = Maps.newHashMap();
    headers.put(CONTINUUITY_API_KEY, "api-key-example");
    headers.put("Content-Type", "application/json");

    HttpResponse response = TestUtil.sendGetRequest(deployStatusUrl, headers);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    Map<String, Object> map = new Gson().fromJson(reader, new TypeToken<Map<String, Object>>() {}.getType());

    Assert.assertTrue((Double) map.get("status") == 5.0);
  }

  @Test
  public void testPing() throws Exception {
    Assert.assertEquals(200, deploy("WordCount.jar"));

    Map<String, String> headers = Maps.newHashMap();

    HttpResponse response = TestUtil.sendGetRequest(pingURL, headers);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testSetFlowletInstances() throws Exception {
    Assert.assertEquals(200, deploy("WordCount.jar"));
    String startFlowUrl = baseURL + "/WordCountApp/flow/WordCountFlow?op=start";
    String stopFlowUrl = baseURL + "/WordCountApp/flow/WordCountFlow?op=stop";
    String queryInstancesUrl = baseURL + "/WordCountApp/flow/WordCountFlow/Tokenizer?op=instances";
    String setInstancesUrl = baseURL + "/WordCountApp/flow/WordCountFlow/Tokenizer?op=instances";

    Map<String, String> headers = Maps.newHashMap();
    headers.put(CONTINUUITY_API_KEY, "api-key-example");
    headers.put("Content-Type", "application/json");

    Assert.assertEquals(200, TestUtil.sendPostRequest(startFlowUrl, headers));

    Assert.assertEquals(200, TestUtil.sendPutRequest(setInstancesUrl, "{\"instances\":3}", headers));

    HttpResponse response = TestUtil.sendGetRequest(queryInstancesUrl, headers);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    Map<String, Integer> map = new Gson().fromJson(reader, new TypeToken<Map<String, Integer>>() {}.getType());

    Assert.assertTrue(map.get("instances") == 3);

    Assert.assertEquals(200, TestUtil.sendPostRequest(stopFlowUrl, headers));
  }

  @Test
  public void testStartAndStopFlow() throws Exception {
    Assert.assertEquals(200, deploy("WordCount.jar"));
    String startFlowUrl = baseURL + "/WordCountApp/flow/WordCountFlow?op=start";
    String stopFlowUrl = baseURL + "/WordCountApp/flow/WordCountFlow?op=stop";
    String startProcedureUrl = baseURL + "/WordCountApp/procedure/WordFrequency?op=start";
    String stopProcedureUrl = baseURL + "/WordCountApp/procedure/WordFrequency?op=stop";
    String statusFlowUrl = baseURL + "/WordCountApp/flow/WordCountFlow?op=status";

    Map<String, String> headers = Maps.newHashMap();
    headers.put(CONTINUUITY_API_KEY, "api-key-example");
    headers.put("Content-Type", "application/json");

    Assert.assertEquals(200, TestUtil.sendPostRequest(startFlowUrl, headers));

    HttpResponse response = TestUtil.sendGetRequest(statusFlowUrl, headers);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    Map<String, String> map = new Gson().fromJson(reader, new TypeToken<Map<String, String>>() {}.getType());

    Assert.assertEquals("RUNNING", map.get("status"));

    Assert.assertEquals(200, TestUtil.sendPostRequest(stopFlowUrl, headers));

    Assert.assertEquals(200, TestUtil.sendPostRequest(startFlowUrl, "{\"argument1\":\"value1\"}", headers));

    Assert.assertEquals(400, TestUtil.sendPostRequest(startFlowUrl, "{\"argument1\"}", headers));

    Assert.assertEquals(200, TestUtil.sendPostRequest(stopFlowUrl, headers));

    Assert.assertEquals(200, TestUtil.sendPostRequest(startProcedureUrl, headers));
    Assert.assertEquals(200, TestUtil.sendPostRequest(stopProcedureUrl, headers));
  }

  @After
  public void stop() {
    Service.State state = server.stopAndWait();
    Assert.assertTrue(state == Service.State.TERMINATED);
    try {
      restConnector.stop();
    } catch (Exception e) {
      // suck it up.
    }
  }

  private int deploy(String jarFileName) throws Exception {
    return deploy(jarFileName, false);
  }

  private int deploy(String jarFileName, boolean useHttpPut) throws Exception {
    // JAR file is stored in test/resource/WordCount.jar.
    File archive = FileUtils.toFile(getClass().getResource("/" + jarFileName));

    Map<String, String> headers = Maps.newHashMap();
    headers.put(CONTINUUITY_API_KEY, "api-key-example");
    headers.put("X-Archive-Name", jarFileName);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      ByteStreams.copy(new FileInputStream(archive), bos);
    } finally {
      bos.close();
    }
    if (useHttpPut) {
      return TestUtil.sendPutRequest(baseURL, bos.toByteArray(), headers);
    } else {
      return TestUtil.sendPostRequest(baseURL, bos.toByteArray(), headers);
    }
  }
}
