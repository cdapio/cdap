package com.continuuity.gateway;

import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.discovery.DiscoveryService;
import com.continuuity.discovery.DiscoveryServiceClient;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.gateway.connector.AppFabricRestConnector;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.passport.PassportConstants;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AppFabricConnectorTest {
  private final static String CONTINUUITY_API_KEY = PassportConstants.CONTINUUITY_API_KEY_HEADER;
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

  private static AppFabricServer server;
  private static LocationFactory lf;
  private static CConfiguration configuration;
  private static Connector restConnector;
  private static int port;
  private static String prefix = "";
  private static String middle = "/app/";

  /**
   * Set up in-memory data fabric
   */
  @Before
  public void setup() throws Exception {
    configuration = CConfiguration.create();
    configuration.setInt(com.continuuity.common.conf.Constants.CFG_APP_FABRIC_SERVER_PORT,
                         PortDetector.findFreePort());
    configuration.set("app.output.dir", "/tmp/app");
    configuration.set("app.tmp.dir", "/tmp/temp");
    Injector injector = Guice.createInjector(
      new BigMamaModule(configuration),
      new DataFabricModules().getInMemoryModules()
    );

    // Start the discovery service. Used to find where FAR is running.
    DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);
    discoveryService.startAndWait();
    TimeUnit.SECONDS.sleep(2);

    // Get the instance of AppFabricServer, start it.
    server = injector.getInstance(AppFabricServer.class);
    server.startAndWait();

    // As we are handling Locations for File and Path, get the factory for creating locations.
    lf = injector.getInstance(LocationFactory.class);

    // Create and configure the AppFabricConnector.
    restConnector = new AppFabricRestConnector();
    DiscoveryServiceClient ds = injector.getInstance(DiscoveryServiceClient.class);
    restConnector.setDiscoveryServiceClient(ds);

    String name = "test";
    restConnector.setName(name);
    restConnector.setAuthenticator(new NoAuthenticator());

    port = PortDetector.findFreePort();
    // configure it
    configuration.setInt(Constants.buildConnectorPropertyName(name,Constants.CONFIG_PORT), port);
    configuration.set(Constants.buildConnectorPropertyName(name,Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_MIDDLE), middle);
    restConnector.configure(configuration);
    restConnector.start();
  } // end of setupGateway


  @Test
  public void testDeploy() throws Exception {
    // setup connector
    String deployUrl = "http://localhost:" + port + "/app";

    // JAR file is stored in test/resource/WordCount.jar.
    String jarFileName="WordCount.jar";
    File archive = FileUtils.toFile(getClass().getResource("/" + jarFileName));

    Map<String,String> headers= Maps.newHashMap();
    headers.put(CONTINUUITY_API_KEY,"api-key-example"); // Very important header.
    headers.put("X-Archive-Name", "WordCount.jar"); // Important header.

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      ByteStreams.copy(new FileInputStream(archive), bos);
    } finally {
      bos.close();
    }
    Assert.assertEquals(200, TestUtil.sendPutRequest(deployUrl, bos.toByteArray(), headers));
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
}
