package com.continuuity.gateway;

import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.discovery.DiscoveryService;
import com.continuuity.discovery.DiscoveryServiceClient;
import com.continuuity.gateway.accessor.DataRestAccessor;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test configures a gateway with a test accessor as the single connector
 * and verifies that key/values that have been persisted to the in-memory data
 * fabric can be retrieved vie HTTP get requests.
 */
public class GatewayRestAccessorTest {

  // Our logger object
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory
      .getLogger(GatewayRestAccessorTest.class);

  // A set of constants we'll use in these tests
  static String name = "access.rest";
  static final String prefix = "/continuuity";
  static final String path = "/table/";
  static final int valuesToGet = 10;
  static int port = 10000;

  // This is the Gateway object we'll use for these tests
  private Gateway theGateway = null;

  private OperationExecutor executor;

  private static DiscoveryService discoveryService;

  /**
   * Create a new Gateway instance to use in these set of tests. This method
   * is called before any of the test methods.
   *
   * @throws Exception If the Gateway can not be created.
   */
  @Before
  public void setupGateway() throws Exception {
    CConfiguration configuration = new CConfiguration();

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
        new DataFabricModules().getInMemoryModules(),
        new BigMamaModule(configuration));
    this.executor = injector.getInstance(OperationExecutor.class);
    discoveryService = injector.getInstance(DiscoveryService.class);

    // Look for a free port
    port = PortDetector.findFreePort();

    // Create and populate a new config object

    configuration.setBoolean(Constants.CONFIG_DO_SERVICE_DISCOVERY, false);
    configuration.set(Constants.CONFIG_CONNECTORS, name);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_CLASSNAME), DataRestAccessor.class.getCanonicalName());
    configuration.setInt(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PORT), port);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_MIDDLE), path);

    // Now create our Gateway
    discoveryService.startAndWait();
    theGateway = new Gateway();
    theGateway.setExecutor(this.executor);
    theGateway.setConsumer(new TestUtil.NoopConsumer());
    theGateway.setDiscoveryServiceClient(
        injector.getInstance(DiscoveryServiceClient.class));
    theGateway.start(null, configuration);

  } // end of setupGateway


  /**
   * Test that we can send simulated REST events to a Queue
   *
   * @throws Exception If any exceptions happen during the test
   */
  @Test
  public void testReadFromGateway() throws Exception {

    // Send some REST events
    for (int i = 0; i < valuesToGet; i++) {
      TestUtil.writeAndGet(this.executor,
          "http://localhost:" + port + prefix + path,
          "key" + i, "value" + i);
    }

    // Stop the Gateway
    theGateway.stop(false);
  }


}
