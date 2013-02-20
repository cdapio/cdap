package com.continuuity.gateway;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.accessor.DataRestAccessor;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * This test configures a gateway with a test accessor as the single connector
 * and verifies that key/values that have been persisted to the in-memory data
 * fabric can be retrieved vie HTTP get requests.
 */
public class GatewayRestAccessorAuthTest {

  // Our logger object
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory
      .getLogger(GatewayRestAccessorAuthTest.class);

  // A set of constants we'll use in these tests
  static String name = "access.rest";
  static final String prefix = "/continuuity";
  static final String path = "/table/";
  static final int valuesToGet = 10;
  static int port = 10000;

  static final String apiKey = "SampleTestApiKey";
  static final String cluster = "SampleTestClusterName";

  // This is the Gateway object we'll use for these tests
  private Gateway theGateway = null;

  private OperationExecutor executor;

  /**
   * Create a new Gateway instance to use in these set of tests. This method
   * is called before any of the test methods.
   *
   * @throws Exception If the Gateway can not be created.
   */
  @Before
  public void setupGateway() throws Exception {

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
        new DataFabricModules().getInMemoryModules());
    this.executor = injector.getInstance(OperationExecutor.class);

    // Look for a free port
    port = PortDetector.findFreePort();

    // Create and populate a new config object
    CConfiguration configuration = new CConfiguration();

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
    
    // Authentication configuration
    configuration.setBoolean(Constants.CONFIG_AUTHENTICATION_REQUIRED, true);
    configuration.set(Constants.CONFIG_CLUSTER_NAME, cluster);
    Map<String,List<String>> keysAndClusters =
        new TreeMap<String,List<String>>();
    keysAndClusters.put(apiKey, Arrays.asList(new String [] { cluster }));

    // Now create our Gateway
    theGateway = new Gateway();
    theGateway.setExecutor(this.executor);
    theGateway.setConsumer(new TestUtil.NoopConsumer());
    theGateway.setPassportClient(new MockedPassportClient(keysAndClusters));
    theGateway.start(null, configuration);

  } // end of setupGateway


  /**
   * Test that we can send simulated REST events to a Queue
   *
   * @throws Exception If any exceptions happen during the test
   */
  @Test
  public void testReadFromGateway() throws Exception {

    // Enable authentication with the correct key
    TestUtil.enableAuth(apiKey);
    // Send some REST events
    for (int i = 0; i < valuesToGet; i++) {
      TestUtil.writeAndGet(this.executor,
          "http://localhost:" + port + prefix + path,
          "key" + i, "value" + i);
    }
    
    // Enable authentication with the wrong key
    TestUtil.enableAuth(apiKey + "BAD");
    // Send some REST events
    for (int i = 0; i < valuesToGet; i++) {
      // Except a security exception to be thrown
      try {
        TestUtil.writeAndGet(this.executor,
            "http://localhost:" + port + prefix + path,
            "key" + i, "value" + i);
        assertTrue("Expected authentication to fail but passed", false);
      } catch (SecurityException se) {
        // Expected
      }
    }
    
    // Disable authentication for other tests
    TestUtil.disableAuth();

    // Stop the Gateway
    theGateway.stop(false);
  }


}
