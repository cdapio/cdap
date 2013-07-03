package com.continuuity.gateway;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.gateway.accessor.DatasetRestAccessor;
import com.continuuity.gateway.tools.DataClient;
import com.continuuity.gateway.tools.DataSetClient;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Data set client.
 */
public class DataSetClientTest {

  static CConfiguration configuration = null;
  static OperationExecutor executor;
  static Gateway gateway = null;

  /**
   * Set up our data fabric and insert some test key/value pairs.
   */
  @BeforeClass
  public static void setupDataFabricAndGateway() throws Exception {
    DataClient.debug = true;
    configuration = new CConfiguration();

    // Set up our Guice injections
    Injector injector = Guice.createInjector(new GatewayTestModule(configuration));
    executor = injector.getInstance(OperationExecutor.class);

    final String name = "access.rest";
    final String prefix = "/continuuity";
    final String path = "/table/";

    // configure a gateway
    final int port = PortDetector.findFreePort();
    configuration.setBoolean(Constants.CONFIG_DO_SERVICE_DISCOVERY, false);
    configuration.set(Constants.CONFIG_CONNECTORS, name);
    configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_CLASSNAME),
                      DatasetRestAccessor.class.getCanonicalName());
    configuration.setInt(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
    configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_MIDDLE), path);

    // Now create our Gateway with a dummy consumer (we don't run collectors)
    // and make sure to pass the data fabric executor to the gateway.
    gateway = new Gateway();
    gateway.setExecutor(executor);
    gateway.setConsumer(new TestUtil.NoopConsumer());
    gateway.setDiscoveryServiceClient(injector.getInstance(DiscoveryServiceClient.class));
    gateway.start(null, configuration);
  }

  @AfterClass
  public static void shutDownGateway() throws Exception {
    // and shut down
    gateway.stop(false);
  }

  @Test
  public void testValueAsCounter() throws OperationException {
    final String table = "tVAC";
    final String row = "myRow";
    final String column = "myCounter";
    Assert.assertEquals("OK.", new DataSetClient().execute(new String[] {
      "create", "--table", table }, configuration));
    Assert.assertEquals("OK.", new DataSetClient().execute(new String[] {
      "write", "--table", table, "--row", row, "--column", column, "--value", "41", "--counter" }, configuration));
    Assert.assertEquals("42", new DataSetClient().execute(new String[] {
      "increment", "--table", table, "--row", row, "--column", column, "--value", "1" }, configuration));
    Assert.assertEquals("42", new DataSetClient().execute(new String[] {
      "read", "--table", table, "--row", row, "--column", column, "--counter" }, configuration));
  }
}
