package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.accessor.DataRestAccessor;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *
 */
public class GatewayHttpsTest {
  // Our logger object
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory
    .getLogger(GatewayHttpsTest.class);

  // A set of constants we'll use in these tests
  static String name = "access.rest";
  static final String prefix = "/continuuity";
  static final String path = "/table/";
  static final int valuesToGet = 10;
  static int port = 10000;

  private Gateway theGateway = null;

  private String sslCertFileName ;

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
    //Enable SSL
    configuration.set(com.continuuity.common.conf.Constants.CFG_APPFABRIC_ENVIRONMENT,
                  "notDevsuite");
    // Update SSL passwords andpaths
    configuration.set(Constants.CFG_SSL_CERT_KEY_PASSWORD,"realtime");


    File filePath = FileUtils.toFile(this.getClass().getResource("ssl.cert"));
    configuration.set(Constants.CFG_SSL_CERT_KEY_PATH,filePath.getAbsolutePath());




    // Now create our Gateway
    theGateway = new Gateway();
    theGateway.setExecutor(this.executor);
    theGateway.setConsumer(new TestUtil.NoopConsumer());
    theGateway.start(null, configuration);

  } // end of setupGateway


  /**
   * Test that we can send simulated REST events to a Queue
   *
   * @throws Exception If any exceptions happen during the test
   */
  @Test  @Ignore
  public void testReadFromHttpsGateway() throws Exception {
  // Send some REST events
      for (int i = 0; i < valuesToGet; i++) {
        TestUtil.writeAndGet(this.executor,
          "https://localhost:" + port + prefix + path,
          "key" + i, "value" + i);
      }
    // Stop the Gateway
    theGateway.stop(false);
  }
}
