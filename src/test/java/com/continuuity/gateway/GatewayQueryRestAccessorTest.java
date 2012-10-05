package com.continuuity.gateway;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.accessor.QueryRestAccessor;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * This test configures a gateway with a test accessor as the single connector
 * and verifies that key/values that have been persisted to the in-memory data
 * fabric can be retrieved vie HTTP get requests.
 */
public class GatewayQueryRestAccessorTest {

  // Our logger object
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory
      .getLogger(GatewayQueryRestAccessorTest.class);

  // A set of constants we'll use in these tests
  static String name = "access.query";
  static final String prefix = "/continuuity";
  static final String path = "/feedread/";
  static int port = 10010;

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

    configuration.set(Constants.CONFIG_CONNECTORS, name);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_CLASSNAME),
        QueryRestAccessor.class.getCanonicalName());
    configuration.setInt(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PORT), port);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_MIDDLE), path);

    // Now create our Gateway
    theGateway = new Gateway();
    theGateway.setExecutor(this.executor);
    theGateway.setConsumer(new TestUtil.NoopConsumer());
    theGateway.start(null, configuration);

    this.baseUri = "http://localhost:" + port + "/rest-query/";
    
  } // end of setupGateway

  private String baseUri;
  
  /**
   * Test basic payvment query.
   * @throws Exception If any exceptions happen during the test
   */
  @Test @Ignore
  public void testPayvmentQuery() throws Exception {
    
    // make a get URL
    String getUrl = baseUri +
        "feedreader/readmethod?somekey=avalue&anotherkey=bvalue";

    // and issue a GET request to the server
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(new HttpGet(getUrl));
    client.getConnectionManager().shutdown();

    // verify the response is ok
    Assert.assertEquals(HttpStatus.SC_OK,
        response.getStatusLine().getStatusCode());

    // Stop the Gateway
    theGateway.stop(false);
  }


}
