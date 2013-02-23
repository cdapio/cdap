package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.gateway.util.HttpConfig;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class GatewayHttpsTest {
  // Our logger object
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory
    .getLogger(GatewayHttpsTest.class);

  static int port = 10000;
  /**
   * Test SSL Configs
   * @throws Exception
   */

@Test
public void testHttpsConfigs() throws Exception {

  String name = "https-test";
    // Look for a free port
    port =  PortDetector.findFreePort();

    // Create and populate a new config object
    CConfiguration configuration = new CConfiguration();

    //Enable SSL
    configuration.set(com.continuuity.common.conf.Constants.CFG_APPFABRIC_ENVIRONMENT,
                  "notDevsuite");
    // Update SSL passwords andpaths
    configuration.set(Constants.CFG_SSL_CERT_KEY_PASSWORD,"realtime");


    File filePath = FileUtils.toFile(this.getClass().getResource("/ssl.cert"));
    configuration.set(Constants.CFG_SSL_CERT_KEY_PATH,filePath.getAbsolutePath());

    HttpConfig defaults = new HttpConfig(name);
    HttpConfig config = HttpConfig.configure("https", configuration,defaults);

    assertTrue(config.isSsl());  // Check if ssl is enabled
    assert(config.getPort()==443); //Check if port is set to 443
  }

  /**
   * This test needs to be commented out since you need to start services as root to bind to 443
   *
   * @throws Exception If any exceptions happen during the test
   */
//  @Test @Ignore
//  public void testReadFromHttpsGateway() throws Exception {
//
//    // Send some REST events
//      for (int i = 0; i < valuesToGet; i++) {
//        TestUtil.writeAndGet(this.executor,
//          "https://localhost:" + port + prefix + path,
//          "key" + i, "value" + i);
//      }
//    // Stop the Gateway
//    theGateway.stop(false);
//  }
}
