package com.continuuity.gateway.v2.handlers;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.gateway.v2.Gateway;
import com.continuuity.gateway.v2.GatewayConstants;
import com.google.common.collect.ImmutableSet;
import junit.framework.Assert;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;

/**
 * Test ping handler.
 */
public class PingHandlerTest {

  private static Gateway gatewayV2;
  private static String hostName = "localhost";
  private static int port;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(GatewayConstants.ConfigKeys.PORT, 0);
    gatewayV2 = new Gateway(cConf, InetAddress.getByName(hostName), ImmutableSet.<HttpHandler>of(new PingHandler()));
    gatewayV2.startAndWait();
    port = gatewayV2.getBindAddress().getPort();
  }

  @AfterClass
  public static void finish() throws Exception {
    gatewayV2.stopAndWait();
  }

  @Test
  public void testPing() throws Exception {
    DefaultHttpClient httpclient = new DefaultHttpClient();
    HttpGet httpget = new HttpGet(String.format("http://%s:%d/ping", hostName, port));
    HttpResponse response = httpclient.execute(httpget);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals("OK.\n", EntityUtils.toString(response.getEntity()));
  }
}
