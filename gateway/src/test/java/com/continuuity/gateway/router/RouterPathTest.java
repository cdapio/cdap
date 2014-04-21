package com.continuuity.gateway.router;

import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.GatewayFastTestsSuite;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *  To test the RouterPathLookup regular expression tests.
 */
public class RouterPathTest {

  private static RouterPathLookup pathLookup;
  private final String VERSION = "HTTP/1.1";
  private final String APIKEY = "undefined";

  @BeforeClass
  public static void init() throws Exception {
    pathLookup = GatewayFastTestsSuite.getInjector().getInstance(RouterPathLookup.class);
  }

  @Test
  public void testRouterFlowPathLookUp() throws Exception {
    String flowPath = "/v2//apps/ResponseCodeAnalytics/flows/LogAnalyticsFlow/status";
    String result = pathLookup.getRoutingPath(flowPath, "GET", APIKEY, VERSION);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testRouterWorkFlowPathLookUp() throws Exception {
    String procPath = "/v2/apps/PurchaseHistory/workflows/PurchaseHistoryWorkflow/status";
    String result = pathLookup.getRoutingPath(procPath, "GET", APIKEY, VERSION);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterProcedurePathLookUp() throws Exception {
    String procPath = "/v2//apps/ResponseCodeAnalytics/procedures/StatusCodeProcedure/status";
    String result = pathLookup.getRoutingPath(procPath, "GET", APIKEY, VERSION);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterDeployPathLookUp() throws Exception {
    String procPath = "/v2//apps/";
    String result = pathLookup.getRoutingPath(procPath, "PUT", APIKEY, VERSION);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterFlowletInstancesLookUp() throws Exception {
    String procPath = "/v2//apps/WordCount/flows/WordCountFlow/flowlets/StreamSource/instances";
    String result = pathLookup.getRoutingPath(procPath, "PUT", APIKEY, VERSION);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

}
