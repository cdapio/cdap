package com.continuuity.gateway.router;

import com.continuuity.common.conf.Constants;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *  To test the RouterPathLookup regular expression tests.
 */
public class RouterPathTest {

  @Test
  public void testRouterFlowPathLookUp() throws Exception {
    String flowPath = "/v2//apps/ResponseCodeAnalytics/flows/LogAnalyticsFlow/status";
    String result = RouterPathLookup.getRoutingPath(flowPath, "GET");
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testRouterWorkFlowPathLookUp() throws Exception {
    String procPath = "/v2/apps/PurchaseHistory/workflows/PurchaseHistoryWorkflow/status";
    String result = RouterPathLookup.getRoutingPath(procPath, "GET");
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterProcedurePathLookUp() throws Exception {
    String procPath = "/v2//apps/ResponseCodeAnalytics/procedures/StatusCodeProcedure/status";
    String result = RouterPathLookup.getRoutingPath(procPath, "GET");
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterDeployPathLookUp() throws Exception {
    String procPath = "/v2//apps/";
    String result = RouterPathLookup.getRoutingPath(procPath, "PUT");
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterFlowletInstancesLookUp() throws Exception {
    String procPath = "/v2//apps/WordCount/flows/WordCountFlow/flowlets/StreamSource/instances";
    String result = RouterPathLookup.getRoutingPath(procPath, "PUT");
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }
}
