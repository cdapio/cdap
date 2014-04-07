package com.continuuity.gateway.router;

import com.continuuity.common.conf.Constants;
import junit.framework.Assert;
import org.junit.Test;

/**
 *  To test the RouterPathLookup regular expression tests.
 */
public class RouterPathTest {

  @Test
  public void testRouterFlowPathLookUp() throws Exception {
    String flowPath = "/v2//apps/ResponseCodeAnalytics/flows/LogAnalyticsFlow/status";
    String result = RouterPathLookup.getRoutingPath(flowPath);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testRouterWorkFlowPathLookUp() throws Exception {
    String procPath = "/v2/apps/PurchaseHistory/workflows/PurchaseHistoryWorkflow/status";
    String result = RouterPathLookup.getRoutingPath(procPath);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterProcedurePathLookUp() throws Exception {
    String procPath = "/v2//apps/ResponseCodeAnalytics/procedures/StatusCodeProcedure/status";
    String result = RouterPathLookup.getRoutingPath(procPath);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }


}
