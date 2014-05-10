package com.continuuity.gateway.handlers;

import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.gateway.apps.HighPassFilterApp;
import com.google.gson.JsonObject;
import junit.framework.Assert;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Test;

/**
 * Tests the runtime args - setting it through runtimearg API and Program start API
 */
public class RuntimeArgumentTest {
  @Test
  public void testFlowRuntimeArgs() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.deploy(HighPassFilterApp.class, "HighPassFilterApp");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    //Set flow runtime arg threshold to 30
    JsonObject json = new JsonObject();
    json.addProperty("threshold", "30");
    response = GatewayFastTestsSuite.doPut("/v2/apps/HighPassFilterApp/flows/FilterFlow/runtimeargs", json.toString());
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    response = GatewayFastTestsSuite.doPost("/v2/apps/HighPassFilterApp/flows/FilterFlow/start", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    response = GatewayFastTestsSuite.doPost("/v2/apps/HighPassFilterApp/procedures/Count/start", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    //Send two values - 25 and 35; Since threshold is 30, expected count is 1
    response = GatewayFastTestsSuite.doPost("/v2/streams/inputvalue", "25");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    response = GatewayFastTestsSuite.doPost("/v2/streams/inputvalue", "35");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    response = GatewayFastTestsSuite.doPost("/v2/apps/HighPassFilterApp/procedures/Count/methods/result", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    String count = EntityUtils.toString(response.getEntity());
    Assert.assertEquals(count, "1");

    //Now modify the threshold to 50
    json.addProperty("threshold", "50");
    response = GatewayFastTestsSuite.doPut("/v2/apps/HighPassFilterApp/flows/FilterFlow/runtimeargs", json.toString());
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    //Stop and start the flow and it should pick up the new threshold value; Verify that by sending 45 and 55
    //Then count should be 2
    response = GatewayFastTestsSuite.doPost("/v2/apps/HighPassFilterApp/flows/FilterFlow/stop", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    response = GatewayFastTestsSuite.doPost("/v2/apps/HighPassFilterApp/flows/FilterFlow/start", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    response = GatewayFastTestsSuite.doPost("/v2/streams/inputvalue", "45");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    response = GatewayFastTestsSuite.doPost("/v2/streams/inputvalue", "55");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    response = GatewayFastTestsSuite.doPost("/v2/apps/HighPassFilterApp/procedures/Count/methods/result", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    count = EntityUtils.toString(response.getEntity());
    Assert.assertEquals(count, "2");

    //Now stop the flow and update the threshold value during the start POST call to 100
    //Test it by sending 95 and 105 and the count should be 3
    response = GatewayFastTestsSuite.doPost("/v2/apps/HighPassFilterApp/flows/FilterFlow/stop", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    json.addProperty("threshold", "100");
    response = GatewayFastTestsSuite.doPost("/v2/apps/HighPassFilterApp/flows/FilterFlow/start", json.toString());
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    response = GatewayFastTestsSuite.doPost("/v2/streams/inputvalue", "95");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    response = GatewayFastTestsSuite.doPost("/v2/streams/inputvalue", "105");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    response = GatewayFastTestsSuite.doPost("/v2/apps/HighPassFilterApp/procedures/Count/methods/result", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    count = EntityUtils.toString(response.getEntity());
    Assert.assertEquals(count, "3");

    //Stop all flows and procedures and reset the state of the reactor
    response = GatewayFastTestsSuite.doPost("/v2/apps/HighPassFilterApp/flows/FilterFlow/stop", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    response = GatewayFastTestsSuite.doPost("/v2/apps/HighPassFilterApp/procedures/Count/stop", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    response = GatewayFastTestsSuite.doDelete("/v2/apps/HighPassFilterApp");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
  }
}
