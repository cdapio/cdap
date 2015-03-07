/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.gateway.GatewayFastTestsSuite;
import co.cask.cdap.gateway.GatewayTestBase;
import co.cask.cdap.gateway.apps.HighPassFilterApp;
import com.google.gson.JsonObject;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests the runtime args - setting it through runtimearg API and Program start API
 */
public class RuntimeArgumentTestRun extends GatewayTestBase {

  @Test
  public void testFlowRuntimeArgs() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.deploy(HighPassFilterApp.class, "HighPassFilterApp");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    //Set flow runtime arg threshold to 30
    JsonObject json = new JsonObject();
    json.addProperty("threshold", "30");
    response = GatewayFastTestsSuite.doPut(PREFIX + "/apps/HighPassFilterApp/flows/FilterFlow/runtimeargs",
                                           json.toString());
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    response = GatewayFastTestsSuite.doPost(PREFIX + "/apps/HighPassFilterApp/flows/FilterFlow/start", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    response = GatewayFastTestsSuite.doPost(PREFIX + "/apps/HighPassFilterApp/procedures/Count/start", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    //Send two values - 25 and 35; Since threshold is 30, expected count is 1
    response = GatewayFastTestsSuite.doPost(PREFIX + "/streams/inputvalue", "25");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    response = GatewayFastTestsSuite.doPost(PREFIX + "/streams/inputvalue", "35");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    // Check the procedure status. Make sure it is running before querying it
    waitState("procedures", "HighPassFilterApp", "Count", "RUNNING");

    // Check the count. Gives it couple trials as it takes time for flow to process and write to the table
    checkCount("1");

    //Now modify the threshold to 50
    json.addProperty("threshold", "50");
    response = GatewayFastTestsSuite.doPut(PREFIX + "/apps/HighPassFilterApp/flows/FilterFlow/runtimeargs",
                                           json.toString());
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    //Stop and start the flow and it should pick up the new threshold value; Verify that by sending 45 and 55
    //Then count should be 2
    response = GatewayFastTestsSuite.doPost(PREFIX + "/apps/HighPassFilterApp/flows/FilterFlow/stop", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    waitState("flows", "HighPassFilterApp", "FilterFlow", "STOPPED");

    response = GatewayFastTestsSuite.doPost(PREFIX + "/apps/HighPassFilterApp/flows/FilterFlow/start", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    response = GatewayFastTestsSuite.doPost(PREFIX + "/streams/inputvalue", "45");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    response = GatewayFastTestsSuite.doPost(PREFIX + "/streams/inputvalue", "55");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    // Check the count. Gives it couple trials as it takes time for flow to process and write to the table
    checkCount("2");

    //Now stop the flow and update the threshold value during the start POST call to 100
    //Test it by sending 95 and 105 and the count should be 3
    response = GatewayFastTestsSuite.doPost(PREFIX + "/apps/HighPassFilterApp/flows/FilterFlow/stop", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    waitState("flows", "HighPassFilterApp", "FilterFlow", "STOPPED");

    json.addProperty("threshold", "100");
    response = GatewayFastTestsSuite.doPost(PREFIX + "/apps/HighPassFilterApp/flows/FilterFlow/start",
                                            json.toString());
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    response = GatewayFastTestsSuite.doPost(PREFIX + "/streams/inputvalue", "95");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    response = GatewayFastTestsSuite.doPost(PREFIX + "/streams/inputvalue", "105");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    // Check the count. Gives it couple trials as it takes time for flow to process and write to the table
    checkCount("3");

    //Stop all flows and procedures and reset the state of the cdap
    response = GatewayFastTestsSuite.doPost(PREFIX + "/apps/HighPassFilterApp/flows/FilterFlow/stop", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
    response = GatewayFastTestsSuite.doPost(PREFIX + "/apps/HighPassFilterApp/procedures/Count/stop", null);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());

    // Wait for program states. Make sure they are stopped before deletion
    waitState("flows", "HighPassFilterApp", "FilterFlow", "STOPPED");
    waitState("procedures", "HighPassFilterApp", "Count", "STOPPED");

    response = GatewayFastTestsSuite.doDelete(PREFIX + "/apps/HighPassFilterApp");
    Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.getCode());
  }

  private void checkCount(String expected) throws Exception {
    int trials = 0;
    while (trials++ < 5) {
      HttpResponse response = GatewayFastTestsSuite.doPost(
        PREFIX + "/apps/HighPassFilterApp/procedures/Count/methods/result", null);
      if (response.getStatusLine().getStatusCode() == HttpResponseStatus.OK.getCode()) {
        String count = EntityUtils.toString(response.getEntity());
        if (expected.equals(count)) {
          break;
        }
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(trials < 5);
  }
}
