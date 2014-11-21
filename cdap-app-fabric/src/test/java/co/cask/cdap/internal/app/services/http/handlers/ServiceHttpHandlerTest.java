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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.AppWithServices;
import co.cask.cdap.gateway.handlers.ServiceHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.ProgramRecord;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link ServiceHttpHandler}.
 */
public class ServiceHttpHandlerTest extends AppFabricTestBase {

  @Test
  public void testAllServices() throws Exception {
    deploy(AppWithServices.class);

    HttpResponse response = doGet("/v2/services");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Type typeToken = new TypeToken<List<ProgramRecord>>() { }.getType();
    List<ProgramRecord> programRecords = readResponse(response, typeToken);

    Assert.assertEquals(1, programRecords.size());
    Assert.assertEquals("NoOpService", programRecords.get(0).getId());
  }

  @Test
  public void testServices() throws Exception {
    deploy(AppWithServices.class);

    HttpResponse response = doGet("/v2/apps/AppWithServices/services");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Type typeToken = new TypeToken<List<JsonObject>>() { }.getType();
    List<JsonObject> returnedBody = readResponse(response, typeToken);

    // One service exists in AppWithServices
    Assert.assertEquals(1, returnedBody.size());
    Assert.assertEquals("NoOpService", returnedBody.get(0).get("name").getAsString());
    Assert.assertEquals("Service", returnedBody.get(0).get("type").getAsString());

    // Verify that a non-existent app returns a 404
    response = doGet("/v2/apps/nonexistentAppName/services");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testChangingInstances() throws Exception {
    deploy(AppWithServices.class);

    //Test invalid app name, invalid service name, and invalid runnable name:
    HttpResponse response = doGet("/v2/apps/invalidApp/services/NoOpService/runnables/NoOpService/instances");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = doGet("/v2/apps/AppWithServices/services/InvalidService/runnables/NoOpService/instances");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = doGet("/v2/apps/AppWithServices/services/NoOpService/runnables/InvalidRunnable/instances");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    //Set instances to numRequested, and then check with a get that the instances were indeed set.
    String instancesUrl = "/v2/apps/AppWithServices/services/NoOpService/runnables/NoOpService/instances";
    String numRequested = "13";

    JsonObject jsonData = new JsonObject();
    jsonData.addProperty("instances", numRequested);
    response = doPut(instancesUrl, jsonData.toString());
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet(instancesUrl);
    Type typeToken = new TypeToken<Map<String, String>>() { }.getType();
    Map<String, String> returnedBody = readResponse(response, typeToken);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals(numRequested, returnedBody.get("requested"));
  }
}
