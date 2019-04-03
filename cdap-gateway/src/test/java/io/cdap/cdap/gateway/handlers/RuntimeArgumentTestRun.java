/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.gateway.GatewayFastTestsSuite;
import io.cdap.cdap.gateway.GatewayTestBase;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Tests the runtime args - setting it through runtimearg API and Program start API
 */
public class RuntimeArgumentTestRun extends GatewayTestBase {

  private static final Gson GSON = new Gson();
  private static final Type ARGS_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  /**
   * Test app for testing runtime arguments
   */
  public static final class RuntimeArgumentTestApp extends AbstractApplication {

    @Override
    public void configure() {
      addService("TestService", new RuntimeArgumentTestHandler());
    }
  }

  /**
   * Service handler for testing runtime arguments
   */
  public static final class RuntimeArgumentTestHandler extends AbstractHttpServiceHandler {

    @Path("/getargs")
    @GET
    public void getArgs(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendString(GSON.toJson(getContext().getRuntimeArguments(), ARGS_TYPE));
    }
  }


  @Test
  public void testServiceRuntimeArgs() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.deploy(RuntimeArgumentTestApp.class, TEMP_FOLDER.newFolder());
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());

    // Set some runtime arguments.
    Map<String, String> args = new HashMap<>();
    args.put("arg1", "1");
    args.put("arg2", "2");
    response = GatewayFastTestsSuite.doPut(
      "/v3/namespaces/default/apps/RuntimeArgumentTestApp/services/TestService/runtimeargs", GSON.toJson(args));
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());

    // Start the service with extra and overwritten runtime arguments
    Map<String, String> startArgs = new HashMap<>();
    startArgs.put("arg2", "20");
    startArgs.put("arg3", "3");
    response = GatewayFastTestsSuite.doPost(
      "/v3/namespaces/default/apps/RuntimeArgumentTestApp/services/TestService/start", GSON.toJson(startArgs));
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());

    // Check the service status. Make sure it is running before querying it
    waitForProgramRuns("services", "RuntimeArgumentTestApp", "TestService", ProgramRunStatus.RUNNING, 1);

    // Query for runtime arguments from the service
    response = GatewayFastTestsSuite.doGet(
      "/v3/namespaces/default/apps/RuntimeArgumentTestApp/services/TestService/methods/getargs", null);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());
    Map<String, String> queryArgs = GSON.fromJson(EntityUtils.toString(response.getEntity()), ARGS_TYPE);

    Map<String, String> expectedArgs = new HashMap<>(args);
    expectedArgs.putAll(startArgs);
    // Runtime argument received by the service could have more entries then the one being set (e.g. logical start time)
    // Hence we only check for keys that are set with the correct value
    Assert.assertTrue(expectedArgs.entrySet().stream()
                        .allMatch(e -> Objects.equals(e.getValue(), queryArgs.get(e.getKey()))));

    // Stop the service
    response = GatewayFastTestsSuite.doPost(
      "/v3/namespaces/default/apps/RuntimeArgumentTestApp/services/TestService/stop", null);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());

    waitForProgramRuns("services", "RuntimeArgumentTestApp", "TestService", ProgramRunStatus.KILLED, 1);
  }
}
