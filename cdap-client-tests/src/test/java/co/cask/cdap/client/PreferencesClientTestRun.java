/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.client.app.AppReturnsArgs;
import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.FlowId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.cdap.test.XSlowTests;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link PreferencesClient}
 */
@Category(XSlowTests.class)
public class PreferencesClientTestRun extends ClientTestBase {

  private static final Gson GSON = new Gson();
  private static final ApplicationId FAKE_APP_ID = NamespaceId.DEFAULT.app(FakeApp.NAME);
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private PreferencesClient client;
  private ApplicationClient appClient;
  private ServiceClient serviceClient;
  private ProgramClient programClient;
  private NamespaceClient namespaceClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    client = new PreferencesClient(clientConfig);
    appClient = new ApplicationClient(clientConfig);
    serviceClient = new ServiceClient(clientConfig);
    programClient = new ProgramClient(clientConfig);
    namespaceClient = new NamespaceClient(clientConfig);
  }

  @Test
  public void testProgramAPI() throws Exception {
    Map<String, String> propMap = Maps.newHashMap();
    propMap.put("key", "instance");
    File jarFile = createAppJarFile(AppReturnsArgs.class);
    appClient.deploy(NamespaceId.DEFAULT, jarFile);
    ApplicationId app = NamespaceId.DEFAULT.app(AppReturnsArgs.NAME);
    ServiceId service = app.service(AppReturnsArgs.SERVICE);

    try {
      client.setInstancePreferences(propMap);
      Map<String, String> setMap = Maps.newHashMap();
      setMap.put("saved", "args");

      programClient.setRuntimeArgs(service, setMap);
      assertEquals(setMap, programClient.getRuntimeArgs(service));

      propMap.put("run", "value");
      propMap.put("logical.start.time", "1234567890000");
      propMap.putAll(setMap);

      programClient.start(service, false, propMap);
      assertProgramRunning(programClient, service);

      URL serviceURL = new URL(serviceClient.getServiceURL(service), AppReturnsArgs.ENDPOINT);
      HttpResponse response = getServiceResponse(serviceURL);
      assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
      Map<String, String> responseMap = GSON.fromJson(response.getResponseBodyAsString(), STRING_MAP_TYPE);
      assertEquals(propMap, responseMap);
      programClient.stop(service);
      assertProgramStopped(programClient, service);

      long minStartTime = System.currentTimeMillis();
      client.deleteInstancePreferences();
      programClient.start(service);
      assertProgramRunning(programClient, service);
      propMap.remove("key");
      propMap.remove("run");
      propMap.remove("logical.start.time");

      serviceURL = new URL(serviceClient.getServiceURL(service), AppReturnsArgs.ENDPOINT);
      response = getServiceResponse(serviceURL);
      responseMap = GSON.fromJson(response.getResponseBodyAsString(), STRING_MAP_TYPE);
      long actualStartTime = Long.parseLong(responseMap.remove("logical.start.time"));
      Assert.assertTrue(actualStartTime >= minStartTime);
      assertEquals(propMap, responseMap);
      programClient.stop(service);
      assertProgramStopped(programClient, service);

      propMap.clear();
      minStartTime = System.currentTimeMillis();
      programClient.setRuntimeArgs(service, propMap);
      programClient.start(service);
      assertProgramRunning(programClient, service);
      serviceURL = new URL(serviceClient.getServiceURL(service), AppReturnsArgs.ENDPOINT);
      response = getServiceResponse(serviceURL);
      assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
      responseMap = GSON.fromJson(response.getResponseBodyAsString(), STRING_MAP_TYPE);
      actualStartTime = Long.parseLong(responseMap.remove("logical.start.time"));
      Assert.assertTrue(actualStartTime >= minStartTime);
      assertEquals(propMap, responseMap);
    } finally {
      programClient.stop(service);
      assertProgramStopped(programClient, service);
      appClient.delete(app);
    }
  }

  private HttpResponse getServiceResponse(URL serviceURL) throws IOException, InterruptedException {
    int iterations = 0;
    HttpResponse response;
    do {
      response = HttpRequests.execute(HttpRequest.builder(HttpMethod.GET, serviceURL).build());
      if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
        return response;
      }
      TimeUnit.MILLISECONDS.sleep(50);
      iterations++;
    } while (iterations <= 100);
    return response;
  }

  @Test
  public void testPreferences() throws Exception {
    NamespaceId invalidNamespace = new NamespaceId("invalid");
    namespaceClient.create(new NamespaceMeta.Builder().setName(invalidNamespace).build());

    Map<String, String> propMap = client.getInstancePreferences();
    Assert.assertEquals(ImmutableMap.<String, String>of(), propMap);
    propMap.put("k1", "instance");
    client.setInstancePreferences(propMap);
    Assert.assertEquals(propMap, client.getInstancePreferences());

    File jarFile = createAppJarFile(FakeApp.class);
    appClient.deploy(NamespaceId.DEFAULT, jarFile);

    try {
      propMap.put("k1", "namespace");
      client.setNamespacePreferences(NamespaceId.DEFAULT, propMap);
      Assert.assertEquals(propMap, client.getNamespacePreferences(NamespaceId.DEFAULT, true));
      Assert.assertEquals(propMap, client.getNamespacePreferences(NamespaceId.DEFAULT, false));
      Assert.assertTrue(client.getNamespacePreferences(invalidNamespace, false).isEmpty());
      Assert.assertEquals("instance", client.getNamespacePreferences(invalidNamespace, true).get("k1"));

      client.deleteNamespacePreferences(NamespaceId.DEFAULT);
      propMap.put("k1", "instance");
      Assert.assertEquals(propMap, client.getNamespacePreferences(NamespaceId.DEFAULT, true));
      Assert.assertEquals(ImmutableMap.<String, String>of(),
                          client.getNamespacePreferences(NamespaceId.DEFAULT, false));

      propMap.put("k1", "namespace");
      client.setNamespacePreferences(NamespaceId.DEFAULT, propMap);
      Assert.assertEquals(propMap, client.getNamespacePreferences(NamespaceId.DEFAULT, true));
      Assert.assertEquals(propMap, client.getNamespacePreferences(NamespaceId.DEFAULT, false));

      propMap.put("k1", "application");
      client.setApplicationPreferences(FAKE_APP_ID, propMap);
      Assert.assertEquals(propMap, client.getApplicationPreferences(FAKE_APP_ID, true));
      Assert.assertEquals(propMap, client.getApplicationPreferences(FAKE_APP_ID, false));

      propMap.put("k1", "program");
      FlowId flow = FAKE_APP_ID.flow(FakeApp.FLOWS.get(0));
      client.setProgramPreferences(flow, propMap);
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, true));
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, false));
      client.deleteProgramPreferences(flow);

      propMap.put("k1", "application");
      Assert.assertTrue(client.getProgramPreferences(flow, false).isEmpty());
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, true));

      client.deleteApplicationPreferences(FAKE_APP_ID);

      propMap.put("k1", "namespace");
      Assert.assertTrue(client.getApplicationPreferences(FAKE_APP_ID, false).isEmpty());
      Assert.assertEquals(propMap, client.getApplicationPreferences(FAKE_APP_ID, true));
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, true));

      client.deleteNamespacePreferences(NamespaceId.DEFAULT);
      propMap.put("k1", "instance");
      Assert.assertTrue(client.getNamespacePreferences(NamespaceId.DEFAULT, false).isEmpty());
      Assert.assertEquals(propMap, client.getNamespacePreferences(NamespaceId.DEFAULT, true));
      Assert.assertEquals(propMap, client.getApplicationPreferences(FAKE_APP_ID, true));
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, true));

      client.deleteInstancePreferences();
      propMap.clear();
      Assert.assertEquals(propMap, client.getInstancePreferences());
      Assert.assertEquals(propMap, client.getNamespacePreferences(NamespaceId.DEFAULT, true));
      Assert.assertEquals(propMap, client.getNamespacePreferences(NamespaceId.DEFAULT, true));
      Assert.assertEquals(propMap, client.getApplicationPreferences(FAKE_APP_ID, true));
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, true));


      //Test Deleting Application
      propMap.put("k1", "application");
      client.setApplicationPreferences(FAKE_APP_ID, propMap);
      Assert.assertEquals(propMap, client.getApplicationPreferences(FAKE_APP_ID, false));

      propMap.put("k1", "program");
      client.setProgramPreferences(flow, propMap);
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, false));

      appClient.delete(FAKE_APP_ID);
      // deleting the app should have deleted the preferences that were stored. so deploy the app and check
      // if the preferences are empty. we need to deploy the app again since getting preferences of non-existent apps
      // is not allowed by the API.
      appClient.deploy(NamespaceId.DEFAULT, jarFile);
      propMap.clear();
      Assert.assertEquals(propMap, client.getApplicationPreferences(FAKE_APP_ID, false));
      Assert.assertEquals(propMap, client.getProgramPreferences(flow, false));
    } finally {
      try {
        appClient.delete(FAKE_APP_ID);
      } catch (ApplicationNotFoundException e) {
        // ok if this happens, means its already deleted.
      }
      namespaceClient.delete(invalidNamespace);
    }
  }

  @Test
  public void testDeletingNamespace() throws Exception {
    Map<String, String> propMap = Maps.newHashMap();
    propMap.put("k1", "namespace");

    NamespaceId myspace = new NamespaceId("myspace");
    namespaceClient.create(new NamespaceMeta.Builder().setName(myspace).build());

    client.setNamespacePreferences(myspace, propMap);
    Assert.assertEquals(propMap, client.getNamespacePreferences(myspace, false));
    Assert.assertEquals(propMap, client.getNamespacePreferences(myspace, true));

    namespaceClient.delete(myspace);
    namespaceClient.create(new NamespaceMeta.Builder().setName(myspace).build());
    Assert.assertTrue(client.getNamespacePreferences(myspace, false).isEmpty());
    Assert.assertTrue(client.getNamespacePreferences(myspace, true).isEmpty());

    namespaceClient.delete(myspace);
  }

  @Test(expected = NotFoundException.class)
  public void testInvalidNamespace() throws Exception {
    NamespaceId somespace = new NamespaceId("somespace");
    client.setNamespacePreferences(somespace, ImmutableMap.of("k1", "v1"));
  }

  @Test(expected = NotFoundException.class)
  public void testInvalidApplication() throws Exception {
    ApplicationId someapp = new NamespaceId("somespace").app("someapp");
    client.getApplicationPreferences(someapp, true);
  }

  @Test(expected = ProgramNotFoundException.class)
  public void testInvalidProgram() throws Exception {
    ApplicationId someapp = new NamespaceId("somespace").app("someapp");
    client.deleteProgramPreferences(someapp.flow("myflow"));
  }
}
