/*
 * Copyright © 2019 Cask Data, Inc.
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
 *
 */

package co.cask.cdap.service;

import co.cask.cdap.api.artifact.ArtifactManager;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.service.function.ConstantFunction;
import co.cask.cdap.service.function.DelegatingFunction;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.gson.Gson;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for the {@link ArtifactManager} from {@link Service}.
 */
public class DynamicPluginServiceTestRun extends TestFrameworkTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);
  private static final Gson GSON = new Gson();

  private ServiceManager serviceManager;
  private URI baseURI;


  @Before
  public void initTest() throws Exception {
    ArtifactId appArtifactId = NamespaceId.DEFAULT.artifact("dynamicPlugin", "1.0.0");
    addAppArtifact(appArtifactId, DynamicPluginServiceApp.class);

    ArtifactId pluginArtifactId = NamespaceId.DEFAULT.artifact("plugins", "1.0.0");
    addPluginArtifact(pluginArtifactId, appArtifactId, ConstantFunction.class, DelegatingFunction.class);
    ApplicationId appId = NamespaceId.DEFAULT.app("dynamicPluginService");
    ArtifactSummary summary = new ArtifactSummary(appArtifactId.getArtifact(), appArtifactId.getVersion());
    AppRequest<Void> appRequest = new AppRequest<>(summary);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    serviceManager = appManager.getServiceManager(DynamicPluginServiceApp.SERVICE_NAME);
    serviceManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 2, TimeUnit.MINUTES);

    baseURI = serviceManager.getServiceURL(1, TimeUnit.MINUTES).toURI();
  }

  @After
  public void cleanupTest() throws Exception {
    serviceManager.stop();
    serviceManager.waitForStopped(2, TimeUnit.MINUTES);
  }

  @Test
  public void testNamespaceIsolation() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("value", "x");
    URL url = baseURI.resolve(String.format("plugins/%s/apply", ConstantFunction.NAME)).toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, url)
      .withBody(GSON.toJson(properties))
      .addHeader(DynamicPluginServiceApp.NAMESPACE_HEADER, "ghost")
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(404, response.getResponseCode());
  }

  @Test
  public void testDynamicPluginSimple() throws Exception {
    // test a single plugin
    Map<String, String> properties = new HashMap<>();
    properties.put("value", "x");
    URL url = baseURI.resolve(String.format("plugins/%s/apply", ConstantFunction.NAME)).toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, url)
      .withBody(GSON.toJson(properties))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("x", response.getResponseBodyAsString());

    // test plugin that uses a plugin
    Map<String, String> delegateProperties = new HashMap<>();
    delegateProperties.put("value", "y");

    properties = new HashMap<>();
    properties.put("delegateName", ConstantFunction.NAME);
    properties.put("properties", GSON.toJson(delegateProperties));
    url = baseURI.resolve(String.format("plugins/%s/apply", DelegatingFunction.NAME)).toURL();
    request = HttpRequest.builder(HttpMethod.POST, url)
      .withBody(GSON.toJson(properties))
      .build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("y", response.getResponseBodyAsString());
  }

  @Test
  public void testDynamicPluginBodyProducer() throws Exception {
    // test that a plugin can be instantiated in the body producer chunk() and onFinish() methods
    // the good plugin should be found, so the response should be 'x'
    DynamicPluginServiceApp.PluginRequest requestBody =
      new DynamicPluginServiceApp.PluginRequest(ConstantFunction.NAME, Collections.singletonMap("value", "x"),
                                                ConstantFunction.NAME, Collections.singletonMap("value", "y"));
    URL producerUrl = baseURI.resolve("producer").toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, producerUrl)
      .withBody(GSON.toJson(requestBody))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("x", response.getResponseBodyAsString());

    URL onFinishUrl = baseURI.resolve("onFinishSuccessful").toURL();
    request = HttpRequest.builder(HttpMethod.GET, onFinishUrl).build();
    response = HttpRequests.execute(request);
    Assert.assertTrue(Boolean.valueOf(response.getResponseBodyAsString()));
  }

  @Test
  public void testDynamicPluginContentConsumer() throws Exception {
    // test that a plugin can be instantiated in the content consumer finish method
    // the good plugin should be found, so the response should be 'x'
    DynamicPluginServiceApp.PluginRequest requestBody =
      new DynamicPluginServiceApp.PluginRequest(ConstantFunction.NAME, Collections.singletonMap("value", "x"),
                                                ConstantFunction.NAME, Collections.singletonMap("value", "y"));
    URL url = baseURI.resolve("consumer").toURL();
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, url)
      .withBody(GSON.toJson(requestBody))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("x", response.getResponseBodyAsString());

    // test that a plugin can be instantiated in the content consumer error method
    // the "good" plugin should not be found, so the response should be 'y'
    requestBody =
      new DynamicPluginServiceApp.PluginRequest("non-existent", Collections.singletonMap("value", "x"),
                                                ConstantFunction.NAME, Collections.singletonMap("value", "y"));
    request = HttpRequest.builder(HttpMethod.POST, url)
      .withBody(GSON.toJson(requestBody))
      .build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(400, response.getResponseCode());
    Assert.assertEquals("y", response.getResponseBodyAsString());
  }
}
