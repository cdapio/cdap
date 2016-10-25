/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.http.ServiceHttpEndpoint;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.client.app.AppReturnsArgs;
import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.app.PingService;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.XSlowTests;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.InputSupplier;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.Manifest;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ServiceClient}.
 */
@Category(XSlowTests.class)
public class ServiceClientTestRun extends ClientTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceClientTestRun.class);
  private static final Gson GSON = new Gson();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final Id.Namespace namespace = Id.Namespace.DEFAULT;
  private final Id.Application app = Id.Application.from(namespace, FakeApp.NAME);
  private final Id.Service service = Id.Service.from(app, PingService.NAME);
  private static final ServiceId serviceId = new ServiceId(NamespaceId.DEFAULT.getNamespace(), AppReturnsArgs.NAME,
                                                           AppReturnsArgs.SERVICE);
  private static final ArtifactId artifactId = NamespaceId.DEFAULT.artifact("appreturnsArgs", "1.0.0-SNAPSHOT");

  private ArtifactClient artifactClient;
  private ServiceClient serviceClient;
  private ProgramClient programClient;
  private ApplicationClient appClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();

    artifactClient = new ArtifactClient(clientConfig);
    appClient = new ApplicationClient(clientConfig);
    serviceClient = new ServiceClient(clientConfig);
    programClient = new ProgramClient(clientConfig);
    try {
      serviceClient.checkAvailability(service);
      Assert.fail();
    } catch (NotFoundException ex) {
      // Expected since the app has not been deployed yet
    }

    appClient.deploy(namespace, createAppJarFile(FakeApp.class));
    try {
      serviceClient.checkAvailability(service);
      Assert.fail();
    } catch (ServiceUnavailableException ex) {
      // Expected since the service has not been started
    }
    programClient.start(service);
    assertProgramRunning(programClient, service);
    addArtifact();
  }

  @After
  public void tearDown() throws Throwable {
    programClient.stop(service);
    assertProgramStopped(programClient, service);
    try {
      appClient.delete(app);
    } catch (Exception e) {
      LOG.error("Error deleting app {} during test cleanup.", e);
    }
  }

  @Test
  public void testGetServiceSpecification() throws Exception {
    ServiceSpecification serviceSpecification = serviceClient.get(service);
    assertEquals(serviceSpecification.getName(), PingService.NAME);
    assertEquals(serviceSpecification.getHandlers().size(), 1);
  }

  @Test
  public void testGetEndpoints() throws Exception {
    List<ServiceHttpEndpoint> endpoints = serviceClient.getEndpoints(service);
    assertEquals(1, endpoints.size());
    ServiceHttpEndpoint endpoint = endpoints.get(0);
    assertEquals("GET", endpoint.getMethod());
    assertEquals("/ping", endpoint.getPath());
  }

  @Test
  public void testActiveStatus() throws Exception {
    serviceClient.checkAvailability(service);
  }

  @Test
  public void testGetServiceURL() throws Exception {
    URL url = new URL(serviceClient.getServiceURL(service), "ping");
    HttpRequest request = HttpRequest.builder(HttpMethod.GET, url).build();
    HttpResponse response = HttpRequests.execute(request);
    assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
  }

  @Test
  public void testRouteConfig() throws Exception {
    // Deploy AppReturnsArgs of version v1 and start its service
    String version1 = "v1";
    ApplicationId app1 = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), AppReturnsArgs.NAME, version1);
    deployApp(app1);
    Map<String, String> propMap = new HashMap<>();
    propMap.put("version", version1);
    propMap.put("logical.start.time", "1234567890000");
    startService(app1, propMap);

    // RouteConfig is empty by default
    Assert.assertTrue(serviceClient.getRouteConfig(serviceId).isEmpty());

    // Calls to the non-versioned service endpoint are routed to the only running service of version v1
    assertArgsEqual(propMap);

    // Deploy AppReturnsArgs of version v2 and start its service
    String version2 = "v2";
    ApplicationId app2 = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), AppReturnsArgs.NAME, version2);
    deployApp(app2);
    propMap.put("version", version2);
    startService(app2, propMap);

    // Storing and getting RouteConfig with total percentage equal to 100 will succeed
    storeAndGetValidRouteConfig(ImmutableMap.of(version1, 10, version2, 90));
    storeAndGetValidRouteConfig(ImmutableMap.of(version1, 20, version2, 80));
    storeAndGetValidRouteConfig(ImmutableMap.of(version1, 60, version2, 40));

    // Route all calls to the non-versioned service endpoint are routed to the service of v2
    storeAndGetValidRouteConfig(ImmutableMap.of(version1, 0, version2, 100));
    assertArgsEqual(propMap);

    // Setting RouteConfig with total percentage not equal to 100 will fail
    storeInvalidRouteConfig(ImmutableMap.of(version1, 100, version2, 1), "Percentage");
    // Setting RouteConfig with total percentage not equal to 100 will fail
    storeInvalidRouteConfig(ImmutableMap.of(version1, 98, version2, 1), "Percentage");
    // Setting RouteConfig with non-existing version will fail
    storeInvalidRouteConfig(ImmutableMap.of(version1, 100, "NONEXISTING", 0), "NONEXISTING");

    // Delete RouteConfig
    serviceClient.deleteRouteConfig(serviceId);
    // RouteConfig is empty after deletion
    Assert.assertTrue(serviceClient.getRouteConfig(serviceId).isEmpty());

    stopService(app1);
    stopService(app2);
    appClient.delete(app1);
    appClient.delete(app2);
  }

  /**
   * Deploy app of given version
   */
  private void deployApp(ApplicationId app) throws Exception {
    AppRequest createRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()));
    appClient.deploy(app, createRequest);
  }

  /**
   * Start the ArgService of the AppReturnsArgs app
   */
  private void startService(ApplicationId app, Map<String, String> propMap) throws Exception {
    ServiceId service = app.service(AppReturnsArgs.SERVICE);
    programClient.start(service, false, propMap);
    assertProgramRunning(programClient, service);
  }

  /**
   * Stop the ArgService of the AppReturnsArgs app
   */
  private void stopService(ApplicationId app) throws Exception {
    ServiceId service = app.service(AppReturnsArgs.SERVICE);
    programClient.stop(service);
    assertProgramStopped(programClient, service);
  }

  /**
   * Add AppReturnsArgs artifact
   */
  private void addArtifact() throws Exception {
    LocalLocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.BUNDLE_VERSION, "1.0.0");
    final Location appJarLoc = AppJarHelper.createDeploymentJar(locationFactory, AppReturnsArgs.class, manifest);
    InputSupplier<InputStream> inputSupplier = new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return appJarLoc.getInputStream();
      }
    };
    artifactClient.add(Id.Namespace.DEFAULT, artifactId.getArtifact(), inputSupplier, artifactId.getVersion());
    appJarLoc.delete();
  }

  private void assertArgsEqual(Map<String, String> expectedArgs)
    throws UnauthorizedException, IOException, UnauthenticatedException {
    HttpResponse response = serviceClient.callServiceMethod(serviceId, AppReturnsArgs.ENDPOINT);
    Map<String, String> responseMap = GSON.fromJson(response.getResponseBodyAsString(), STRING_MAP_TYPE);
    Assert.assertEquals(expectedArgs, responseMap);
  }

  private void storeAndGetValidRouteConfig(Map<String, Integer> routeConfig) throws Exception {
    serviceClient.storeRouteConfig(serviceId, routeConfig);
    Assert.assertEquals(routeConfig, serviceClient.getRouteConfig(serviceId));
  }

  private void storeInvalidRouteConfig(Map<String, Integer> routeConfig, String expectedMsg) throws Exception {
    try {
      serviceClient.storeRouteConfig(serviceId, routeConfig);
    } catch (IOException expected) {
      Assert.assertTrue(expected.getMessage().contains(expectedMsg));
    }
  }
}
