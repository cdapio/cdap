/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.client;

import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.api.service.http.ServiceHttpEndpoint;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.client.app.AppReturnsArgs;
import io.cdap.cdap.client.app.FakeApp;
import io.cdap.cdap.client.app.PingService;
import io.cdap.cdap.client.common.ClientTestBase;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.cdap.test.XSlowTests;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.jar.Manifest;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ServiceClient}.
 */
@Category(XSlowTests.class)
public class ServiceClientTestRun extends ClientTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceClientTestRun.class);

  private final NamespaceId namespace = NamespaceId.DEFAULT;
  private final ApplicationId app = namespace.app(FakeApp.NAME);
  private final ServiceId service = app.service(PingService.NAME);
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
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
  }

  /**
   * Add AppReturnsArgs artifact
   */
  private void addArtifact() throws Exception {
    LocalLocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.BUNDLE_VERSION, "1.0.0");
    final Location appJarLoc = AppJarHelper.createDeploymentJar(locationFactory, AppReturnsArgs.class, manifest);
    artifactClient.add(NamespaceId.DEFAULT, artifactId.getArtifact(),
                       appJarLoc::getInputStream, artifactId.getVersion());
    appJarLoc.delete();
  }

}
