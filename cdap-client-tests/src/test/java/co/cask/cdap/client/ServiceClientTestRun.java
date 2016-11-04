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
import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.app.PingService;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.XSlowTests;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
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

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ServiceClient}.
 */
@Category(XSlowTests.class)
public class ServiceClientTestRun extends ClientTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceClientTestRun.class);

  private final Id.Namespace namespace = Id.Namespace.DEFAULT;
  private final Id.Application app = Id.Application.from(namespace, FakeApp.NAME);
  private final Id.Service service = Id.Service.from(app, PingService.NAME);

  private ServiceClient serviceClient;
  private ProgramClient programClient;
  private ApplicationClient appClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();

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
}
