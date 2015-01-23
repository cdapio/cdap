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

package co.cask.cdap.client;

import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.http.ServiceHttpEndpoint;
import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.app.PingService;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ServiceClient}.
 */
@Category(XSlowTests.class)
public class ServiceClientTestRun extends ClientTestBase {
  private ServiceClient serviceClient;
  private ProgramClient programClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();

    ApplicationClient appClient = new ApplicationClient(clientConfig);
    serviceClient = new ServiceClient(clientConfig);
    programClient = new ProgramClient(clientConfig);

    appClient.deploy(createAppJarFile(FakeApp.class));
    programClient.start(FakeApp.NAME, ProgramType.SERVICE, PingService.NAME);
    assertProgramRunning(programClient, FakeApp.NAME, ProgramType.SERVICE, PingService.NAME);
  }

  @After
  public void tearDown() throws Exception {
    programClient.stop(FakeApp.NAME, ProgramType.SERVICE, PingService.NAME);
    assertProgramStopped(programClient, FakeApp.NAME, ProgramType.SERVICE, PingService.NAME);
  }

  @Test
  public void testGetServiceSpecification() throws Exception {
    ServiceSpecification serviceSpecification = serviceClient.get(FakeApp.NAME, PingService.NAME);
    assertEquals(serviceSpecification.getName(), PingService.NAME);
    assertEquals(serviceSpecification.getHandlers().size(), 1);
    assertEquals(serviceSpecification.getWorkers().size(), 0);
  }

  @Test
  public void testGetEndpoints() throws Exception {
    List<ServiceHttpEndpoint> endpoints = serviceClient.getEndpoints(FakeApp.NAME, PingService.NAME);
    assertEquals(1, endpoints.size());
    ServiceHttpEndpoint endpoint = endpoints.get(0);
    assertEquals("GET", endpoint.getMethod());
    assertEquals("/ping", endpoint.getPath());
  }

  @Test
  public void testGetServiceURL() throws Exception {
    URL url = new URL(serviceClient.getServiceURL(FakeApp.NAME, PingService.NAME), "ping");
    HttpRequest request = HttpRequest.builder(HttpMethod.GET, url).build();
    HttpResponse response = HttpRequests.execute(request);
    assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
  }
}
