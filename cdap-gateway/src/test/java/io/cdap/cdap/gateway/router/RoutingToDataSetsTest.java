/*
 * Copyright © 2014-2021 Cask Data, Inc.
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

package io.cdap.cdap.gateway.router;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.net.InetAddresses;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.internal.guice.AppFabricTestModule;
import io.cdap.cdap.security.auth.UserIdentityExtractor;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.guice.ExternalAuthenticationModule;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * TODO: Eventually this can be removed, since we do not have any proxy rules anymore for datasets.
 */
public class RoutingToDataSetsTest {
  private static NettyRouter nettyRouter;
  private static MockHttpService mockService;
  private static int port;

  @BeforeClass
  public static void before() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(new CoreSecurityRuntimeModule().getInMemoryModules(),
                                             new ExternalAuthenticationModule(),
                                             new InMemoryDiscoveryModule(),
                                             new AppFabricTestModule(cConf));

    // Starting router
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    UserIdentityExtractor userIdentityExtractor = injector.getInstance(UserIdentityExtractor.class);

    SConfiguration sConf = SConfiguration.create();
    cConf.set(Constants.Router.ADDRESS, "localhost");
    port = Networks.getRandomPort();

    cConf.setInt(Constants.Router.ROUTER_PORT, port);
    nettyRouter = new NettyRouter(cConf, sConf, InetAddresses.forString("127.0.0.1"),
                                  new RouterServiceLookup(cConf, discoveryServiceClient, new RouterPathLookup()),
                                  new SuccessTokenValidator(), userIdentityExtractor, discoveryServiceClient);
    nettyRouter.startAndWait();

    // Starting mock DataSet service
    DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);
    mockService = new MockHttpService(discoveryService, Constants.Service.DATASET_MANAGER,
                                      new MockDatasetTypeHandler(), new MockDatasetInstanceHandler());
    mockService.startAndWait();
  }

  @AfterClass
  public static void after() {
    try {
      nettyRouter.stopAndWait();
    } finally {
      mockService.stopAndWait();
    }
  }

  @Test
  public void testTypeHandlerRequests() throws Exception {
    Assert.assertEquals("listModules", doRequest("/namespaces/myspace/data/modules", "GET"));
    Assert.assertEquals("post:myModule", doRequest("/namespaces/myspace/data/modules/myModule", "POST"));
    Assert.assertEquals("delete:myModule", doRequest("/namespaces/myspace/data/modules/myModule", "DELETE"));
    Assert.assertEquals("get:myModule", doRequest("/namespaces/myspace/data/modules/myModule", "GET"));
    Assert.assertEquals("listTypes", doRequest("/namespaces/myspace/data/types", "GET"));
    Assert.assertEquals("getType:myType", doRequest("/namespaces/myspace/data/types/myType", "GET"));
  }

  @Test
  public void testInstanceHandlerRequests() throws Exception {
    Assert.assertEquals("list", doRequest("/namespaces/myspace/data/datasets", "GET"));
    Assert.assertEquals("post:myInstance",
                        doRequest("/namespaces/myspace/data/datasets/myInstance", "POST"));
    Assert.assertEquals("delete:myInstance",
                        doRequest("/namespaces/myspace/data/datasets/myInstance", "DELETE"));
    Assert.assertEquals("get:myInstance",
                        doRequest("/namespaces/myspace/data/datasets/myInstance", "GET"));
  }

  @Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
  public static final class MockDatasetTypeHandler extends AbstractHttpHandler {
    @GET
    @Path("/data/modules")
    public void listModules(HttpRequest request, final HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "listModules");
    }

    @POST
    @Path("/data/modules/{name}")
    public void addModule(HttpRequest request, final HttpResponder responder,
                          @PathParam("name") String name) throws IOException {
      responder.sendString(HttpResponseStatus.OK, "post:" + name);
    }

    @DELETE
    @Path("/data/modules/{name}")
    public void deleteModule(HttpRequest request, final HttpResponder responder, @PathParam("name") String name) {
      responder.sendString(HttpResponseStatus.OK, "delete:" + name);
    }

    @GET
    @Path("/data/modules/{name}")
    public void getModuleInfo(HttpRequest request, final HttpResponder responder, @PathParam("name") String name) {
      responder.sendString(HttpResponseStatus.OK, "get:" + name);
    }

    @GET
    @Path("/data/types")
    public void listTypes(HttpRequest request, final HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "listTypes");
    }

    @GET
    @Path("/data/types/{name}")
    public void getTypeInfo(HttpRequest request, final HttpResponder responder,
                            @PathParam("name") String name) {
      responder.sendString(HttpResponseStatus.OK, "getType:" + name);
    }
  }

  @Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
  public static final class MockDatasetInstanceHandler extends AbstractHttpHandler {
    @GET
    @Path("/data/datasets/")
    public void list(HttpRequest request, final HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "list");
    }

    @GET
    @Path("/data/datasets/{instance-name}")
    public void getInfo(HttpRequest request, final HttpResponder responder,
                        @PathParam("instance-name") String name) {
      responder.sendString(HttpResponseStatus.OK, "get:" + name);
    }

    @POST
    @Path("/data/datasets/{instance-name}")
    public void add(HttpRequest request, final HttpResponder responder,
                    @PathParam("instance-name") String name) {
      responder.sendString(HttpResponseStatus.OK, "post:" + name);
    }

    @DELETE
    @Path("/data/datasets/{instance-name}")
    public void drop(HttpRequest request, final HttpResponder responder,
                     @PathParam("instance-name") String instanceName) {
      responder.sendString(HttpResponseStatus.OK, "delete:" + instanceName);
    }
  }

  private String doRequest(String resource, String requestMethod) throws Exception {
    resource = String.format("http://localhost:%d%s" + resource, port, Constants.Gateway.API_VERSION_3);
    URL url = new URL(resource);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(requestMethod);

    conn.setDoInput(true);

    conn.connect();
    try {
      byte[] responseBody = null;
      if (HttpURLConnection.HTTP_OK == conn.getResponseCode() && conn.getDoInput()) {
        InputStream is = conn.getInputStream();
        try {
          responseBody = ByteStreams.toByteArray(is);
        } finally {
          is.close();
        }
      }
      return new String(responseBody, Charsets.UTF_8);
    } finally {
      conn.disconnect();
    }
  }
}
