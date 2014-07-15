/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.security.auth.AccessTokenTransformer;
import com.continuuity.security.guice.SecurityModules;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.net.InetAddresses;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 *
 */
public class RoutingToExploreTest {
  private static NettyRouter nettyRouter;
  private static MockHttpService mockService;
  private static int port;

  @BeforeClass
  public static void before() throws Exception {
    Injector injector = Guice.createInjector(new IOModule(), new SecurityModules().getInMemoryModules(),
        new DiscoveryRuntimeModule().getInMemoryModules());

    // Starting router
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    AccessTokenTransformer accessTokenTransformer = injector.getInstance(AccessTokenTransformer.class);
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Router.ADDRESS, "localhost");
    port = Networks.getRandomPort();
    cConf.set(Constants.Router.FORWARD, port + ":" + Constants.Service.GATEWAY);
    nettyRouter = new NettyRouter(cConf, InetAddresses.forString("127.0.0.1"),
        new RouterServiceLookup(discoveryServiceClient,
            new RouterPathLookup(new NoAuthenticator())),
        new SuccessTokenValidator(), accessTokenTransformer, discoveryServiceClient);
    nettyRouter.startAndWait();

    // Starting mock DataSet service
    DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);
    mockService = new MockHttpService(discoveryService, Constants.Service.EXPLORE_HTTP_USER_SERVICE,
                                      new MockExploreExecutorHandler(), new MockExplorePingHandler(),
                                      new MockExploreMetadataHandler());
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
  public void testExploreHandlerRequests() throws Exception {
    Assert.assertEquals("sendQuery", doRequest("/data/queries", "POST"));
    Assert.assertEquals("stop:fooId", doRequest("/data/queries/fooId", "DELETE"));
    Assert.assertEquals("cancel:fooId", doRequest("/data/queries/fooId/cancel", "POST"));
    Assert.assertEquals("status:fooId", doRequest("/data/queries/fooId/status", "GET"));
    Assert.assertEquals("schema:fooId", doRequest("/data/queries/fooId/schema", "GET"));
    Assert.assertEquals("nextResults:fooId", doRequest("/data/queries/fooId/nextResults", "POST"));
  }

  @Test
  public void testPingHandler() throws Exception {
    Assert.assertEquals("OK.\n", doRequest("/explore/status", "GET"));
  }

  @Test
  public void testExploreMetadataHandlerRequests() throws Exception {
    Assert.assertEquals("tables", doRequest("/data/metadata/tables", "POST"));
    Assert.assertEquals("columns", doRequest("/data/metadata/columns", "POST"));
    Assert.assertEquals("catalogs", doRequest("/data/metadata/catalogs", "POST"));
    Assert.assertEquals("schemas", doRequest("/data/metadata/schemas", "POST"));
    Assert.assertEquals("functions", doRequest("/data/metadata/functions", "POST"));
    Assert.assertEquals("tableTypes", doRequest("/data/metadata/tableTypes", "POST"));
    Assert.assertEquals("typeInfo", doRequest("/data/metadata/typeInfo", "POST"));
    Assert.assertEquals("info:some_type", doRequest("/data/metadata/info/some_type", "GET"));
  }

  @Path(Constants.Gateway.GATEWAY_VERSION)
  public static final class MockExplorePingHandler extends AbstractHttpHandler {
    @GET
    @Path("/explore/status")
    public void status(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "OK.\n");
    }
  }

  @Path(Constants.Gateway.GATEWAY_VERSION)
  public static final class MockExploreExecutorHandler extends AbstractHttpHandler {

    @POST
    @Path("/data/queries")
    public void query(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "sendQuery");
    }

    @DELETE
    @Path("/data/queries/{id}")
    public void closeQuery(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                           @PathParam("id") final String id) {
      responder.sendString(HttpResponseStatus.OK, "stop:" + id);
    }

    @POST
    @Path("/data/queries/{id}/cancel")
    public void cancelQuery(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                            @PathParam("id") final String id) {
      responder.sendString(HttpResponseStatus.OK, "cancel:" + id);
    }

    @GET
    @Path("/data/queries/{id}/status")
    public void getQueryStatus(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                               @PathParam("id") final String id) {
      responder.sendString(HttpResponseStatus.OK, "status:" + id);
    }

    @GET
    @Path("/data/queries/{id}/schema")
    public void getQueryResultsSchema(@SuppressWarnings("UnusedParameters") HttpRequest request,
                                      HttpResponder responder, @PathParam("id") final String id) {
      responder.sendString(HttpResponseStatus.OK, "schema:" + id);
    }

    @POST
    @Path("/data/queries/{id}/nextResults")
    public void getQueryNextResults(HttpRequest request, HttpResponder responder, @PathParam("id") final String id) {
      responder.sendString(HttpResponseStatus.OK, "nextResults:" + id);
    }
  }

  @Path(Constants.Gateway.GATEWAY_VERSION)
  public static final class MockExploreMetadataHandler extends AbstractHttpHandler {
    private static final String PATH = "data/metadata/";

    @POST
    @Path(PATH + "tables")
    public void getTables(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "tables");
    }

    @POST
    @Path(PATH + "columns")
    public void getColumns(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "columns");
    }

    @POST
    @Path(PATH + "catalogs")
    public void getCatalogs(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "catalogs");
    }

    @POST
    @Path(PATH + "schemas")
    public void getSchemas(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "schemas");
    }

    @POST
    @Path(PATH + "functions")
    public void getFunctions(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "functions");
    }

    @POST
    @Path(PATH + "tableTypes")
    public void getTableTypes(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "tableTypes");
    }

    @POST
    @Path(PATH + "typeInfo")
    public void getTypeInfo(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "typeInfo");
    }

    @GET
    @Path(PATH + "info/{type}")
    public void getInfo(HttpRequest request, HttpResponder responder, @PathParam("type") final String type) {
      responder.sendString(HttpResponseStatus.OK, "info:" + type);
    }
  }

  private String doRequest(String resource, String requestMethod) throws Exception {
    resource = String.format("http://localhost:%d%s" + resource, port, Constants.Gateway.GATEWAY_VERSION);
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
