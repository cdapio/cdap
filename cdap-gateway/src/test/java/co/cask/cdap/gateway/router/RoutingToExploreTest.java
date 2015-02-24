/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.gateway.auth.NoAuthenticator;
import co.cask.cdap.security.auth.AccessTokenTransformer;
import co.cask.cdap.security.guice.SecurityModules;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
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

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class RoutingToExploreTest {
  private static NettyRouter nettyRouter;
  private static MockHttpService mockService;
  private static int port;

  @BeforeClass
  public static void before() throws Exception {
    Injector injector = Guice.createInjector(new ConfigModule(), new IOModule(),
                                             new SecurityModules().getInMemoryModules(),
                                             new DiscoveryRuntimeModule().getInMemoryModules());

    // Starting router
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    AccessTokenTransformer accessTokenTransformer = injector.getInstance(AccessTokenTransformer.class);
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Router.ADDRESS, "localhost");
    SConfiguration sConf = injector.getInstance(SConfiguration.class);
    port = Networks.getRandomPort();

    cConf.setInt(Constants.Router.ROUTER_PORT, port);
    nettyRouter = new NettyRouter(cConf, sConf, InetAddresses.forString("127.0.0.1"),

        new RouterServiceLookup(discoveryServiceClient,
            new RouterPathLookup(new NoAuthenticator())),
        new SuccessTokenValidator(), accessTokenTransformer, discoveryServiceClient);
    nettyRouter.startAndWait();

    // Starting mock DataSet service
    DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);
    mockService = new MockHttpService(discoveryService, Constants.Service.EXPLORE_HTTP_USER_SERVICE,
                                      new MockQueryExecutorHandler(), new MockExplorePingHandler(),
                                      new MockExploreExecutorHandler(), new MockExploreMetadataHandler());
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
  public void testExploreQueriesHandlerRequests() throws Exception {
    Assert.assertEquals("sendQuery", doRequest("/data/explore/queries", "POST"));
    Assert.assertEquals("stop:fooId", doRequest("/data/explore/queries/fooId", "DELETE"));
    Assert.assertEquals("status:fooId", doRequest("/data/explore/queries/fooId/status", "GET"));
    Assert.assertEquals("schema:fooId", doRequest("/data/explore/queries/fooId/schema", "GET"));
    Assert.assertEquals("nextResults:fooId", doRequest("/data/explore/queries/fooId/nextResults", "POST"));
    Assert.assertEquals("queries-list", doRequest("/data/explore/queries", "GET"));
    Assert.assertEquals("preview:fooId", doRequest("/data/explore/queries/fooId/preview", "POST"));
  }

  @Test
  public void testExploreExecutorHandlerRequests() throws Exception {
    Assert.assertEquals("schema:cdap.default.foobar", doRequest("/data/explore/datasets/foobar/schema", "GET"));
  }

  @Test
  public void testPingHandler() throws Exception {
    Assert.assertEquals("OK.\n", doRequest("/explore/status", "GET"));
  }

  @Test
  public void testExploreMetadataHandlerRequests() throws Exception {
    Assert.assertEquals("tables", doRequest("/data/explore/jdbc/tables", "POST"));
    Assert.assertEquals("columns", doRequest("/data/explore/jdbc/columns", "POST"));
    Assert.assertEquals("catalogs", doRequest("/data/explore/jdbc/catalogs", "POST"));
    Assert.assertEquals("schemas", doRequest("/data/explore/jdbc/schemas", "POST"));
    Assert.assertEquals("functions", doRequest("/data/explore/jdbc/functions", "POST"));
    Assert.assertEquals("tableTypes", doRequest("/data/explore/jdbc/tableTypes", "POST"));
    Assert.assertEquals("types", doRequest("/data/explore/jdbc/types", "POST"));
    Assert.assertEquals("info:some_type", doRequest("/data/explore/jdbc/info/some_type", "GET"));
  }

  @Path(Constants.Gateway.API_VERSION_2)
  public static final class MockExplorePingHandler extends AbstractHttpHandler {
    @GET
    @Path("/explore/status")
    public void status(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "OK.\n");
    }
  }

  @Path(Constants.Gateway.API_VERSION_2)
  public static final class MockQueryExecutorHandler extends AbstractHttpHandler {

    @POST
    @Path("/data/explore/queries")
    public void query(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "sendQuery");
    }

    @DELETE
    @Path("/data/explore/queries/{id}")
    public void closeQuery(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                           @PathParam("id") final String id) {
      responder.sendString(HttpResponseStatus.OK, "stop:" + id);
    }

    @GET
    @Path("/data/explore/queries/{id}/status")
    public void getQueryStatus(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                               @PathParam("id") final String id) {
      responder.sendString(HttpResponseStatus.OK, "status:" + id);
    }

    @GET
    @Path("/data/explore/queries/{id}/schema")
    public void getQueryResultsSchema(@SuppressWarnings("UnusedParameters") HttpRequest request,
                                      HttpResponder responder, @PathParam("id") final String id) {
      responder.sendString(HttpResponseStatus.OK, "schema:" + id);
    }

    @POST
    @Path("/data/explore/queries/{id}/nextResults")
    public void getQueryNextResults(HttpRequest request, HttpResponder responder, @PathParam("id") final String id) {
      responder.sendString(HttpResponseStatus.OK, "nextResults:" + id);
    }

    @GET
    @Path("/data/explore/queries")
    public void getQueries(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "queries-list");
    }

    @POST
    @Path("/data/explore/queries/{id}/preview")
    public void previewResults(HttpRequest request, HttpResponder responder, @PathParam("id") final String id) {
      responder.sendString(HttpResponseStatus.OK, "preview:" + id);
    }
  }

  @Path(Constants.Gateway.API_VERSION_2)
  public static final class MockExploreMetadataHandler extends AbstractHttpHandler {
    @POST
    @Path("/data/explore/jdbc/tables")
    public void getTables(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "tables");
    }

    @POST
    @Path("/data/explore/jdbc/columns")
    public void getColumns(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "columns");
    }

    @POST
    @Path("/data/explore/jdbc/catalogs")
    public void getCatalogs(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "catalogs");
    }

    @POST
    @Path("/data/explore/jdbc/schemas")
    public void getSchemas(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "schemas");
    }

    @POST
    @Path("/data/explore/jdbc/functions")
    public void getFunctions(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "functions");
    }

    @POST
    @Path("/data/explore/jdbc/tableTypes")
    public void getTableTypes(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "tableTypes");
    }

    @POST
    @Path("/data/explore/jdbc/types")
    public void getTypeInfo(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "types");
    }

    @GET
    @Path("/data/explore/jdbc/info/{type}")
    public void getInfo(HttpRequest request, HttpResponder responder, @PathParam("type") final String type) {
      responder.sendString(HttpResponseStatus.OK, "info:" + type);
    }
  }

  @Path(Constants.Gateway.API_VERSION_2)
  public static class MockExploreExecutorHandler extends AbstractHttpHandler {
    @GET
    @Path("/data/explore/datasets/{dataset}/schema")
    public void getSchema(HttpRequest request, HttpResponder responder,
                          @PathParam("dataset") final String datasetName) {
      responder.sendString(HttpResponseStatus.OK, "schema:" + datasetName);
    }
  }

  private String doRequest(String resource, String requestMethod) throws Exception {
    resource = String.format("http://localhost:%d%s" + resource, port, Constants.Gateway.API_VERSION_2);
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
