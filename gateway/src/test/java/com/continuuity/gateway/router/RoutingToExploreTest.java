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
                                      new MockExploreExecutorHandler(), new MockExplorePingHandler());
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
