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
 *
 */
public class RoutingToDataSetsTest {
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
    mockService = new MockHttpService(discoveryService, Constants.Service.DATASET_MANAGER,
                                      new MockDatasetTypeHandler(), new DatasetInstanceHandler());
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
    Assert.assertEquals("listModules", doRequest("/data/modules", "GET"));
    Assert.assertEquals("post:myModule", doRequest("/data/modules/myModule", "POST"));
    Assert.assertEquals("delete:myModule", doRequest("/data/modules/myModule", "DELETE"));
    Assert.assertEquals("get:myModule", doRequest("/data/modules/myModule", "GET"));
    Assert.assertEquals("listTypes", doRequest("/data/types", "GET"));
    Assert.assertEquals("getType:myType", doRequest("/data/types/myType", "GET"));
  }

  @Test
  public void testInstanceHandlerRequests() throws Exception {
    Assert.assertEquals("list", doRequest("/data/datasets", "GET"));
    Assert.assertEquals("post:continuuity.user.myInstance", doRequest("/data/datasets/myInstance", "POST"));
    Assert.assertEquals("delete:continuuity.user.myInstance", doRequest("/data/datasets/myInstance", "DELETE"));
    Assert.assertEquals("get:continuuity.user.myInstance", doRequest("/data/datasets/myInstance", "GET"));
  }

  @Path(Constants.Gateway.GATEWAY_VERSION)
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

  @Path(Constants.Gateway.GATEWAY_VERSION)
  public static final class DatasetInstanceHandler extends AbstractHttpHandler {
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
