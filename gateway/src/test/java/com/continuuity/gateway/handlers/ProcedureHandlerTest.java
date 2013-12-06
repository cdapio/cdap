package com.continuuity.gateway.handlers;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.http.core.NettyHttpService;
import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryService;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

import static com.continuuity.gateway.GatewayFastTestsSuite.doGet;
import static com.continuuity.gateway.GatewayFastTestsSuite.doPost;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;

/**
 * Tests ProcedureHandler.
 */
public class ProcedureHandlerTest  {
  private static final Gson GSON = new Gson();
  private static final String hostname = "127.0.0.1";
  private static NettyHttpService httpService;
  private static int port;

  @BeforeClass
  public static void startProcedureServer() throws Exception {

    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(ImmutableSet.of(new TestHandler()));
    builder.setHost(hostname);
    builder.setPort(0);
    httpService = builder.build();
    httpService.startAndWait();

    // Register services of test server
    DiscoveryService discoveryService = GatewayFastTestsSuite.getInjector().getInstance(DiscoveryService.class);
    discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return String.format("procedure.%s.%s.%s", "developer", "testApp1", "testProc1");
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    });

    discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return String.format("procedure.%s.%s.%s", "developer", "testApp2", "testProc2");
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    });

    port = httpService.getBindAddress().getPort();
    testTestServer();
  }

  @AfterClass
  public static void stopProcedureServer() {
    httpService.stopAndWait();
  }

  @Test
  public void testPostProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key3", "val3");
    Type type = new TypeToken<Map<String, String>>() {}.getType();
    String contentStr = GSON.toJson(content, type);
    Assert.assertNotNull(contentStr);
    Assert.assertFalse(contentStr.isEmpty());

    HttpResponse response = 
      doPost("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1", contentStr,
             new Header[]{new BasicHeader("X-Test", "1234")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals(content, GSON.fromJson(responseStr, type));
    Assert.assertEquals("1234", response.getFirstHeader("X-Test").getValue());
  }

  @Test
  public void testPostEmptyProcedureCall() throws Exception {
    HttpResponse response =
      doPost("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1", "",
             new Header[]{new BasicHeader("X-Test", "1234")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals("", responseStr);
    Assert.assertEquals("1234", response.getFirstHeader("X-Test").getValue());
  }

  @Test
  public void testPostNullProcedureCall() throws Exception {
    HttpResponse response =
      doPost("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1", null,
             new Header[]{new BasicHeader("X-Test", "1234")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals("", responseStr);
    Assert.assertEquals("1234", response.getFirstHeader("X-Test").getValue());
  }

  @Test
  public void testPostNoProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key3", "val3");
    HttpResponse response =
      GatewayFastTestsSuite.doPost("/v2/apps/testApp1/procedures/testProc2/methods/testMethod1",
                                   GSON.toJson(content, new TypeToken<Map<String, String>>() {
                                   }.getType()));
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatusLine().getStatusCode());
  }

  @Test
  public void testPostChunkedProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key5", "val5");
    Type type = new TypeToken<Map<String, String>>() {}.getType();
    String contentStr = GSON.toJson(content, type);
    Assert.assertNotNull(contentStr);
    Assert.assertFalse(contentStr.isEmpty());

    HttpResponse response =
      GatewayFastTestsSuite.doPost("/v2/apps/testApp2/procedures/testProc2/methods/testChunkedMethod", contentStr);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String expected = contentStr + contentStr;
    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals(expected, responseStr);
  }

  @Test
  public void testPostErrorProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key3", "val3");
    HttpResponse response =
      GatewayFastTestsSuite.doPost("/v2/apps/testApp2/procedures/testProc2/methods/testExceptionMethod",
                                   GSON.toJson(content, new TypeToken<Map<String, String>>() {
                                   }.getType()));
    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(), response.getStatusLine().getStatusCode());
  }

  @Test
  public void testGetProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1&", "val1=", "key3", "\"val3\"");
    Type type = new TypeToken<Map<String, String>>() {}.getType();

    HttpResponse response =
      doGet("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1?" + getQueryParams(content),
            new Header[]{new BasicHeader("X-Test", "1234")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals(content, GSON.fromJson(responseStr, type));
    Assert.assertEquals("1234", response.getFirstHeader("X-Test").getValue());
  }

  @Test
  public void testGetEmptyProcedureCall() throws Exception {
    HttpResponse response =
      doGet("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1",
            new Header[]{new BasicHeader("X-Test", "1234")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals("", responseStr);
    Assert.assertEquals("1234", response.getFirstHeader("X-Test").getValue());
  }

  @Test
  public void testGetNoProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key3", "val3");
    HttpResponse response =
      GatewayFastTestsSuite.doGet("/v2/apps/testApp1/procedures/testProc2/methods/testMethod1&" + getQueryParams
        (content));
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatusLine().getStatusCode());
  }

  @Test
  public void testGetChunkedProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key5", "val5");
    Type type = new TypeToken<Map<String, String>>() {}.getType();
    String contentStr = GSON.toJson(content, type);
    Assert.assertNotNull(contentStr);
    Assert.assertFalse(contentStr.isEmpty());

    HttpResponse response =
      GatewayFastTestsSuite.doGet("/v2/apps/testApp2/procedures/testProc2/methods/testChunkedMethod?"
                                    + getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String expected = contentStr + contentStr;
    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals(expected, responseStr);
  }

  @Test
  public void testGetErrorProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key3", "val3");
    HttpResponse response =
      GatewayFastTestsSuite.doGet("/v2/apps/testApp2/procedures/testProc2/methods/testExceptionMethod?" +
                                    getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(), response.getStatusLine().getStatusCode());
  }

  @Test
  public void testRealProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key3", "val3");

    // Make procedure call without deploying ProcedureTestApp
    HttpResponse response =
      GatewayFastTestsSuite.doGet("/v2/apps/ProcedureTestApp/procedures/TestProcedure/methods/TestMethod?" +
                                    getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals("Procedure not deployed", EntityUtils.toString(response.getEntity()));

    // Deploy procedure, but do not start it.
    AppFabricServiceHandlerTest.deploy(ProcedureTestApp.class);
    response =
      GatewayFastTestsSuite.doGet("/v2/apps/ProcedureTestApp/procedures/TestProcedure/methods/TestMethod?" +
                                    getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals("Procedure not running", EntityUtils.toString(response.getEntity()));

    // Start procedure
    response =
      GatewayFastTestsSuite.doPost("/v2/apps/ProcedureTestApp/procedures/TestProcedure/start", null);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    // Make procedure call
    response =
      GatewayFastTestsSuite.doGet("/v2/apps/ProcedureTestApp/procedures/TestProcedure/methods/TestMethod?" +
                                    getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals(content, GSON.fromJson(EntityUtils.toString(response.getEntity()),
                                               new TypeToken<Map<String, String>>() {}.getType()));

    // Stop procedure
    response =
      GatewayFastTestsSuite.doPost("/v2/apps/ProcedureTestApp/procedures/TestProcedure/stop", null);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    response =
      GatewayFastTestsSuite.doGet("/v2/apps/ProcedureTestApp/procedures/TestProcedure/methods/TestMethod?" +
                                    getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals("Procedure not running", EntityUtils.toString(response.getEntity()));

    // Delete app
    Assert.assertEquals(HttpResponseStatus.OK.getCode(),
                        GatewayFastTestsSuite.doDelete("/v2/apps/ProcedureTestApp").getStatusLine().getStatusCode());

    response =
      GatewayFastTestsSuite.doGet("/v2/apps/ProcedureTestApp/procedures/TestProcedure/methods/TestMethod?" +
                                    getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals("Procedure not deployed", EntityUtils.toString(response.getEntity()));
  }

  /**
   * Handler for test server.
   */
  public static class TestHandler extends AbstractHttpHandler {
    @POST
    @Path("/apps/{appId}/procedures/{procedureName}/{methodName}")
    public void handle(HttpRequest request, final HttpResponder responder,
                       @PathParam("appId") String appId, @PathParam("procedureName") String procedureName,
                       @PathParam("methodName") String methodName) {

      // /apps/testApp1/procedures/testProc1/testMethod1
      if ("testApp1".equals(appId) && "testProc1".equals(procedureName) && "testMethod1".equals(methodName)) {
        byte [] content = request.getContent().array();

        ImmutableMultimap.Builder<String, String> headerBuilder = ImmutableMultimap.builder();
        for (Map.Entry<String, String> entry : request.getHeaders()) {
          headerBuilder.put(entry.getKey(), entry.getValue());
        }

        if (request.getHeader(CONTENT_TYPE) == null) {
          headerBuilder.put(CONTENT_TYPE, "text/plain");
        }
        if (request.getHeader(CONTENT_LENGTH) == null || Integer.parseInt(request.getHeader(CONTENT_LENGTH)) == 0) {
          headerBuilder.put(CONTENT_LENGTH, Integer.toString(content.length));
        }

        responder.sendByteArray(HttpResponseStatus.OK, content, headerBuilder.build());

      } else if ("testApp2".equals(appId) && "testProc2".equals(procedureName) &&
        "testChunkedMethod".equals(methodName)) {
        // /apps/testApp2/procedures/testProc2/testChunkedMethod
        responder.sendChunkStart(HttpResponseStatus.OK, ImmutableMultimap.of(CONTENT_TYPE, "text/plain"));
        responder.sendChunk(ChannelBuffers.wrappedBuffer(request.getContent().array()));
        responder.sendChunk(ChannelBuffers.wrappedBuffer(request.getContent().array()));
        responder.sendChunkEnd();

      } else if ("testApp2".equals(appId) && "testProc2".equals(procedureName) &&
        "testExceptionMethod".equals(methodName)) {
        // /apps/testApp2/procedures/testProc2/testExceptionMethod
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      } else {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      }
    }
  }

  private static void testTestServer() throws Exception {
    DefaultHttpClient httpclient = new DefaultHttpClient();
    HttpPost request = new HttpPost(String.format("http://%s:%d/apps/testApp1/procedures/testProc1/testMethod1",
                                                hostname, port));
    HttpResponse response = httpclient.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
  }

  private String getQueryParams(Map<String, String> params) throws Exception {
    List<String> plist = Lists.newArrayList();
    for (Map.Entry<String, String> entry : params.entrySet()) {
      plist.add(String.format("%s=%s", URLEncoder.encode(entry.getKey(), "utf-8"),
                              URLEncoder.encode(entry.getValue(), "utf-8")));
    }
    return Joiner.on("&").join(plist);
  }

  /**
   * App to test ProcedureHandler.
   */
  public static class ProcedureTestApp implements Application {
    @Override
    public ApplicationSpecification configure() {
      return ApplicationSpecification.Builder.with()
        .setName("ProcedureTestApp")
        .setDescription("App to test ProcedureHandler")
        .noStream()
        .noDataSet()
        .noFlow()
        .withProcedures()
        .add(new TestProcedure())
        .noMapReduce()
        .noWorkflow()
        .build();
    }

    /**
     * TestProcedure handler.
     */
    public static class TestProcedure extends AbstractProcedure {
      @Override
      public ProcedureSpecification configure() {
        return ProcedureSpecification.Builder.with()
          .setName("TestProcedure")
          .setDescription("Test Procedure")
          .build();
      }

      @SuppressWarnings("UnusedDeclaration")
      @Handle("TestMethod")
      public void testMethod1(ProcedureRequest request, ProcedureResponder responder) throws Exception {
        responder.sendJson(request.getArguments());
      }
    }
  }
}
