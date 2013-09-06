package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.http.core.NettyHttpService;
import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.weave.discovery.Discoverable;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import junit.framework.Assert;
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

import static com.continuuity.gateway.GatewayFastTestsSuite.GET;
import static com.continuuity.gateway.GatewayFastTestsSuite.POST;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;

/**
 * Tests ProcedureHandler.
 */
public class ProcedureHandlerTest  {
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
    GatewayFastTestsSuite.getInMemoryDiscoveryService().register(new Discoverable() {
      @Override
      public String getName() {
        return String.format("procedure.%s.%s.%s", "developer", "testApp1", "testProc1");
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    });

    GatewayFastTestsSuite.getInMemoryDiscoveryService().register(new Discoverable() {
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
    Gson gson = new Gson();
    String contentStr = gson.toJson(content, type);
    Assert.assertNotNull(contentStr);
    Assert.assertFalse(contentStr.isEmpty());

    HttpResponse response = 
      POST("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1", contentStr,
           new Header[]{new BasicHeader("X-Test", "1234")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals(content, gson.fromJson(responseStr, type));
    Assert.assertEquals("1234", response.getFirstHeader("X-Test").getValue());
  }

  @Test
  public void testPostEmptyProcedureCall() throws Exception {
    HttpResponse response =
      POST("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1", "",
           new Header[]{new BasicHeader("X-Test", "1234")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals("", responseStr);
    Assert.assertEquals("1234", response.getFirstHeader("X-Test").getValue());
  }

  @Test
  public void testPostNullProcedureCall() throws Exception {
    HttpResponse response =
      POST("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1", null,
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
      POST("/v2/apps/testApp1/procedures/testProc2/methods/testMethod1",
           new Gson().toJson(content, new TypeToken<Map<String, String>>() {
           }.getType()));
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatusLine().getStatusCode());
  }

  @Test
  public void testPostChunkedProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key5", "val5");
    Type type = new TypeToken<Map<String, String>>() {}.getType();
    Gson gson = new Gson();
    String contentStr = gson.toJson(content, type);
    Assert.assertNotNull(contentStr);
    Assert.assertFalse(contentStr.isEmpty());

    HttpResponse response =
      POST("/v2/apps/testApp2/procedures/testProc2/methods/testChunkedMethod", contentStr);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String expected = contentStr + contentStr;
    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals(expected, responseStr);
  }

  @Test
  public void testPostErrorProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key3", "val3");
    HttpResponse response =
      POST("/v2/apps/testApp2/procedures/testProc2/methods/testExceptionMethod",
           new Gson().toJson(content, new TypeToken<Map<String, String>>() {
           }.getType()));
    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(), response.getStatusLine().getStatusCode());
  }

  @Test
  public void testGetProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1&", "val1=", "key3", "\"val3\"");
    Type type = new TypeToken<Map<String, String>>() {}.getType();
    Gson gson = new Gson();

    HttpResponse response =
      GET("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1?" + getQueryParams(content),
          new Header[]{new BasicHeader("X-Test", "1234")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals(content, gson.fromJson(responseStr, type));
    Assert.assertEquals("1234", response.getFirstHeader("X-Test").getValue());
  }

  @Test
  public void testGetEmptyProcedureCall() throws Exception {
    HttpResponse response =
      GET("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1",
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
      GET("/v2/apps/testApp1/procedures/testProc2/methods/testMethod1&" + getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatusLine().getStatusCode());
  }

  @Test
  public void testGetChunkedProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key5", "val5");
    Type type = new TypeToken<Map<String, String>>() {}.getType();
    Gson gson = new Gson();
    String contentStr = gson.toJson(content, type);
    Assert.assertNotNull(contentStr);
    Assert.assertFalse(contentStr.isEmpty());

    HttpResponse response =
      GET("/v2/apps/testApp2/procedures/testProc2/methods/testChunkedMethod?" + getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String expected = contentStr + contentStr;
    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals(expected, responseStr);
    System.out.println(responseStr);
  }

  @Test
  public void testGetErrorProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key3", "val3");
    HttpResponse response =
      GET("/v2/apps/testApp2/procedures/testProc2/methods/testExceptionMethod?" + getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(), response.getStatusLine().getStatusCode());
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
}
