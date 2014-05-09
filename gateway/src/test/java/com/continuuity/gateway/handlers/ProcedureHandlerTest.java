package com.continuuity.gateway.handlers;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.http.NettyHttpService;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.internal.utils.Dependencies;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

import static com.continuuity.gateway.GatewayFastTestsSuite.doGet;
import static com.continuuity.gateway.GatewayFastTestsSuite.doPost;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;

/**
 * Tests Procedure API Handling.
 */
public class ProcedureHandlerTest  {
  private static final Gson GSON = new Gson();
  private static final String hostname = "127.0.0.1";
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
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
    Type type = MAP_STRING_STRING_TYPE;
    String contentStr = GSON.toJson(content, type);
    Assert.assertNotNull(contentStr);
    Assert.assertFalse(contentStr.isEmpty());

    HttpResponse response = 
      doPost("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1", contentStr,
             new Header[]{new BasicHeader("X-Test", "1234")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals(content, GSON.fromJson(responseStr, type));
  }

  @Test
  public void testPostEmptyProcedureCall() throws Exception {
    HttpResponse response =
      doPost("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1", "",
             new Header[]{new BasicHeader("X-Test", "1234")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals("", responseStr);
  }

  @Test
  public void testPostNullProcedureCall() throws Exception {
    HttpResponse response =
      doPost("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1", null,
             new Header[]{new BasicHeader("X-Test", "1234")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals("", responseStr);
  }

  @Test
  public void testPostNoProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key3", "val3");
    HttpResponse response =
      GatewayFastTestsSuite.doPost("/v2/apps/testApp1/procedures/testProc2/methods/testMethod1",
                                   GSON.toJson(content, new TypeToken<Map<String, String>>() {
                                   }.getType()));
    Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.getCode(), response.getStatusLine().getStatusCode());
  }

  /**
   * Test big content in Post request is not chunked. The content length is char[1423].
   */
  @Test
  public void testPostBigContentProcedureCall() throws Exception {
    Map<String, String> content = new ImmutableMap.Builder<String, String>()
      .put("key1", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva1")
      .put("key2", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva2")
      .put("key3", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva3")
      .put("key4", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva4")
      .put("key5", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva5")
      .put("key6", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva6")
      .put("key7", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva7")
      .put("key8", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva8")
      .put("key9", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva9")
      .put("key10", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva10")
      .put("key11", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva11")
      .put("key12", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva12")
      .put("key13", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva13")
      .put("key14", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva14")
      .put("key15", "valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalva15")
      .build();
    Type type = MAP_STRING_STRING_TYPE;
    String contentStr = GSON.toJson(content, type);
    Assert.assertNotNull(contentStr);
    Assert.assertFalse(contentStr.isEmpty());

    // Set entity chunked
    StringEntity entity = new StringEntity(contentStr);
    entity.setChunked(true);

    HttpPost post = GatewayFastTestsSuite.getPost("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1");
    post.setHeader("Expect", "100-continue");
    post.setEntity(entity);

    HttpResponse response = GatewayFastTestsSuite.doPost(post);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals(content, GSON.fromJson(responseStr, type));
  }

  @Test
  public void testPostChunkedProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key5", "val5");
    Type type = MAP_STRING_STRING_TYPE;
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
    HttpResponse response =
      doGet("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1?" + getQueryParams(content),
            new Header[]{new BasicHeader("X-Test", "1234")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
  }

  @Test
  public void testGetEmptyProcedureCall() throws Exception {
    HttpResponse response =
      doGet("/v2/apps/testApp1/procedures/testProc1/methods/testMethod1",
            new Header[]{new BasicHeader("X-Test", "1234")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    String responseStr = EntityUtils.toString(response.getEntity());
    Assert.assertEquals("", responseStr);
  }

  @Test
  public void testGetNoProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key3", "val3");
    HttpResponse response =
      GatewayFastTestsSuite.doGet("/v2/apps/testApp1/procedures/testProc2/methods/testMethod1&" + getQueryParams
        (content));
    Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.getCode(), response.getStatusLine().getStatusCode());
  }

  @Test
  public void testGetChunkedProcedureCall() throws Exception {
    Map<String, String> content = ImmutableMap.of("key1", "val1", "key5", "val5");
    Type type = MAP_STRING_STRING_TYPE;
    String contentStr = GSON.toJson(content, type);
    Assert.assertNotNull(contentStr);
    Assert.assertFalse(contentStr.isEmpty());

    HttpResponse response =
      GatewayFastTestsSuite.doGet("/v2/apps/testApp2/procedures/testProc2/methods/testChunkedMethod?"
                                    + getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
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

    //Make procedure call without deploying ProcedureTestApp
    HttpResponse response =
      GatewayFastTestsSuite.doGet("/v2/apps/ProcedureTestApp/procedures/TestProcedure/methods/TestMethod?" +
                                    getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals("Router cannot forward this request to any service",
                        EntityUtils.toString(response.getEntity()));

    // Deploy procedure, but do not start it.
    response = deploy(ProcedureTestApp.class, null);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    response =
      GatewayFastTestsSuite.doGet("/v2/apps/ProcedureTestApp/procedures/TestProcedure/methods/TestMethod?" +
                                    getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals("Router cannot forward this request to any service",
                        EntityUtils.toString(response.getEntity()));

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
                                               MAP_STRING_STRING_TYPE));

    // Stop procedure
    response =
      GatewayFastTestsSuite.doPost("/v2/apps/ProcedureTestApp/procedures/TestProcedure/stop", null);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    response =
      GatewayFastTestsSuite.doGet("/v2/apps/ProcedureTestApp/procedures/TestProcedure/methods/TestMethod?" +
                                    getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals("Router cannot forward this request to any service",
                        EntityUtils.toString(response.getEntity()));


    // Delete app
    response = GatewayFastTestsSuite.doDelete("/v2/apps/ProcedureTestApp");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    response =
      GatewayFastTestsSuite.doGet("/v2/apps/ProcedureTestApp/procedures/TestProcedure/methods/TestMethod?" +
                                    getQueryParams(content));
    Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals("Router cannot forward this request to any service",
                        EntityUtils.toString(response.getEntity()));
  }

  private static HttpResponse deploy(Class<?> application, @Nullable String appName) throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, application.getName());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    final JarOutputStream jarOut = new JarOutputStream(bos, manifest);
    final String pkgName = application.getPackage().getName();

    // Grab every classes under the application class package.
    try {
      ClassLoader classLoader = application.getClassLoader();
      if (classLoader == null) {
        classLoader = ClassLoader.getSystemClassLoader();
      }
      Dependencies.findClassDependencies(classLoader, new Dependencies.ClassAcceptor() {
        @Override
        public boolean accept(String className, URL classUrl, URL classPathUrl) {
          try {
            if (className.startsWith(pkgName)) {
              jarOut.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));
              InputStream in = classUrl.openStream();
              try {
                ByteStreams.copy(in, jarOut);
              } finally {
                in.close();
              }
              return true;
            }
            return false;
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }, application.getName());

      // Add webapp
      jarOut.putNextEntry(new ZipEntry("webapp/default/netlens/src/1.txt"));
      ByteStreams.copy(new ByteArrayInputStream("dummy data".getBytes(Charsets.UTF_8)), jarOut);
    } finally {
      jarOut.close();
    }

    HttpEntityEnclosingRequestBase request;
    if (appName == null) {
      request = GatewayFastTestsSuite.getPost("/v2/apps");
    } else {
      request = GatewayFastTestsSuite.getPut("/v2/apps/" + appName);
    }
    request.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, "api-key-example");
    request.setHeader("X-Archive-Name", application.getSimpleName() + ".jar");
    request.setEntity(new ByteArrayEntity(bos.toByteArray()));
    return GatewayFastTestsSuite.execute(request);
  }


  /**
   * Handler for test server.
   */
  public static class TestHandler extends AbstractHttpHandler {
    @POST
    @GET
    @Path("/v2/apps/{appId}/procedures/{procedureName}/methods/{methodName}")
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
    HttpPost request = new HttpPost(String.format(
      "http://%s:%d/v2/apps/testApp1/procedures/testProc1/methods/testMethod1", hostname, port));
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
   * App to test Procedure API Handling.
   */
  public static class ProcedureTestApp implements Application {
    @Override
    public ApplicationSpecification configure() {
      return ApplicationSpecification.Builder.with()
        .setName("ProcedureTestApp")
        .setDescription("App to test Procedure API Handling")
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
