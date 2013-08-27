package com.continuuity.common.http.core;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the HttpServer
 */
public class HttpServerTest {

  static int port;
  static NettyHttpService service;

  //TODO: Add more tests.
  @BeforeClass
  public static void setup() throws Exception {

    List<HttpHandler> handlers = Lists.newArrayList();
    handlers.add(new Handler());

    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(handlers);

    service = builder.build();
    service.startAndWait();
    Service.State state = service.state();
    assertEquals(Service.State.RUNNING, state);
    port = service.getBindAddress().getPort();
  }

  @AfterClass
  public static void teardown() throws Exception {
    service.shutDown();
  }

  @Test
  public void testValidEndPoints() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/resource?num=10", port);
    HttpGet get = new HttpGet(endPoint);
    HttpResponse response = request(get);
    assertEquals(200, response.getStatusLine().getStatusCode());

    String content = getResponseContent(response);
    Gson gson = new Gson();
    Map<String, String> map = gson.fromJson(content, Map.class);
    assertEquals(1, map.size());
    assertEquals("Handled get in resource end-point", map.get("status"));

    endPoint = String.format("http://localhost:%d/test/v1/tweets/1", port);
    get = new HttpGet(endPoint);
    response = request(get);

    assertEquals(200, response.getStatusLine().getStatusCode());
    content = getResponseContent(response);
    map = gson.fromJson(content, Map.class);
    assertEquals(1, map.size());
    assertEquals("Handled get in tweets end-point, id: 1", map.get("status"));
  }

  @Test
  public void testPathWithMultipleMethods() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/tweets/1", port);
    HttpPut put = new HttpPut(endPoint);
    put.setEntity(new StringEntity("data"));
    HttpResponse response = request(put);
    assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testPathWithPost() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/tweets/1", port);
    HttpPut put = new HttpPut(endPoint);
    put.setEntity(new StringEntity("data"));
    HttpResponse response = request(put);
    assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testNonExistingEndPoints() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/users", port);
    HttpPost post = new HttpPost(endPoint);
    post.setEntity(new StringEntity("data"));
    HttpResponse response = request(post);
    assertEquals(404, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testPutWithData() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/facebook/1/message", port);
    HttpPut put = new HttpPut(endPoint);
    put.setEntity(new StringEntity("Hello, World"));
    HttpResponse response = request(put);
    assertEquals(200, response.getStatusLine().getStatusCode());
    String content = getResponseContent(response);
    Gson gson = new Gson();
    Map<String, String> map = gson.fromJson(content, Map.class);
    assertEquals(1, map.size());
    assertEquals("Handled put in tweets end-point, id: 1. Content: Hello, World", map.get("result"));
  }

  @Test
  public void testPostWithData() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/facebook/1/message", port);
    HttpPost post = new HttpPost(endPoint);
    post.setEntity(new StringEntity("Hello, World"));
    HttpResponse response = request(post);
    assertEquals(200, response.getStatusLine().getStatusCode());
    String content = getResponseContent(response);
    Gson gson = new Gson();
    Map<String, String> map = gson.fromJson(content, Map.class);
    assertEquals(1, map.size());
    assertEquals("Handled post in tweets end-point, id: 1. Content: Hello, World", map.get("result"));
  }

  @Test
  public void testNonExistingMethods() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/facebook/1/message", port);
    HttpGet get = new HttpGet(endPoint);
    HttpResponse response = request(get);
    assertEquals(405, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testKeepAlive() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/tweets/1", port);
    HttpPut put = new HttpPut(endPoint);
    put.setEntity(new StringEntity("data"));
    HttpResponse response = request(put, true);
    assertEquals(200, response.getStatusLine().getStatusCode());
    assertEquals("keep-alive", response.getFirstHeader("Connection").getValue());
  }

  @Test
  public void testMultiplePathParameters() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/user/sree/message/12", port);
    HttpGet get = new HttpGet(endPoint);
    HttpResponse response = request(get);
    assertEquals(200, response.getStatusLine().getStatusCode());
    String content = getResponseContent(response);
    Gson gson = new Gson();
    Map<String, String> map = gson.fromJson(content, Map.class);
    assertEquals(1, map.size());
    assertEquals("Handled multiple path parameters sree 12", map.get("result"));
  }


  //Test the end point where the parameter in path and order of declaration in method signature are different
  @Test
  public void testMultiplePathParametersWithParamterInDifferentOrder() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/message/21/user/sree", port);
    HttpGet get = new HttpGet(endPoint);
    HttpResponse response = request(get);
    assertEquals(200, response.getStatusLine().getStatusCode());
    String content = getResponseContent(response);
    Gson gson = new Gson();
    Map<String, String> map = gson.fromJson(content, Map.class);
    assertEquals(1, map.size());
    assertEquals("Handled multiple path parameters sree 21", map.get("result"));
  }

  @Test
  public void testNotRoutablePathParamMismatch() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/NotRoutable/sree", port);
    HttpGet get = new HttpGet(endPoint);
    HttpResponse response = request(get);
    assertEquals(500, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testNotRoutableMissingPathParam() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/NotRoutable/sree/message/12", port);
    HttpGet get = new HttpGet(endPoint);
    HttpResponse response = request(get);
    assertEquals(500, response.getStatusLine().getStatusCode());
  }

  /**
   * Test handler.
   */
  @Path("/test/v1")
  public static class Handler implements HttpHandler {
    @Path("resource")
    @GET
    public void testGet(HttpRequest request, HttpResponder responder){
      JsonObject object = new JsonObject();
      object.addProperty("status", "Handled get in resource end-point");
      responder.sendJson(HttpResponseStatus.OK, object);
    }

    @Path("tweets/{id}")
    @GET
    public void testGetTweet(HttpRequest request, HttpResponder responder, @PathParam("id") String id){
      JsonObject object = new JsonObject();
      object.addProperty("status", String.format("Handled get in tweets end-point, id: %s", id));
      responder.sendJson(HttpResponseStatus.OK, object);
    }

    @Path("tweets/{id}")
    @PUT
    public void testPutTweet(HttpRequest request, HttpResponder responder, @PathParam("id") String id){
      JsonObject object = new JsonObject();
      object.addProperty("status", String.format("Handled get in tweets end-point, id: %s", id));
      responder.sendJson(HttpResponseStatus.OK, object);
    }

    @Path("facebook/{id}/message")
    @DELETE
    public void testNoMethodRoute(HttpRequest request, HttpResponder responder, @PathParam("id") String id){

    }

    @Path("facebook/{id}/message")
    @PUT
    public void testPutMessage(HttpRequest request, HttpResponder responder, @PathParam("id") String id){
      String message = String.format("Handled put in tweets end-point, id: %s. ", id);
      try {
        String data = getStringContent(request);
        message = message.concat(String.format("Content: %s", data));
      } catch (IOException e) {
        //This condition should never occur
        assertTrue(false);
      }
      JsonObject object = new JsonObject();
      object.addProperty("result", message);
      responder.sendJson(HttpResponseStatus.OK, object);
    }

    @Path("facebook/{id}/message")
    @POST
    public void testPostMessage(HttpRequest request, HttpResponder responder, @PathParam("id") String id){
      String message = String.format("Handled post in tweets end-point, id: %s. ", id);
      try {
        String data = getStringContent(request);
        message = message.concat(String.format("Content: %s", data));
      } catch (IOException e) {
        //This condition should never occur
        assertTrue(false);
      }
      JsonObject object = new JsonObject();
      object.addProperty("result", message);
      responder.sendJson(HttpResponseStatus.OK, object);
    }

    @Path("/user/{userId}/message/{messageId}")
    @GET
    public void testMultipleParametersInPath(HttpRequest request, HttpResponder responder,
                                             @PathParam("userId") String userId,
                                             @PathParam("messageId") int messageId){
      JsonObject object = new JsonObject();
      object.addProperty("result", String.format("Handled multiple path parameters %s %d", userId, messageId));
      responder.sendJson(HttpResponseStatus.OK, object);
    }

    @Path("/message/{messageId}/user/{userId}")
    @GET
    public void testMultipleParametersInDifferentParameterDeclarationOrder(HttpRequest request, HttpResponder responder,
                                             @PathParam("userId") String userId,
                                             @PathParam("messageId") int messageId){
      JsonObject object = new JsonObject();
      object.addProperty("result", String.format("Handled multiple path parameters %s %d", userId, messageId));
      responder.sendJson(HttpResponseStatus.OK, object);
    }

    @Path("/NotRoutable/{id}")
    @GET
    public void notRoutableParameterMismatch(HttpRequest request,
                                             HttpResponder responder, @PathParam("userid") String userId){
      JsonObject object = new JsonObject();
      object.addProperty("result", String.format("Handled Not routable path %s ", userId));
      responder.sendJson(HttpResponseStatus.OK, object);
    }

    @Path("/NotRoutable/{userId}/message/{messageId}")
    @GET
    public void notRoutableMissingParameter(HttpRequest request, HttpResponder responder,
                                            @PathParam("userId") String userId, String messageId){
      JsonObject object = new JsonObject();
      object.addProperty("result", String.format("Handled Not routable path %s ", userId));
      responder.sendJson(HttpResponseStatus.OK, object);
    }

    private String getStringContent(HttpRequest request) throws IOException {
      return IOUtils.toString(new ChannelBufferInputStream(request.getContent()));
    }

    @Override
    public void init(HandlerContext context) {}

    @Override
    public void destroy(HandlerContext context) {}
  }

  private HttpResponse request(HttpUriRequest uri) throws IOException {
    return request(uri, false);
  }

  private HttpResponse request(HttpUriRequest uri, boolean keepalive) throws IOException {
    DefaultHttpClient client = new DefaultHttpClient();

    if (keepalive) {
      client.setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy());
    }
    HttpResponse response = client.execute(uri);
    return response;
  }

  private String getResponseContent(HttpResponse response) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteStreams.copy(response.getEntity().getContent(), bos);
    String result = bos.toString("UTF-8");
    bos.close();
    return result;
  }
}
