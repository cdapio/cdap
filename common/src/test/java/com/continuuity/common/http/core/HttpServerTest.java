package com.continuuity.common.http.core;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
    handlers.add(new TestHandler());

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
