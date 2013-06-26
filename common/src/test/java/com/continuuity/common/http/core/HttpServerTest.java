package com.continuuity.common.http.core;

import com.continuuity.common.http.core.HttpDispatcher;
import com.continuuity.common.http.core.HttpResourceHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.utils.PortDetector;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class HttpServerTest {

  static int port;
  static Channel serverChannel;

  //TODO: Add more tests.
  @BeforeClass
  public static void setup() throws IOException {
    port = PortDetector.findFreePort();
    ServerBootstrap bootstrap = new ServerBootstrap( new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                                                                     Executors.newCachedThreadPool()));

    HttpResourceHandler.Builder builder = HttpResourceHandler.builder();
    builder.addHandler(new Handler());
    HttpResourceHandler resourceHandler = builder.build();


    ChannelPipeline pipeline = bootstrap.getPipeline();
    pipeline.addLast("decoder", new HttpRequestDecoder());
    pipeline.addLast("encoder", new HttpResponseEncoder());
    pipeline.addLast("compressor", new HttpContentCompressor());
    pipeline.addLast("executor", new ExecutionHandler(new OrderedMemoryAwareThreadPoolExecutor(16, 1048576, 1048576)));
    pipeline.addLast("dispatcher", new HttpDispatcher(resourceHandler));

    InetSocketAddress address = new InetSocketAddress(port);
    serverChannel = bootstrap.bind(address);
  }

  @Test
  public void testValidEndPoints() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/resource", port);
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
    HttpResponse response = request(put);
    assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testNonExistingEndPoints() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/users", port);
    HttpGet get = new HttpGet(endPoint);
    HttpResponse response = request(get);
    assertEquals(404, response.getStatusLine().getStatusCode());
  }


  @Test
  public void testNonExistingMethods() throws IOException {
    String endPoint = String.format("http://localhost:%d/test/v1/facebook/1/message", port);
    HttpGet get = new HttpGet(endPoint);
    HttpResponse response = request(get);
    assertEquals(405, response.getStatusLine().getStatusCode());
  }


  @AfterClass
  public static void teardown(){
    if(serverChannel != null){
      serverChannel.close();
    }
  }

  @Path("/test/v1")
  public static class Handler{
    @Path("resource")
    @GET
    public void testGet(HttpResponder responder){
      JsonObject object = new JsonObject();
      object.addProperty("status", "Handled get in resource end-point");
      responder.sendJson(HttpResponseStatus.OK, object);
    }

    @Path("tweets/{id}")
    @GET
    public void testGetTweet(HttpResponder responder, @PathParam("id") String id){
      JsonObject object = new JsonObject();
      object.addProperty("status", String.format("Handled get in tweets end-point, id: %s", id));
      responder.sendJson(HttpResponseStatus.OK, object);
    }

    @Path("tweets/{id}")
    @PUT
    public void testPutTweet(HttpResponder responder, @PathParam("id") String id){
      JsonObject object = new JsonObject();
      object.addProperty("status", String.format("Handled put in tweets end-point, id: %s", id));
      responder.sendJson(HttpResponseStatus.OK, object);
    }

    @Path("facebook/{id}/message")
    public void testNoMethodRoute(HttpResponder responder){

    }
  }

  private HttpResponse request(HttpUriRequest uri) throws IOException {
    HttpClient client = new DefaultHttpClient();
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
