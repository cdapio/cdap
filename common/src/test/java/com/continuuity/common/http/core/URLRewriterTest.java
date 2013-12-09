package com.continuuity.common.http.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test URLRewriter.
 */
public class URLRewriterTest {
  private static final Gson GSON = new Gson();

  private static String hostname = "127.0.0.1";
  private static int port;
  private static NettyHttpService service;

  @BeforeClass
  public static void setup() throws Exception {

    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(ImmutableList.of(new TestHandler()));
    builder.setUrlRewriter(new TestURLRewriter());
    builder.setHost(hostname);

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
  public void testUrlRewrite() throws Exception {
    HttpResponse response = doGet("/rewrite/test/v1/resource");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    response = doPut("/rewrite/test/v1/tweets/7648");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Map<String, String> stringMap = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                                                  new TypeToken<Map<String, String>>() {}.getType());
    Assert.assertEquals(ImmutableMap.of("status", "Handled put in tweets end-point, id: 7648"),
                        stringMap);
  }

  @Test
  public void testUrlRewriteNormalize() throws Exception {
    HttpResponse response = doGet("//rewrite//test/v1//resource");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
  }

  @Test
  public void testRegularCall() throws Exception {
    HttpResponse response = doGet("/test/v1/resource");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
  }

  @Test
  public void testUrlRewriteUnknownPath() throws Exception {
    HttpResponse response = doGet("/rewrite/unknown/test/v1/resource");
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatusLine().getStatusCode());
  }

  @Test
  public void testUrlRewriteRedirect() throws Exception {
    HttpResponse response = doGet("/redirect/test/v1/resource");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
  }

  private static class TestURLRewriter implements URLRewriter {
    @Override
    public boolean rewrite(HttpRequest request, HttpResponder responder) {
      if (request.getUri().startsWith("/rewrite/")) {
        request.setUri(request.getUri().replace("/rewrite/", "/"));
      }

      if (request.getUri().startsWith("/redirect/")) {
        responder.sendStatus(HttpResponseStatus.MOVED_PERMANENTLY,
                             ImmutableMultimap.of("Location", request.getUri().replace("/redirect/", "/rewrite/")));
        return false;
      }
      return true;
    }
  }

  public static HttpResponse doGet(String resource) throws Exception {
    return doGet(resource, null);
  }

  public static HttpResponse doGet(String resource, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet("http://" + hostname + ":" + port + resource);

    if (headers != null) {
      get.setHeaders(headers);
    }
    return client.execute(get);
  }

  public static HttpResponse doPut(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut("http://" + hostname + ":" + port + resource);
    return client.execute(put);
  }
}
