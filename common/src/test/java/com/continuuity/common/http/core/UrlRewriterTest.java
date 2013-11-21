package com.continuuity.common.http.core;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test UrlRewriter.
 */
public class UrlRewriterTest {
  private static String hostname = "127.0.0.1";
  private static int port;
  private static NettyHttpService service;

  @BeforeClass
  public static void setup() throws Exception {

    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(ImmutableList.of(new TestHandler()));
    builder.setUrlRewriter(new TestUrlRewriter());
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

  private static class TestUrlRewriter implements UrlRewriter {
    @Override
    public void rewrite(HttpRequest request) {
      if (request.getUri().startsWith("/rewrite/")) {
        request.setUri(request.getUri().replace("/rewrite/", "/"));
      }
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
}
