package com.continuuity.common.http.core;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

/**
 * Tests handler hooks.
 */
public class HandlerHookTest {
  private static String hostname = "127.0.0.1";
  private static int port;
  private static NettyHttpService service;
  private static final TestHandlerHook handlerHook1 = new TestHandlerHook();
  private static final TestHandlerHook handlerHook2 = new TestHandlerHook();

  @BeforeClass
  public static void setup() throws Exception {

    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(ImmutableList.of(new TestHandler()));
    builder.setHandlerHooks(ImmutableList.of(handlerHook1, handlerHook2));
    builder.setHost(hostname);

    service = builder.build();
    service.startAndWait();
    Service.State state = service.state();
    assertEquals(Service.State.RUNNING, state);
    port = service.getBindAddress().getPort();
  }

  @Before
  public void reset() {
    handlerHook1.reset();
    handlerHook2.reset();
  }

  @Test
  public void testHandlerHookCall() throws Exception {
    HttpResponse response = doGet("/test/v1/resource");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    Assert.assertEquals(1, handlerHook1.getNumPreCalls());
    Assert.assertEquals(1, handlerHook1.getNumPostCalls());

    Assert.assertEquals(1, handlerHook2.getNumPreCalls());
    Assert.assertEquals(1, handlerHook2.getNumPostCalls());
  }

  @Test
  public void testPreHookReject() throws Exception {
    HttpResponse response = doGet("/test/v1/resource", new Header[]{new BasicHeader("X-Request-Type", "Reject")});
    Assert.assertEquals(HttpResponseStatus.NOT_ACCEPTABLE.getCode(), response.getStatusLine().getStatusCode());

    Assert.assertEquals(1, handlerHook1.getNumPreCalls());
    Assert.assertEquals(1, handlerHook1.getNumPostCalls());

    // The second pre-call should not have happened due to rejection by the first pre-call
    Assert.assertEquals(0, handlerHook2.getNumPreCalls());
    Assert.assertEquals(1, handlerHook2.getNumPostCalls());
  }

  @Test
  public void testHandlerException() throws Exception {
    HttpResponse response = doGet("/test/v1/exception");
    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(), response.getStatusLine().getStatusCode());

    Assert.assertEquals(1, handlerHook1.getNumPreCalls());
    Assert.assertEquals(1, handlerHook1.getNumPostCalls());

    Assert.assertEquals(1, handlerHook2.getNumPreCalls());
    Assert.assertEquals(1, handlerHook2.getNumPostCalls());
  }

  @Test
  public void testPreException() throws Exception {
    HttpResponse response = doGet("/test/v1/resource",
                                  new Header[]{new BasicHeader("X-Request-Type", "PreException")});
    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(), response.getStatusLine().getStatusCode());

    Assert.assertEquals(1, handlerHook1.getNumPreCalls());
    Assert.assertEquals(1, handlerHook1.getNumPostCalls());

    Assert.assertEquals(0, handlerHook2.getNumPreCalls());
    Assert.assertEquals(1, handlerHook2.getNumPostCalls());
  }

  @Test
  public void testPostException() throws Exception {
    HttpResponse response = doGet("/test/v1/resource",
                                  new Header[]{new BasicHeader("X-Request-Type", "PostException")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    Assert.assertEquals(1, handlerHook1.getNumPreCalls());
    Assert.assertEquals(1, handlerHook1.getNumPostCalls());

    Assert.assertEquals(1, handlerHook2.getNumPreCalls());
    Assert.assertEquals(0, handlerHook2.getNumPostCalls());
  }

  @AfterClass
  public static void teardown() throws Exception {
    service.shutDown();
  }

  private static class TestHandlerHook extends AbstractHandlerHook {
    private volatile int numPreCalls = 0;
    private volatile int numPostCalls = 0;

    public int getNumPreCalls() {
      return numPreCalls;
    }

    public int getNumPostCalls() {
      return numPostCalls;
    }

    public void reset() {
      numPreCalls = 0;
      numPostCalls = 0;
    }

    @Override
    public boolean preCall(HttpRequest request, HttpResponder responder, Method method) {
      ++numPreCalls;

      String header = request.getHeader("X-Request-Type");
      if (header != null && header.equals("Reject")) {
        responder.sendStatus(HttpResponseStatus.NOT_ACCEPTABLE);
        return false;
      }

      if (header != null && header.equals("PreException")) {
        throw new IllegalArgumentException("PreException");
      }

      return true;
    }

    @Override
    public void postCall(HttpRequest request, HttpResponseStatus status, Method method) {
      ++numPostCalls;

      String header = request.getHeader("X-Request-Type");
      if (header != null && header.equals("PostException")) {
        throw new IllegalArgumentException("PostException");
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
