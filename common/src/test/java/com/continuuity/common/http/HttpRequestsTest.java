package com.continuuity.common.http;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.http.NettyHttpService;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.matcher.Matcher;
import com.google.inject.matcher.Matchers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Test for {@link HttpRequests}.
 */
public class HttpRequestsTest {

  private TestHttpService httpService;

  @Before
  public void setUp() {
    httpService = new TestHttpService(CConfiguration.create());
    httpService.startAndWait();
  }

  @After
  public void tearDown() {
    httpService.stopAndWait();
  }

  @Test
  public void testHttpStatus() throws Exception {
    doGet("/testHttpStatus", Matchers.only(200), Matchers.only("OK"), Matchers.any());
  }

  private void doGet(String path, Matcher<Object> expectedResponseCode, Matcher<Object> expectedMessage,
                     Matcher<Object> expectedBody) throws Exception {
    HttpRequests.HttpResponse response = HttpRequests.get(getBaseURI().resolve(path).toURL());
    Assert.assertTrue("Response code - expected: " + expectedResponseCode.toString()
                        + " actual: " + response.getResponseCode(),
                      expectedResponseCode.matches(response.getResponseCode()));
    Assert.assertTrue("Response message - expected: " + expectedMessage.toString()
                        + " actual: " + response.getResponseMessage(),
                      expectedMessage.matches(response.getResponseMessage()));
    
    Assert.assertTrue("Response body - expected: " + expectedBody.toString()
                        + " actual: " + Bytes.toString(response.getResponseBody()),
                      expectedBody.matches(response.getResponseBody()));
  }

  private URI getBaseURI() throws URISyntaxException {
    InetSocketAddress bindAddress = httpService.getBindAddress();
    return new URI("http://" + bindAddress.getHostName() + ":" + bindAddress.getPort());
  }

  public final class TestHttpService extends AbstractIdleService {

    private final NettyHttpService httpService;

    public TestHttpService(CConfiguration cConf) {
      this.httpService = NettyHttpService.builder()
        .setHost("localhost")
        .addHttpHandlers(Sets.newHashSet(new TestHandler()))
        .setWorkerThreadPoolSize(10)
        .setExecThreadPoolSize(10)
        .setConnectionBacklog(20000)
        .build();
    }

    public InetSocketAddress getBindAddress() {
      return httpService.getBindAddress();
    }

    @Override
    protected void startUp() throws Exception {
      httpService.startAndWait();
    }

    @Override
    protected void shutDown() throws Exception {
      httpService.stopAndWait();
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("bindAddress", httpService.getBindAddress())
        .toString();
    }
  }

  public final class TestHandler extends AbstractHttpHandler {

    @GET
    @Path("/testHttpStatus")
    public void testHttpStatus(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.OK);
    }

  }

}
