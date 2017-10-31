/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


package co.cask.cdap.client.util;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.inject.matcher.Matcher;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;

import static com.google.inject.matcher.Matchers.any;
import static com.google.inject.matcher.Matchers.only;


public class RESTClientTest {

  private static final String ACCESS_TOKEN = "ssdw221e2ffderrfg33322rr";
  private static final int RETRY_LIMIT = 2;

  private TestHttpService httpService;
  private RESTClient restClient;

  @Before
  public void setUp() throws IOException {
    httpService = new TestHttpService();
    httpService.startAndWait();
    restClient = new RESTClient(ClientConfig.builder().setUnavailableRetryLimit(3).build());
  }

  @After
  public void tearDown() {
    httpService.stopAndWait();
  }

  @Test
  public void testPostSuccessWithAccessToken() throws Exception {
    URL url = getBaseURI().resolve("/api/testPostAuth").toURL();
    HttpRequest request = HttpRequest.post(url).build();
    HttpResponse response = restClient.execute(request, new AccessToken(ACCESS_TOKEN, 82000L, "Bearer"));
    verifyResponse(response, only(200), any(), only("Access token received: " + ACCESS_TOKEN));
  }

  @Test(expected = UnauthenticatedException.class)
  public void testPostUnauthorizedWithAccessToken() throws Exception {
    URL url = getBaseURI().resolve("/api/testPostAuth").toURL();
    HttpRequest request = HttpRequest.post(url).build();
    restClient.execute(request, new AccessToken("Unknown", 82000L, "Bearer"));
  }

  @Test
  public void testPutSuccessWithAccessToken() throws Exception {
    URL url = getBaseURI().resolve("/api/testPutAuth").toURL();
    HttpRequest request = HttpRequest.put(url).build();
    HttpResponse response = restClient.execute(request, new AccessToken(ACCESS_TOKEN, 82000L, "Bearer"));
    verifyResponse(response, only(200), any(), only("Access token received: " + ACCESS_TOKEN));
  }

  @Test(expected = UnauthenticatedException.class)
  public void testPutUnauthorizedWithAccessToken() throws Exception {
    URL url = getBaseURI().resolve("/api/testPutAuth").toURL();
    HttpRequest request = HttpRequest.put(url).build();
    restClient.execute(request, new AccessToken("Unknown", 82000L, "Bearer"));
  }

  @Test
  public void testGetSuccessWithAccessToken() throws Exception {
    URL url = getBaseURI().resolve("/api/testGetAuth").toURL();
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = restClient.execute(request, new AccessToken(ACCESS_TOKEN, 82000L, "Bearer"));
    verifyResponse(response, only(200), any(), only("Access token received: " + ACCESS_TOKEN));
  }

  @Test(expected = UnauthenticatedException.class)
  public void testGetUnauthorizedWithAccessToken() throws Exception {
    URL url = getBaseURI().resolve("/api/testGetAuth").toURL();
    HttpRequest request = HttpRequest.get(url).build();
    restClient.execute(request, new AccessToken("Unknown", 82000L, "Bearer"));
  }

  @Test
  public void testDeleteSuccessWithAccessToken() throws Exception {
    URL url = getBaseURI().resolve("/api/testDeleteAuth").toURL();
    HttpRequest request = HttpRequest.delete(url).build();
    HttpResponse response = restClient.execute(request, new AccessToken(ACCESS_TOKEN, 82000L, "Bearer"));
    verifyResponse(response, only(200), any(), only("Access token received: " + ACCESS_TOKEN));
  }

  @Test(expected = UnauthenticatedException.class)
  public void testDeleteUnauthorizedWithAccessToken() throws Exception {
    URL url = getBaseURI().resolve("/api/testDeleteAuth").toURL();
    HttpRequest request = HttpRequest.delete(url).build();
    restClient.execute(request, new AccessToken("Unknown", 82000L, "Bearer"));
  }

  @Test
  public void testUnavailableLimit() throws Exception {
    URL url = getBaseURI().resolve("/api/testUnavail").toURL();
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = restClient.execute(request, new AccessToken(ACCESS_TOKEN, 82000L, "Bearer"));
    verifyResponse(response, only(200), any(), any());
  }

  @Test
  public void testBody() throws Exception {
    URL url = getBaseURI().resolve("/api/testCount").toURL();
    HttpResponse response = restClient.execute(HttpMethod.POST, url, "increment", null,
                                               new AccessToken(ACCESS_TOKEN, 82000L, "Bearer"), 200);
    Assert.assertEquals("1", response.getResponseBodyAsString());
    response = restClient.execute(HttpMethod.POST, url, "increment", null,
                                               new AccessToken(ACCESS_TOKEN, 82000L, "Bearer"), 200);
    Assert.assertEquals("2", response.getResponseBodyAsString());
    response = restClient.execute(HttpMethod.POST, url, "decrement", null,
                                  new AccessToken(ACCESS_TOKEN, 82000L, "Bearer"), 200);
    Assert.assertEquals("1", response.getResponseBodyAsString());
    response = restClient.execute(HttpMethod.POST, url, "decrement", null,
                                  new AccessToken(ACCESS_TOKEN, 82000L, "Bearer"), 200);
    Assert.assertEquals("0", response.getResponseBodyAsString());
  }

  @Test(expected = UnauthorizedException.class)
  public void testDeleteForbidden() throws Exception {
    URL url = getBaseURI().resolve("/api/testDeleteForbidden").toURL();
    HttpRequest request = HttpRequest.delete(url).build();
    restClient.execute(request, new AccessToken("Unknown", 82000L, "Bearer"));
  }

  @Test(expected = UnauthorizedException.class)
  public void testGetForbidden() throws Exception {
    URL url = getBaseURI().resolve("/api/testGetForbidden").toURL();
    HttpRequest request = HttpRequest.get(url).build();
    restClient.execute(request, new AccessToken("Unknown", 82000L, "Bearer"));
  }

  @Test(expected = UnauthorizedException.class)
  public void testPutForbidden() throws Exception {
    URL url = getBaseURI().resolve("/api/testPutForbidden").toURL();
    HttpRequest request = HttpRequest.put(url).build();
    restClient.execute(request, new AccessToken("Unknown", 82000L, "Bearer"));
  }

  @Test(expected = UnauthorizedException.class)
  public void testPostForbidden() throws Exception {
    URL url = getBaseURI().resolve("/api/testPostForbidden").toURL();
    HttpRequest request = HttpRequest.post(url).build();
    restClient.execute(request, new AccessToken("Unknown", 82000L, "Bearer"));
  }

  private void verifyResponse(HttpResponse response, Matcher<Object> expectedResponseCode,
                              Matcher<Object> expectedMessage, Matcher<Object> expectedBody) {

    Assert.assertTrue("Response code - expected: " + expectedResponseCode.toString()
                        + " actual: " + response.getResponseCode(),
                      expectedResponseCode.matches(response.getResponseCode()));

    Assert.assertTrue("Response message - expected: " + expectedMessage.toString()
                        + " actual: " + response.getResponseMessage(),
                      expectedMessage.matches(response.getResponseMessage()));

    String actualResponseBody = new String(response.getResponseBody());
    Assert.assertTrue("Response body - expected: " + expectedBody.toString() + " actual: " + actualResponseBody,
                      expectedBody.matches(actualResponseBody));
  }

  private URI getBaseURI() throws URISyntaxException {
    InetSocketAddress bindAddress = httpService.getBindAddress();
    return new URI("http://" + bindAddress.getHostName() + ":" + bindAddress.getPort());
  }

  public final class TestHttpService extends AbstractIdleService {

    private final NettyHttpService httpService;

    public TestHttpService() {
      this.httpService = NettyHttpService.builder("rest-client-test")
        .setHost("localhost")
        .setHttpHandlers(new TestHandler())
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
      httpService.start();
    }

    @Override
    protected void shutDown() throws Exception {
      httpService.stop();
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("bindAddress", httpService.getBindAddress())
        .toString();
    }
  }

  @Path("/api")
  public final class TestHandler extends AbstractHttpHandler {
    private int unavailEnpointCount = 0;
    private int integer = 0;

    private final String message = new UnauthorizedException(
      new Principal("test", Principal.PrincipalType.USER), ImmutableSet.of(Action.READ, Action.WRITE),
      NamespaceId.DEFAULT).getMessage();

    @POST
    @Path("/testCount")
    public void testIncrement(FullHttpRequest request, HttpResponder responder) throws Exception {
      Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8);
      String content = (new Gson()).fromJson(reader, String.class);
      switch (content) {
        case "increment":
          responder.sendString(HttpResponseStatus.OK, String.valueOf(++integer));
          break;
        case "decrement":
          responder.sendString(HttpResponseStatus.OK, String.valueOf(--integer));
          break;
        default:
          responder.sendString(HttpResponseStatus.OK, String.valueOf(integer));
          break;
      }
    }

    @POST
    @Path("/testPostAuth")
    public void testPostAuth(FullHttpRequest request, HttpResponder responder) throws Exception {
      String authHeaderVal = request.headers().get(HttpHeaders.AUTHORIZATION);
      if (("Bearer " + ACCESS_TOKEN).equals(authHeaderVal)) {
        responder.sendString(HttpResponseStatus.OK, "Access token received: "
          + request.headers().get(HttpHeaders.AUTHORIZATION).replace("Bearer ", ""));
      } else {
        responder.sendString(HttpResponseStatus.UNAUTHORIZED, "Access token received: Unknown");
      }
    }

    @PUT
    @Path("/testPutAuth")
    public void testPutAuth(FullHttpRequest request,
                            HttpResponder responder) throws Exception {
      String authHeaderVal = request.headers().get(HttpHeaders.AUTHORIZATION);
      if (("Bearer " + ACCESS_TOKEN).equals(authHeaderVal)) {
        responder.sendString(HttpResponseStatus.OK, "Access token received: "
          + request.headers().get(HttpHeaders.AUTHORIZATION).replace("Bearer ", ""));
      } else {
        responder.sendString(HttpResponseStatus.UNAUTHORIZED, "Access token received: Unknown");
      }
    }

    @DELETE
    @Path("/testDeleteAuth")
    public void testDeleteAuth(FullHttpRequest request,
                               HttpResponder responder) throws Exception {
      String authHeaderVal = request.headers().get(HttpHeaders.AUTHORIZATION);
      if (("Bearer " + ACCESS_TOKEN).equals(authHeaderVal)) {
        responder.sendString(HttpResponseStatus.OK, "Access token received: "
          + request.headers().get(HttpHeaders.AUTHORIZATION).replace("Bearer ", ""));
      } else {
        responder.sendString(HttpResponseStatus.UNAUTHORIZED, "Access token received: Unknown");
      }
    }

    @GET
    @Path("/testGetAuth")
    public void testGetAuth(FullHttpRequest request,
                            HttpResponder responder) throws Exception {
      String authHeaderVal = request.headers().get(HttpHeaders.AUTHORIZATION);
      if (("Bearer " + ACCESS_TOKEN).equals(authHeaderVal)) {
        responder.sendString(HttpResponseStatus.OK, "Access token received: "
          + request.headers().get(HttpHeaders.AUTHORIZATION).replace("Bearer ", ""));
      } else {
        responder.sendString(HttpResponseStatus.UNAUTHORIZED, "Access token received: Unknown");
      }
    }

    @PUT
    @Path("/testPutForbidden")
    public void testPutForbidden(FullHttpRequest request,
                                 HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.FORBIDDEN, message);
    }

    @POST
    @Path("/testPostForbidden")
    public void testPostForbidden(FullHttpRequest request,
                                  HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.FORBIDDEN, message);
    }

    @DELETE
    @Path("/testDeleteForbidden")
    public void testDeleteForbidden(FullHttpRequest request,
                                    HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.FORBIDDEN, message);
    }

    @GET
    @Path("/testGetForbidden")
    public void testGetForbidden(FullHttpRequest request,
                                 HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.FORBIDDEN, message);
    }

    @GET
    @Path("/testUnavail")
    public void testUnavail(FullHttpRequest request,
                            HttpResponder responder) throws Exception {
      unavailEnpointCount++;
      //Max number of calls to this endpoint should be 1 (Original request) + RETRY_LIMIT.
      if (unavailEnpointCount < (RETRY_LIMIT + 1)) {
        responder.sendStatus(HttpResponseStatus.SERVICE_UNAVAILABLE);
      } else if (unavailEnpointCount == (RETRY_LIMIT + 1)) {
        responder.sendStatus(HttpResponseStatus.OK);
      } else {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    }
  }
}
