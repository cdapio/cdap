/*
 * Copyright 2014 Cask Data, Inc.
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


package co.cask.cdap.reactor.client.util;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.http.HttpRequest;
import co.cask.cdap.common.http.HttpResponse;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.security.authentication.client.AuthenticationClient;
import co.cask.cdap.security.authentication.client.basic.BasicAuthenticationClient;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.matcher.Matcher;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
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

  private TestHttpService httpService;
  private RESTClient restClient;

  @Before
  public void setUp() {
    AuthenticationClient authenticationClient = new BasicAuthenticationClient();
    ClientConfig clientConfig = new ClientConfig("loclhost", authenticationClient);
    restClient = RESTClient.create(clientConfig);
    httpService = new TestHttpService();
    httpService.startAndWait();
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
    verifyResponse(response, only(200),  any(),  only("Access token received: " + ACCESS_TOKEN));
  }

  @Test(expected = UnAuthorizedAccessTokenException.class)
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
    verifyResponse(response, only(200),  any(),  only("Access token received: " + ACCESS_TOKEN));
  }

  @Test(expected = UnAuthorizedAccessTokenException.class)
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
    verifyResponse(response, only(200),  any(),  only("Access token received: " + ACCESS_TOKEN));
  }

  @Test(expected = UnAuthorizedAccessTokenException.class)
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
    verifyResponse(response, only(200),  any(),  only("Access token received: " + ACCESS_TOKEN));
  }

  @Test(expected = UnAuthorizedAccessTokenException.class)
  public void testDeleteUnauthorizedWithAccessToken() throws Exception {
    URL url = getBaseURI().resolve("/api/testDeleteAuth").toURL();
    HttpRequest request = HttpRequest.delete(url).build();
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
    Assert.assertTrue("Response body - expected: " + expectedBody.toString()
                        + " actual: " + actualResponseBody,
                      expectedBody.matches(actualResponseBody));
  }

  private URI getBaseURI() throws URISyntaxException {
    InetSocketAddress bindAddress = httpService.getBindAddress();
    return new URI("http://" + bindAddress.getHostName() + ":" + bindAddress.getPort());
  }

  public final class TestHttpService extends AbstractIdleService {

    private final NettyHttpService httpService;

    public TestHttpService() {
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

  @Path("/api")
  public final class TestHandler extends AbstractHttpHandler {
    @POST
    @Path("/testPostAuth")
    public void testPostAuth(org.jboss.netty.handler.codec.http.HttpRequest request,
                             HttpResponder responder) throws Exception {
      String authHeaderVal = request.getHeader(HttpHeaders.AUTHORIZATION);
      if (("Bearer " + ACCESS_TOKEN).equals(authHeaderVal)) {
        responder.sendString(HttpResponseStatus.OK, "Access token received: "
          + request.getHeader(HttpHeaders.AUTHORIZATION).replace("Bearer ", StringUtils.EMPTY));
      } else {
        responder.sendString(HttpResponseStatus.UNAUTHORIZED, "Access token received: Unknown");
      }
    }

    @PUT
    @Path("/testPutAuth")
    public void testPutAuth(org.jboss.netty.handler.codec.http.HttpRequest request,
                            HttpResponder responder) throws Exception {
      String authHeaderVal = request.getHeader(HttpHeaders.AUTHORIZATION);
      if (("Bearer " + ACCESS_TOKEN).equals(authHeaderVal)) {
        responder.sendString(HttpResponseStatus.OK, "Access token received: "
          + request.getHeader(HttpHeaders.AUTHORIZATION).replace("Bearer ", StringUtils.EMPTY));
      } else {
        responder.sendString(HttpResponseStatus.UNAUTHORIZED, "Access token received: Unknown");
      }
    }

    @DELETE
    @Path("/testDeleteAuth")
    public void testDeleteAuth(org.jboss.netty.handler.codec.http.HttpRequest request,
                               HttpResponder responder) throws Exception {
      String authHeaderVal = request.getHeader(HttpHeaders.AUTHORIZATION);
      if (("Bearer " + ACCESS_TOKEN).equals(authHeaderVal)) {
        responder.sendString(HttpResponseStatus.OK, "Access token received: "
          + request.getHeader(HttpHeaders.AUTHORIZATION).replace("Bearer ", StringUtils.EMPTY));
      } else {
        responder.sendString(HttpResponseStatus.UNAUTHORIZED, "Access token received: Unknown");
      }
    }

    @GET
    @Path("/testGetAuth")
    public void testGetAuth(org.jboss.netty.handler.codec.http.HttpRequest request,
                            HttpResponder responder) throws Exception {
      String authHeaderVal = request.getHeader(HttpHeaders.AUTHORIZATION);
      if (("Bearer " + ACCESS_TOKEN).equals(authHeaderVal)) {
        responder.sendString(HttpResponseStatus.OK, "Access token received: "
          + request.getHeader(HttpHeaders.AUTHORIZATION).replace("Bearer ", StringUtils.EMPTY));
      } else {
        responder.sendString(HttpResponseStatus.UNAUTHORIZED, "Access token received: Unknown");
      }
    }
  }
}
