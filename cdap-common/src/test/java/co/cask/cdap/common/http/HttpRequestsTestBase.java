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

package co.cask.cdap.common.http;

import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.inject.matcher.Matcher;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

import static com.google.inject.matcher.Matchers.any;
import static com.google.inject.matcher.Matchers.only;

/**
 * Test for {@link HttpRequests}.
 */
public abstract class HttpRequestsTestBase {

  protected abstract URI getBaseURI() throws URISyntaxException;

  protected abstract HttpRequestConfig getHttpRequestsConfig();

  @Test
  public void testHttpStatus() throws Exception {
    testGet("/fake/fake", only(404), only("Not Found"), only("Problem accessing: /fake/fake. Reason: Not Found"));
    testGet("/api/testOkWithResponse", only(200), any(), only("Great response"));
    testGet("/api/testOkWithResponse201", only(201), any(), only("Great response 201"));
    testGet("/api/testOkWithResponse202", only(202), any(), only("Great response 202"));
    testGet("/api/testOkWithResponse203", only(203), any(), only("Great response 203"));
    testGet("/api/testOkWithResponse204", only(204), any(), only(""));
    testGet("/api/testOkWithResponse205", only(205), any(), only("Great response 205"));
    testGet("/api/testOkWithResponse206", only(206), any(), only("Great response 206"));
    testGet("/api/testHttpStatus", only(200), only("OK"), only(""));
    testGet("/api/testBadRequest", only(400), only("Bad Request"), only(""));
    testGet("/api/testBadRequestWithErrorMessage", only(400), only("Bad Request"), only("Cool error message"));
    testGet("/api/testConflict", only(409), only("Conflict"), only(""));
    testGet("/api/testConflictWithMessage", only(409), only("Conflict"), only("Conflictmes"));

    testPost("/fake/fake", only(404), only("Not Found"), only("Problem accessing: /fake/fake. Reason: Not Found"));
    testPost("/api/testOkWithResponse", only(200), any(), only("Great response"));
    testPost("/api/testOkWithResponse201", only(201), any(), only("Great response 201"));
    testPost("/api/testOkWithResponse202", only(202), any(), only("Great response 202"));
    testPost("/api/testOkWithResponse203", only(203), any(), only("Great response 203"));
    testPost("/api/testOkWithResponse204", only(204), any(), only(""));
    testPost("/api/testOkWithResponse205", only(205), any(), only("Great response 205"));
    testPost("/api/testOkWithResponse206", only(206), any(), only("Great response 206"));
    testPost("/api/testHttpStatus", only(200), only("OK"), only(""));
    testPost("/api/testBadRequest", only(400), only("Bad Request"), only(""));
    testPost("/api/testBadRequestWithErrorMessage", only(400), only("Bad Request"), only("Cool error message"));
    testPost("/api/testConflict", only(409), only("Conflict"), only(""));
    testPost("/api/testConflictWithMessage", only(409), only("Conflict"), only("Conflictmes"));

    testPost("/api/testPost", ImmutableMap.of("sdf", "123zz"), "somebody", only(200), any(), only("somebody123zz"));
    testPost("/api/testPost409", ImmutableMap.of("sdf", "123zz"), "somebody", only(409), any(),
             only("somebody123zz409"));

    testPut("/api/testPut", ImmutableMap.of("sdf", "123zz"), "somebody", only(200), any(), only("somebody123zz"));
    testPut("/api/testPut409", ImmutableMap.of("sdf", "123zz"), "somebody", only(409), any(), only("somebody123zz409"));

    testDelete("/api/testDelete", only(200), any(), any());
  }

  private void testPost(String path, Map<String, String> headers, String body, Matcher<Object> expectedResponseCode,
                        Matcher<Object> expectedMessage, Matcher<Object> expectedBody) throws Exception {

    URL url = getBaseURI().resolve(path).toURL();
    HttpRequest request = HttpRequest.post(url).addHeaders(headers).withBody(body).build();
    HttpResponse response = HttpRequests.execute(request, getHttpRequestsConfig());
    verifyResponse(response, expectedResponseCode, expectedMessage, expectedBody);
  }

  private void testPost(String path, Matcher<Object> expectedResponseCode,
                        Matcher<Object> expectedMessage, Matcher<Object> expectedBody) throws Exception {

    testPost(path, ImmutableMap.<String, String>of(), "", expectedResponseCode, expectedMessage, expectedBody);
  }

  private void testPut(String path, Map<String, String> headers, String body, Matcher<Object> expectedResponseCode,
                        Matcher<Object> expectedMessage, Matcher<Object> expectedBody) throws Exception {

    URL url = getBaseURI().resolve(path).toURL();
    HttpRequest request = HttpRequest.put(url).addHeaders(headers).withBody(body).build();
    HttpResponse response = HttpRequests.execute(request, getHttpRequestsConfig());
    verifyResponse(response, expectedResponseCode, expectedMessage, expectedBody);
  }

  private void testGet(String path, Matcher<Object> expectedResponseCode,
                       Matcher<Object> expectedMessage, Matcher<Object> expectedBody) throws Exception {

    URL url = getBaseURI().resolve(path).toURL();
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request, getHttpRequestsConfig());
    verifyResponse(response, expectedResponseCode, expectedMessage, expectedBody);
  }

  private void testDelete(String path, Matcher<Object> expectedResponseCode,
                          Matcher<Object> expectedMessage, Matcher<Object> expectedBody) throws Exception {

    URL url = getBaseURI().resolve(path).toURL();
    HttpRequest request = HttpRequest.delete(url).build();
    HttpResponse response = HttpRequests.execute(request, getHttpRequestsConfig());
    verifyResponse(response, expectedResponseCode, expectedMessage, expectedBody);
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

  @Path("/api")
  public static final class TestHandler extends AbstractHttpHandler {

    @GET
    @Path("/testHttpStatus")
    public void testHttpStatus(org.jboss.netty.handler.codec.http.HttpRequest request,
                               HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @GET
    @Path("/testOkWithResponse")
    public void testOkWithResponse(org.jboss.netty.handler.codec.http.HttpRequest request,
                                   HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.OK, "Great response");
    }

    @GET
    @Path("/testOkWithResponse201")
    public void testOkWithResponse201(org.jboss.netty.handler.codec.http.HttpRequest request,
                                      HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.CREATED, "Great response 201");
    }

    @GET
    @Path("/testOkWithResponse202")
    public void testOkWithResponse202(org.jboss.netty.handler.codec.http.HttpRequest request,
                                      HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.ACCEPTED, "Great response 202");
    }

    @GET
    @Path("/testOkWithResponse203")
    public void testOkWithResponse203(org.jboss.netty.handler.codec.http.HttpRequest request,
                                      HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.NON_AUTHORITATIVE_INFORMATION, "Great response 203");
    }

    @GET
    @Path("/testOkWithResponse204")
    public void testOkWithResponse204(org.jboss.netty.handler.codec.http.HttpRequest request,
                                      HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.NO_CONTENT);
    }

    @GET
    @Path("/testOkWithResponse205")
    public void testOkWithResponse205(org.jboss.netty.handler.codec.http.HttpRequest request,
                                      HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.RESET_CONTENT, "Great response 205");
    }

    @GET
    @Path("/testOkWithResponse206")
    public void testOkWithResponse206(org.jboss.netty.handler.codec.http.HttpRequest request,
                                      HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.PARTIAL_CONTENT, "Great response 206");
    }

    @GET
    @Path("/testBadRequest")
    public void testBadRequest(org.jboss.netty.handler.codec.http.HttpRequest request,
                               HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
    }

    @GET
    @Path("/testBadRequestWithErrorMessage")
    public void testBadRequestWithErrorMessage(org.jboss.netty.handler.codec.http.HttpRequest request,
                                               HttpResponder responder) throws Exception {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Cool error message");
    }

    @GET
    @Path("/testConflict")
    public void testConflict(org.jboss.netty.handler.codec.http.HttpRequest request,
                             HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.CONFLICT);
    }

    @GET
    @Path("/testConflictWithMessage")
    public void testConflictWithMessage(org.jboss.netty.handler.codec.http.HttpRequest request,
                                        HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.CONFLICT, "Conflictmes");
    }

    @POST
    @Path("/testHttpStatus")
    public void testHttpStatusPost(org.jboss.netty.handler.codec.http.HttpRequest request,
                                   HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @POST
    @Path("/testOkWithResponse")
    public void testOkWithResponsePost(org.jboss.netty.handler.codec.http.HttpRequest request,
                                       HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.OK, "Great response");
    }

    @POST
    @Path("/testOkWithResponse201")
    public void testOkWithResponse201Post(org.jboss.netty.handler.codec.http.HttpRequest request,
                                          HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.CREATED, "Great response 201");
    }

    @POST
    @Path("/testOkWithResponse202")
    public void testOkWithResponse202Post(org.jboss.netty.handler.codec.http.HttpRequest request,
                                          HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.ACCEPTED, "Great response 202");
    }

    @POST
    @Path("/testOkWithResponse203")
    public void testOkWithResponse203Post(org.jboss.netty.handler.codec.http.HttpRequest request,
                                          HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.NON_AUTHORITATIVE_INFORMATION, "Great response 203");
    }

    @POST
    @Path("/testOkWithResponse204")
    public void testOkWithResponse204Post(org.jboss.netty.handler.codec.http.HttpRequest request,
                                          HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.NO_CONTENT);
    }

    @POST
    @Path("/testOkWithResponse205")
    public void testOkWithResponse205Post(org.jboss.netty.handler.codec.http.HttpRequest request,
                                          HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.RESET_CONTENT, "Great response 205");
    }

    @POST
    @Path("/testOkWithResponse206")
    public void testOkWithResponse206Post(org.jboss.netty.handler.codec.http.HttpRequest request,
                                          HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.PARTIAL_CONTENT, "Great response 206");
    }

    @POST
    @Path("/testBadRequest")
    public void testBadRequestPost(org.jboss.netty.handler.codec.http.HttpRequest request,
                                   HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
    }

    @POST
    @Path("/testBadRequestWithErrorMessage")
    public void testBadRequestWithErrorMessagePost(org.jboss.netty.handler.codec.http.HttpRequest request,
                                                   HttpResponder responder) throws Exception {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Cool error message");
    }

    @POST
    @Path("/testConflict")
    public void testConflictPost(org.jboss.netty.handler.codec.http.HttpRequest request,
                                 HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.CONFLICT);
    }

    @POST
    @Path("/testConflictWithMessage")
    public void testConflictWithMessagePost(org.jboss.netty.handler.codec.http.HttpRequest request,
                                            HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.CONFLICT, "Conflictmes");
    }

    @POST
    @Path("/testPost")
    public void testPost(org.jboss.netty.handler.codec.http.HttpRequest request,
                         HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.OK,
                           request.getContent().toString(Charsets.UTF_8) + request.getHeader("sdf"));
    }

    @POST
    @Path("/testPost409")
    public void testPost409(org.jboss.netty.handler.codec.http.HttpRequest request,
                            HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.CONFLICT, request.getContent().toString(Charsets.UTF_8)
        + request.getHeader("sdf") + "409");
    }

    @PUT
    @Path("/testPut")
    public void testPut(org.jboss.netty.handler.codec.http.HttpRequest request,
                        HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.OK, request.getContent().toString(Charsets.UTF_8)
        + request.getHeader("sdf"));
    }

    @PUT
    @Path("/testPut409")
    public void testPut409(org.jboss.netty.handler.codec.http.HttpRequest request,
                           HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.CONFLICT, request.getContent().toString(Charsets.UTF_8)
        + request.getHeader("sdf") + "409");
    }

    @DELETE
    @Path("/testDelete")
    public void testDelete(org.jboss.netty.handler.codec.http.HttpRequest request,
                           HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @GET
    @Path("/testWrongMethod")
    public void testWrongMethod(org.jboss.netty.handler.codec.http.HttpRequest request,
                                HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

}
