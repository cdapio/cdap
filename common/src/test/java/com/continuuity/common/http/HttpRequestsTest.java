package com.continuuity.common.http;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.http.NettyHttpService;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.matcher.Matcher;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
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
    // TODO: commented out b/c netty-http appears to hang here
//    testDelete("/api/testWrongMethod", only(405), any(), any());
  }

  private void testPost(String path, Map<String, String> headers, String body, Matcher<Object> expectedResponseCode,
                        Matcher<Object> expectedMessage, Matcher<Object> expectedBody) throws Exception {

    HttpResponse response = HttpRequests.post(getBaseURI().resolve(path).toURL(), body, headers);
    verifyResponse(response, expectedResponseCode, expectedMessage, expectedBody);
  }

  private void testPost(String path, Matcher<Object> expectedResponseCode,
                        Matcher<Object> expectedMessage, Matcher<Object> expectedBody) throws Exception {

    testPost(path, ImmutableMap.<String, String>of(), null, expectedResponseCode, expectedMessage, expectedBody);
  }

  private void testPut(String path, Map<String, String> headers, String body, Matcher<Object> expectedResponseCode,
                        Matcher<Object> expectedMessage, Matcher<Object> expectedBody) throws Exception {

    HttpResponse response = HttpRequests.put(getBaseURI().resolve(path).toURL(), body, headers);
    verifyResponse(response, expectedResponseCode, expectedMessage, expectedBody);
  }

  private void testGet(String path, Matcher<Object> expectedResponseCode,
                       Matcher<Object> expectedMessage, Matcher<Object> expectedBody) throws Exception {

    HttpResponse response = HttpRequests.get(getBaseURI().resolve(path).toURL());
    verifyResponse(response, expectedResponseCode, expectedMessage, expectedBody);
  }

  private void testDelete(String path, Matcher<Object> expectedResponseCode,
                          Matcher<Object> expectedMessage, Matcher<Object> expectedBody) throws Exception {

    HttpResponse response = HttpRequests.delete(getBaseURI().resolve(path).toURL());
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

    String actualResponseBody = Bytes.toString(response.getResponseBody());
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

  @Path("/api")
  public final class TestHandler extends AbstractHttpHandler {

    @GET
    @Path("/testHttpStatus")
    public void testHttpStatus(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @GET
    @Path("/testOkWithResponse")
    public void testOkWithResponse(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.OK, "Great response");
    }

    @GET
    @Path("/testOkWithResponse201")
    public void testOkWithResponse201(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.CREATED, "Great response 201");
    }

    @GET
    @Path("/testOkWithResponse202")
    public void testOkWithResponse202(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.ACCEPTED, "Great response 202");
    }

    @GET
    @Path("/testOkWithResponse203")
    public void testOkWithResponse203(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.NON_AUTHORITATIVE_INFORMATION, "Great response 203");
    }

    @GET
    @Path("/testOkWithResponse204")
    public void testOkWithResponse204(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.NO_CONTENT);
    }

    @GET
    @Path("/testOkWithResponse205")
    public void testOkWithResponse205(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.RESET_CONTENT, "Great response 205");
    }

    @GET
    @Path("/testOkWithResponse206")
    public void testOkWithResponse206(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.PARTIAL_CONTENT, "Great response 206");
    }

    @GET
    @Path("/testBadRequest")
    public void testBadRequest(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
    }

    @GET
    @Path("/testBadRequestWithErrorMessage")
    public void testBadRequestWithErrorMessage(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Cool error message");
    }

    @GET
    @Path("/testConflict")
    public void testConflict(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.CONFLICT);
    }

    @GET
    @Path("/testConflictWithMessage")
    public void testConflictWithMessage(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.CONFLICT, "Conflictmes");
    }

    @POST
    @Path("/testHttpStatus")
    public void testHttpStatusPost(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @POST
    @Path("/testOkWithResponse")
    public void testOkWithResponsePost(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.OK, "Great response");
    }

    @POST
    @Path("/testOkWithResponse201")
    public void testOkWithResponse201Post(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.CREATED, "Great response 201");
    }

    @POST
    @Path("/testOkWithResponse202")
    public void testOkWithResponse202Post(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.ACCEPTED, "Great response 202");
    }

    @POST
    @Path("/testOkWithResponse203")
    public void testOkWithResponse203Post(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.NON_AUTHORITATIVE_INFORMATION, "Great response 203");
    }

    @POST
    @Path("/testOkWithResponse204")
    public void testOkWithResponse204Post(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.NO_CONTENT);
    }

    @POST
    @Path("/testOkWithResponse205")
    public void testOkWithResponse205Post(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.RESET_CONTENT, "Great response 205");
    }

    @POST
    @Path("/testOkWithResponse206")
    public void testOkWithResponse206Post(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.PARTIAL_CONTENT, "Great response 206");
    }

    @POST
    @Path("/testBadRequest")
    public void testBadRequestPost(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
    }

    @POST
    @Path("/testBadRequestWithErrorMessage")
    public void testBadRequestWithErrorMessagePost(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Cool error message");
    }

    @POST
    @Path("/testConflict")
    public void testConflictPost(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.CONFLICT);
    }

    @POST
    @Path("/testConflictWithMessage")
    public void testConflictWithMessagePost(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.CONFLICT, "Conflictmes");
    }

    @POST
    @Path("/testPost")
    public void testPost(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.OK,
                           request.getContent().toString(Charsets.UTF_8) + request.getHeader("sdf"));
    }

    @POST
    @Path("/testPost409")
    public void testPost409(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.CONFLICT, request.getContent().toString(Charsets.UTF_8)
        + request.getHeader("sdf") + "409");
    }

    @PUT
    @Path("/testPut")
    public void testPut(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.OK, request.getContent().toString(Charsets.UTF_8)
        + request.getHeader("sdf"));
    }

    @PUT
    @Path("/testPut409")
    public void testPut409(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendString(HttpResponseStatus.CONFLICT, request.getContent().toString(Charsets.UTF_8)
        + request.getHeader("sdf") + "409");
    }

    @DELETE
    @Path("/testDelete")
    public void testDelete(HttpRequest request, HttpResponder responder) throws Exception {
      responder.sendStatus(HttpResponseStatus.OK);
    }

//    @GET
//    @Path("/testWrongMethod")
//    public void testWrongMethod(HttpRequest request, HttpResponder responder) throws Exception {
//      responder.sendStatus(HttpResponseStatus.OK);
//    }
  }

}
