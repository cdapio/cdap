package com.continuuity.common.http.core;

import com.google.gson.JsonObject;
import org.apache.commons.io.IOUtils;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Test handler.
 */
@SuppressWarnings("UnusedParameters")
@Path("/test/v1")
public class TestHandler implements HttpHandler {
  @Path("resource")
  @GET
  public void testGet(HttpRequest request, HttpResponder responder){
    JsonObject object = new JsonObject();
    object.addProperty("status", "Handled get in resource end-point");
    responder.sendJson(HttpResponseStatus.OK, object);
  }

  @Path("tweets/{id}")
  @GET
  public void testGetTweet(HttpRequest request, HttpResponder responder, @PathParam("id") String id){
    JsonObject object = new JsonObject();
    object.addProperty("status", String.format("Handled get in tweets end-point, id: %s", id));
    responder.sendJson(HttpResponseStatus.OK, object);
  }

  @Path("tweets/{id}")
  @PUT
  public void testPutTweet(HttpRequest request, HttpResponder responder, @PathParam("id") String id){
    JsonObject object = new JsonObject();
    object.addProperty("status", String.format("Handled get in tweets end-point, id: %s", id));
    responder.sendJson(HttpResponseStatus.OK, object);
  }

  @Path("facebook/{id}/message")
  @DELETE
  public void testNoMethodRoute(HttpRequest request, HttpResponder responder, @PathParam("id") String id){

  }

  @Path("facebook/{id}/message")
  @PUT
  public void testPutMessage(HttpRequest request, HttpResponder responder, @PathParam("id") String id){
    String message = String.format("Handled put in tweets end-point, id: %s. ", id);
    try {
      String data = getStringContent(request);
      message = message.concat(String.format("Content: %s", data));
    } catch (IOException e) {
      //This condition should never occur
      assertTrue(false);
    }
    JsonObject object = new JsonObject();
    object.addProperty("result", message);
    responder.sendJson(HttpResponseStatus.OK, object);
  }

  @Path("facebook/{id}/message")
  @POST
  public void testPostMessage(HttpRequest request, HttpResponder responder, @PathParam("id") String id){
    String message = String.format("Handled post in tweets end-point, id: %s. ", id);
    try {
      String data = getStringContent(request);
      message = message.concat(String.format("Content: %s", data));
    } catch (IOException e) {
      //This condition should never occur
      assertTrue(false);
    }
    JsonObject object = new JsonObject();
    object.addProperty("result", message);
    responder.sendJson(HttpResponseStatus.OK, object);
  }

  @Path("/user/{userId}/message/{messageId}")
  @GET
  public void testMultipleParametersInPath(HttpRequest request, HttpResponder responder,
                                           @PathParam("userId") String userId,
                                           @PathParam("messageId") int messageId){
    JsonObject object = new JsonObject();
    object.addProperty("result", String.format("Handled multiple path parameters %s %d", userId, messageId));
    responder.sendJson(HttpResponseStatus.OK, object);
  }

  @Path("/message/{messageId}/user/{userId}")
  @GET
  public void testMultipleParametersInDifferentParameterDeclarationOrder(HttpRequest request, HttpResponder responder,
                                           @PathParam("userId") String userId,
                                           @PathParam("messageId") int messageId){
    JsonObject object = new JsonObject();
    object.addProperty("result", String.format("Handled multiple path parameters %s %d", userId, messageId));
    responder.sendJson(HttpResponseStatus.OK, object);
  }

  @Path("/NotRoutable/{id}")
  @GET
  public void notRoutableParameterMismatch(HttpRequest request,
                                           HttpResponder responder, @PathParam("userid") String userId){
    JsonObject object = new JsonObject();
    object.addProperty("result", String.format("Handled Not routable path %s ", userId));
    responder.sendJson(HttpResponseStatus.OK, object);
  }

  @Path("/NotRoutable/{userId}/message/{messageId}")
  @GET
  public void notRoutableMissingParameter(HttpRequest request, HttpResponder responder,
                                          @PathParam("userId") String userId, String messageId){
    JsonObject object = new JsonObject();
    object.addProperty("result", String.format("Handled Not routable path %s ", userId));
    responder.sendJson(HttpResponseStatus.OK, object);
  }

  @Path("/exception")
  @GET
  public void exception(HttpRequest request, HttpResponder responder) {
    throw new IllegalArgumentException("Illegal argument");
  }

  private String getStringContent(HttpRequest request) throws IOException {
    return IOUtils.toString(new ChannelBufferInputStream(request.getContent()));
  }

  @Override
  public void init(HandlerContext context) {}

  @Override
  public void destroy(HandlerContext context) {}
}
