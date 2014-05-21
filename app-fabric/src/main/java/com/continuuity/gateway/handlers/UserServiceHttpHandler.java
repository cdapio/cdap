package com.continuuity.gateway.handlers;

import com.continuuity.common.conf.Constants;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.util.List;
import java.util.Map;


/**
 *  Handler class for Twill User Apps
 *  TODO: Currently this is a Mock API, Implmentation will be added in next steps.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class UserServiceHttpHandler extends AbstractHttpHandler {

  /**
   * Constructs an new instance. Parameters are binded by Guice.
   */
  @Inject
  public UserServiceHttpHandler() {

  }

  /**Return the list of user twill apps for an application
   *
   * @param request
   * @param responder
   * @param appId
   */
  @Path("/user/{app-id}/")
  @GET
  public void getUserApps(final HttpRequest request, final HttpResponder responder,
                          @PathParam("app-id") final String appId) {
    List<String> result = Lists.newArrayList();
    result.add("LocationService");
    result.add("MaskService");
    String json;
    json = (new Gson()).toJson(result);
    responder.sendByteArray(HttpResponseStatus.OK, json.getBytes(Charsets.UTF_8),
                            ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
  }

  /**
   * Return the runtimeargs for the specified twill app
   * @param request
   * @param responder
   * @param appId
   */
  @GET
  @Path("/user/{app-id}/runtimeargs")
  public void getRunnableRuntimeArgs(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") final String appId) {
      Map<String, String> runtimeArgs = Maps.newHashMap();
      runtimeArgs.put("threshold", "100");
      responder.sendJson(HttpResponseStatus.OK, runtimeArgs);
  }

  /**
   * save the runtimeargs for the twill app
   * @param request
   * @param responder
   * @param appId
   */
  @PUT
  @Path("/user/{app-id}/runtimeargs")
  public void saveRunnableRuntimeArgs(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") final String appId) {
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Return the number of instances for user twill app
   * @param request
   * @param responder
   * @param appId
   */
  @GET
  @Path("/user/{app-id}/instances")
  public void getUserAppInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") final String appId) {
    JsonObject reply = new JsonObject();
    reply.addProperty("instances", 3);
    responder.sendJson(HttpResponseStatus.OK, reply);
  }

  /**
   * set instances
   * @param request
   * @param responder
   * @param appId
   */
  @PUT
  @Path("/user/{app-id}/instances")
  public void setUserAppInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") final String appId) {
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * start twill app
   * @param request
   * @param responder
   * @param appId
   */
  @POST
  @Path("/user/{app-id}/start")
  public void startApp(HttpRequest request, HttpResponder responder,
                       @PathParam("app-id") final String appId) {
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * stop twill app
   * @param request
   * @param responder
   * @param appId
   */
  @POST
  @Path("/user/{app-id}/stop")
  public void stopApp(HttpRequest request, HttpResponder responder,
                       @PathParam("app-id") final String appId) {
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * twill app status
   * @param request
   * @param responder
   * @param appId
   */
  @GET
  @Path("/user/{app-id}/status")
  public void appStatus(HttpRequest request, HttpResponder responder,
                      @PathParam("app-id") final String appId) {
    JsonObject reply = new JsonObject();
    reply.addProperty("status", "RUNNING");
    responder.sendJson(HttpResponseStatus.OK, reply);
  }

  /**
   * live info of a twill app
   * @param request
   * @param responder
   * @param appId
   */
  @GET
  @Path("/user/{app-id}/live-info")
  public void liveInfo(HttpRequest request, HttpResponder responder,
                       @PathParam("app-id") final String appId) {
    JsonObject reply = new JsonObject();
    reply.addProperty("app", appId);
    reply.addProperty("type", "twill");
    reply.addProperty("id", "sampleTwill");
    responder.sendJson(HttpResponseStatus.OK, reply);
  }

}
