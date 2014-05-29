package com.continuuity.gateway.handlers;

import com.continuuity.common.conf.Constants;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;



/**
 *  Handler class for Twill User Apps
 *  TODO: Currently this is a Mock API, Implementation will be added in next steps.
 *  Will Extend AppFabricHelper, which will have the required common methods from AppFabricHttpHandler, once that
 *  is merged
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class ServiceHttpHandler extends AbstractHttpHandler {

  /**
   * Constructs an new instance. Parameters are binded by Guice.
   */
  @Inject
  public ServiceHttpHandler() {

  }

  /**
   * Return the list of user twill apps for an application
   */
  @Path("/apps/{app-id}/services")
  @GET
  public void listServices(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") String appId) {
    List<String> result = Lists.newArrayList();
    result.add("LocationService");
    result.add("MaskService");
    responder.sendJson(HttpResponseStatus.OK, result);
  }

  /**
   * Return the runtimeargs for the specified twill app
   */
  @GET
  @Path("/apps/{app-id}/services/{service-id}/runnables/{runnable-id}/runtimeargs")
  public void getRuntimeArgs(HttpRequest request, HttpResponder responder,
                             @PathParam("app-id") String appId,
                             @PathParam("runnable-id") String runId) {
    Map<String, String> runtimeArgs = Maps.newHashMap();
    runtimeArgs.put("threshold", "100");
    responder.sendJson(HttpResponseStatus.OK, runtimeArgs);
  }

  /**
   * save the runtimeargs for the twill app
   */
  @PUT
  @Path("/apps/{app-id}/services/{service-id}/runnables/{runnable-id}/runtimeargs")
  public void setRuntimeArgs(HttpRequest request, HttpResponder responder,
                             @PathParam("app-id") String appId,
                             @PathParam("runnable-id") String runId) {
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Return the number of instances for user twill app
   */
  @GET
  @Path("/apps/{app-id}/services/{service-id}/runnables/{runnable-id}/instances")
  public void getInstances(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") String appId,
                           @PathParam("runnable-id") String runId) {
    JsonObject reply = new JsonObject();
    reply.addProperty("instances", 3);
    responder.sendJson(HttpResponseStatus.OK, reply);
  }

  /**
   * set instances
   */
  @PUT
  @Path("/apps/{app-id}/services/{service-id}/runnables/{runnable-id}/instances")
  public void setInstances(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") String appId,
                           @PathParam("runnable-id") String runId) {
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * start twill app
   */
  @POST
  @Path("/apps/{app-id}/services/{service-id}/runnables/start")
  public void start(HttpRequest request, HttpResponder responder,
                    @PathParam("app-id") String appId) {
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * stop twill app
   */
  @POST
  @Path("/apps/{app-id}/services/{service-id}/runnables/stop")
  public void stop(HttpRequest request, HttpResponder responder,
                   @PathParam("app-id") String appId) {
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * twill app status
   */
  @GET
  @Path("/apps/{app-id}/services/{service-id}/runnables/status")
  public void status(HttpRequest request, HttpResponder responder,
                     @PathParam("app-id") String appId) {
    JsonObject object = new JsonObject();
    object.addProperty("status", "RUNNING");
    responder.sendJson(HttpResponseStatus.OK, object);
  }

  /**
   * live info of a twill app
   */
  @GET
  @Path("/apps/{app-id}/services/{service-id}/runnables/{runnable-id}/live-info")
  public void liveInfo(HttpRequest request, HttpResponder responder,
                       @PathParam("app-id") String appId,
                       @PathParam("runnable-id") String runId) {
    JsonObject reply = new JsonObject();
    reply.addProperty("app", appId);
    reply.addProperty("type", "twill");
    reply.addProperty("id", "sampleTwill");
    responder.sendJson(HttpResponseStatus.OK, reply);
  }

}
