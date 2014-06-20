package com.continuuity.gateway.handlers;

import com.continuuity.api.service.ServiceSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.OperationException;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.util.AbstractAppFabricHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.apache.twill.api.RuntimeSpecification;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 *  Handler class for Twill User Apps
 *  TODO: Currently this is a Mock API, Implementation will be added in next steps.
 *  Will Extend AbstractAppFabricHttpHandler, which will have the required common methods from
 *  AppFabricHttpHandler, once that is merged
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class ServiceHttpHandler extends AbstractAppFabricHttpHandler {

  private final Store store;
  private static final Logger LOG = LoggerFactory.getLogger(ServiceHttpHandler.class);
  /**
   * Constructs an new instance. Parameters are binded by Guice.
   */
  @Inject
  public ServiceHttpHandler(Authenticator authenticator, StoreFactory storeFactory) {
    super(authenticator);
    this.store = storeFactory.create();
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
   * Return the number of instances for the given runnable of a service.
   */
  @GET
  @Path("/apps/{app-id}/services/{service-name}/runnables/{runnable-id}/instances")
  public void getInstances(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") String appId,
                           @PathParam("service-name") String serviceName,
                           @PathParam("runnable-id") String runId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      RuntimeSpecification specification = getRuntimeSpecification(accountId, appId, serviceName, runId);
      if (specification == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      } else {
        JsonObject reply = new JsonObject();
        reply.addProperty("requested", specification.getResourceSpecification().getInstances());
        reply.addProperty("provisioned", specification.getResourceSpecification().getInstances());
        responder.sendJson(HttpResponseStatus.OK, reply);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Set instances.
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
   * twill app history
   */
  @GET
  @Path("/apps/{app-id}/services/{service-id}/runnables/{runnable-id}/history")
  public void history(HttpRequest request, HttpResponder responder,
                      @PathParam("app-id") String appId) {
    JsonArray history = new JsonArray();
    JsonObject object = new JsonObject();
    object.addProperty("runid", "123");
    object.addProperty("start", "75644443");
    object.addProperty("end", "75644499");
    object.addProperty("status", "STOPPED");
    history.add(object);

    responder.sendJson(HttpResponseStatus.OK, history);
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

  @Nullable
  private ServiceSpecification getServiceSpecification(String accountId, String id,
                                                       String serviceName) throws OperationException {
    Id.Application appId = Id.Application.from(accountId, id);
    ApplicationSpecification applicationSpecification = store.getApplication(appId);
    Map<String, ServiceSpecification> serviceSpecs = applicationSpecification.getServices();
    if (serviceSpecs.containsKey(serviceName)) {
      return serviceSpecs.get(serviceName);
    } else {
      return null;
    }
  }

  @Nullable
  private RuntimeSpecification getRuntimeSpecification (String accountId, String appId, String serviceName,
                                                        String runnableName) throws OperationException {
    ServiceSpecification specification = getServiceSpecification(accountId, appId, serviceName);
    if (specification != null) {
      Map<String, RuntimeSpecification> runtimeSpecs =  specification.getRunnables();
      return runtimeSpecs.containsKey(runnableName) ? runtimeSpecs.get(runnableName) : null;
    } else {
      return null;
    }
  }

}
