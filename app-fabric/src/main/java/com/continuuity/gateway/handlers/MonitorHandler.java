package com.continuuity.gateway.handlers;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.ReactorServiceManager;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.util.AbstractAppFabricHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Monitor Handler returns the status of different discoverable services
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class MonitorHandler extends AbstractAppFabricHttpHandler {
  private final Map<String, ReactorServiceManager> reactorServiceManagementMap;
  private static final String STATUSOK = Constants.Monitor.STATUS_OK;
  private static final String STATUSNOTOK = Constants.Monitor.STATUS_NOTOK;
  private static final String NOTAPPLICABLE = "NA";
  private static final Gson GSON = new Gson();

  @Inject
  public MonitorHandler(Authenticator authenticator, Map<String, ReactorServiceManager> serviceMap) {
    super(authenticator);
    this.reactorServiceManagementMap = serviceMap;
  }

  /**
   * Stops Reactor Service
   */
  @Path("/system/services/{service-name}/stop")
  @POST
  public void stopService(final HttpRequest request, final HttpResponder responder,
                          @PathParam("service-name") String serviceName) {
    responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
  }

  /**
   * Starts Reactor Service
   */
  @Path("/system/services/{service-name}/start")
  @POST
  public void startService(final HttpRequest request, final HttpResponder responder,
                           @PathParam("service-name") String serviceName) {
    responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
  }

  /**
   * Returns the number of instances of Reactor Services
   */
  @Path("/system/services/{service-name}/instances")
  @GET
  public void getServiceInstance(final HttpRequest request, final HttpResponder responder,
                                 @PathParam("service-name") String serviceName) {
    JsonObject reply = new JsonObject();
    if (!reactorServiceManagementMap.containsKey(serviceName)) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, String.format("Invalid service name %s", serviceName));
      return;
    }
    ReactorServiceManager serviceManager = reactorServiceManagementMap.get(serviceName);
    if (serviceManager.isServiceEnabled()) {
      String requestedInstances = String.valueOf(serviceManager.getRequestedInstances());
      String provisionedInstances = String.valueOf(serviceManager.getProvisionedInstances());
      reply.addProperty("requested", requestedInstances);
      reply.addProperty("provisioned", provisionedInstances);
      responder.sendJson(HttpResponseStatus.OK, reply);
    } else {
      responder.sendString(HttpResponseStatus.FORBIDDEN, String.format("Service %s is not enabled", serviceName));
    }
  }

  /**
   * Sets the number of instances of Reactor Services
   */
  @Path("/system/services/{service-name}/instances")
  @PUT
  public void setServiceInstance(final HttpRequest request, final HttpResponder responder,
                                 @PathParam("service-name") String serviceName) {
    try {
      if (!reactorServiceManagementMap.containsKey(serviceName)) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, String.format("Invalid service name %s", serviceName));
        return;
      }
      ReactorServiceManager serviceManager = reactorServiceManagementMap.get(serviceName);
      if (!serviceManager.isServiceEnabled()) {
        responder.sendString(HttpResponseStatus.FORBIDDEN, String.format("Service %s is not enabled", serviceName));
        return;
      }
      int instance = getInstances(request);
      if (instance < serviceManager.getMinInstances() || instance > serviceManager.getMaxInstances()) {
        String response = String.format("Instance count should be between [%s,%s]", serviceManager.getMinInstances(),
                                        serviceManager.getMaxInstances());
        responder.sendString(HttpResponseStatus.BAD_REQUEST, response);
        return;
      }
      if (serviceManager.setInstances(instance)) {
        responder.sendStatus(HttpResponseStatus.OK);
      } else {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Operation Not Valid for this service");
      }
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           String.format("Error updating instances for service: %s", serviceName));
    }
  }

  //Return the status of reactor services in JSON format
  @Path("/system/services/status")
  @GET
  public void getBootStatus(final HttpRequest request, final HttpResponder responder) {
    Map<String, String> result = new HashMap<String, String>();
    for (String service : reactorServiceManagementMap.keySet()) {
      ReactorServiceManager reactorServiceManager = reactorServiceManagementMap.get(service);
      if (reactorServiceManager.isServiceEnabled() && reactorServiceManager.canCheckStatus()) {
        String status = reactorServiceManager.isServiceAvailable() ? STATUSOK : STATUSNOTOK;
        result.put(service, status);
      }
    }
    responder.sendJson(HttpResponseStatus.OK, result);
  }

  @Path("/system/services/{service-name}/status")
  @GET
  public void monitor(final HttpRequest request, final HttpResponder responder,
                      @PathParam("service-name") final String serviceName) {
    if (!reactorServiceManagementMap.containsKey(serviceName)) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, String.format("Invalid service name %s", serviceName));
      return;
    }
    ReactorServiceManager reactorServiceManager = reactorServiceManagementMap.get(serviceName);
    if (!reactorServiceManager.isServiceEnabled()) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, String.format("Service %s is not enabled", serviceName));
      return;
    }
    if (reactorServiceManager.canCheckStatus() && reactorServiceManager.isServiceAvailable()) {
      responder.sendString(HttpResponseStatus.OK, STATUSOK);
    } else if (reactorServiceManager.canCheckStatus()) {
      responder.sendString(HttpResponseStatus.OK, STATUSNOTOK);
    } else {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Operation not valid for this service");
    }
  }

  @Path("/system/services")
  @GET
  public void getServiceSpec(final HttpRequest request, final HttpResponder responder) {
    List<JsonObject> serviceSpec = new ArrayList<JsonObject>();
    SortedSet<String> services = new TreeSet<String>(reactorServiceManagementMap.keySet());
    List<String> serviceList = new ArrayList<String>(services);
    for (String service : serviceList) {
      ReactorServiceManager serviceManager = reactorServiceManagementMap.get(service);
      if (serviceManager.isServiceEnabled()) {
        String logs = serviceManager.isLogAvailable() ? Constants.Monitor.STATUS_OK : Constants.Monitor.STATUS_NOTOK;
        String canCheck = serviceManager.canCheckStatus() ? (
          serviceManager.isServiceAvailable() ? STATUSOK : STATUSNOTOK) : NOTAPPLICABLE;
        String minInstance = String.valueOf(serviceManager.getMinInstances());
        String maxInstance = String.valueOf(serviceManager.getMaxInstances());
        String reqInstance = String.valueOf(serviceManager.getRequestedInstances());
        String provInstance = String.valueOf(serviceManager.getProvisionedInstances());
        JsonObject reply = new JsonObject();
        reply.addProperty("name", service);
        reply.addProperty("logs", logs);
        reply.addProperty("status", canCheck);
        reply.addProperty("min", minInstance);
        reply.addProperty("max", maxInstance);
        reply.addProperty("requested", reqInstance);
        reply.addProperty("provisioned", provInstance);
        //TODO: Add metric name for Event Rate monitoring
        serviceSpec.add(reply);
      }
    }
    responder.sendJson(HttpResponseStatus.OK, serviceSpec);
  }
}
