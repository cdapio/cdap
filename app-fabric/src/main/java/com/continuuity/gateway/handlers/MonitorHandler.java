package com.continuuity.gateway.handlers;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.twill.ReactorServiceManager;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.util.AbstractAppFabricHttpHandler;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMultimap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
  private final String OK = Constants.Monitor.STATUS_OK;
  private final String NOTOK = Constants.Monitor.STATUS_NOTOK;

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
    if (reactorServiceManagementMap.containsKey(serviceName)) {
      int instances = reactorServiceManagementMap.get(serviceName).getInstances();
      responder.sendString(HttpResponseStatus.OK, String.valueOf(instances));
    } else {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid Service Name");
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
      ReactorServiceManager serviceManager = reactorServiceManagementMap.get(serviceName);
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
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           "Invalid Service Name Or Operation Not Valid for this service");
    }
  }

  //Return the status of reactor services in JSON format
  @Path("/system/services/status")
  @GET
  public void getBootStatus(final HttpRequest request, final HttpResponder responder) {
    Map<String, String> result = new HashMap<String, String>();
    String json;
    for (String service : reactorServiceManagementMap.keySet()) {
      ReactorServiceManager reactorServiceManager = reactorServiceManagementMap.get(service);
      if (reactorServiceManager.canCheckStatus()) {
        String status = reactorServiceManager.isServiceAvailable() ? OK : NOTOK;
        result.put(service, status);
      }
    }

    json = (new Gson()).toJson(result);
    responder.sendByteArray(HttpResponseStatus.OK, json.getBytes(Charsets.UTF_8),
                            ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
  }

  @Path("/system/services/{service-id}/status")
  @GET
  public void monitor(final HttpRequest request, final HttpResponder responder,
                      @PathParam("service-id") final String service) {
    if (reactorServiceManagementMap.containsKey(service)) {
      ReactorServiceManager reactorServiceManager = reactorServiceManagementMap.get(service);
      if (reactorServiceManager.canCheckStatus()) {
        if (reactorServiceManager.isServiceAvailable()) {
          String response = String.format("%s is OK\n", service);
          responder.sendString(HttpResponseStatus.OK, response);
        } else {
          String response = String.format("%s not found\n", service);
          responder.sendString(HttpResponseStatus.NOT_FOUND, response);
        }
      } else {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Operation not valid for this service");
      }
    } else {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid Service Name");
    }
  }

  @Path("/system/services")
  @GET
  public void getServiceSpec(final HttpRequest request, final HttpResponder responder) {
    List<Map<String, String>> serviceSpec = new ArrayList<Map<String, String>>();
    String json;

    for (String service : reactorServiceManagementMap.keySet()) {
      Map<String, String> spec = new HashMap<String, String>();
      ReactorServiceManager serviceManager = reactorServiceManagementMap.get(service);
      String logs = serviceManager.isLogAvailable() ? Constants.Monitor.STATUS_OK : Constants.Monitor.STATUS_NOTOK;
      String canCheck = serviceManager.canCheckStatus() ? (
        serviceManager.isServiceAvailable() ? OK : NOTOK) : "NA";
      String minInstance = String.valueOf(serviceManager.getMinInstances());
      String maxInstance = String.valueOf(serviceManager.getMinInstances());
      String curInstance = String.valueOf(serviceManager.getInstances());
      spec.put("name", service);
      spec.put("logs", logs);
      spec.put("status", canCheck);
      spec.put("min", minInstance);
      spec.put("max", maxInstance);
      spec.put("cur", curInstance);
      //TODO: Add metric name for Event Rate monitoring
      serviceSpec.add(spec);
    }

    json = (new Gson()).toJson(serviceSpec);
    responder.sendByteArray(HttpResponseStatus.OK, json.getBytes(Charsets.UTF_8),
                            ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
  }
}
