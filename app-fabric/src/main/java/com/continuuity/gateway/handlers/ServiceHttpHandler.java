package com.continuuity.gateway.handlers;

import com.continuuity.api.service.ServiceSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.Type;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.OperationException;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.util.AbstractAppFabricHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.inject.Inject;
import org.apache.twill.api.RuntimeSpecification;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 *  Handler class for User services.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class ServiceHttpHandler extends AbstractAppFabricHttpHandler {

  private final Store store;
  private static final Logger LOG = LoggerFactory.getLogger(ServiceHttpHandler.class);

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

    try {
      String accountId = getAuthenticatedAccountId(request);
      ApplicationSpecification spec = store.getApplication(Id.Application.from(accountId, appId));
      if (spec != null) {
        JsonArray services = new JsonArray();
        for (Map.Entry<String, ServiceSpecification> entry : spec.getServices().entrySet()) {
          JsonObject service = new JsonObject();
          service.addProperty("type", Type.SERVICE.toString());
          service.addProperty("app", appId);
          service.addProperty("id", entry.getValue().getName());
          service.addProperty("name", entry.getValue().getName());
          service.addProperty("description", entry.getValue().getDescription());
          services.add(service);
        }
        responder.sendJson(HttpResponseStatus.OK, services);
      } else {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Return the service details of a given service.
   */
  @Path("/apps/{app-id}/services/{service-name}")
  @GET
  public void getService(HttpRequest request, HttpResponder responder,
                         @PathParam("app-id") String appId,
                         @PathParam("service-name") String serviceName) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      ServiceSpecification spec = getServiceSpecification(accountId, appId, serviceName);
      if (spec != null) {
        JsonObject service = new JsonObject();
        service.addProperty("id", spec.getName());
        service.addProperty("name", spec.getName());
        service.addProperty("description", spec.getDescription());
        JsonArray runnables = new JsonArray();
        for (Map.Entry<String, RuntimeSpecification> entry : spec.getRunnables().entrySet()) {
          runnables.add(new JsonPrimitive(entry.getKey()));
        }
        responder.sendJson(HttpResponseStatus.OK, service);
      } else {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
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

  @Nullable
  private ServiceSpecification getServiceSpecification(String accountId, String id,
                                                       String serviceName) throws OperationException {
    ApplicationSpecification applicationSpecification = store.getApplication(Id.Application.from(accountId, id));
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
