/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.app.store.ServiceStore;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.ForbiddenException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.security.AuditDetail;
import co.cask.cdap.common.security.AuditPolicy;
import co.cask.cdap.common.twill.MasterServiceManager;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.SystemServiceMeta;
import co.cask.http.HttpResponder;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Monitor Handler V3
 */
@Path(Constants.Gateway.API_VERSION_3)
public class MonitorHandler extends AbstractAppFabricHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorHandler.class);

  private static final String STATUSOK = Constants.Monitor.STATUS_OK;
  private static final String STATUSNOTOK = Constants.Monitor.STATUS_NOTOK;
  private static final String NOTAPPLICABLE = "NA";

  private final Map<String, MasterServiceManager> serviceManagementMap;
  private final ServiceStore serviceStore;

  @Inject
  public MonitorHandler(Map<String, MasterServiceManager> serviceMap,
                        ServiceStore serviceStore) throws Exception {
    this.serviceManagementMap = serviceMap;
    this.serviceStore = serviceStore;
  }

  /**
   * Returns the live info of CDAP Services
   */
  @Path("/system/services/{service-name}/live-info")
  @GET
  public void getServiceLiveInfo(HttpRequest request, HttpResponder responder,
                                 @PathParam("service-name") String serviceName) throws Exception {
    if (!serviceManagementMap.containsKey(serviceName)) {
      throw new NotFoundException(String.format("Invalid service name %s", serviceName));
    }

    MasterServiceManager serviceManager = serviceManagementMap.get(serviceName);
    if (serviceManager.isServiceEnabled()) {
      responder.sendJson(HttpResponseStatus.OK, serviceManager.getLiveInfo());
    } else {
      throw new ForbiddenException(String.format("Service %s is not enabled", serviceName));
    }
  }

  /**
   * Returns the number of instances of CDAP Services
   */
  @Path("/system/services/{service-name}/instances")
  @GET
  public void getServiceInstance(HttpRequest request, HttpResponder responder,
                                 @PathParam("service-name") String serviceName) throws Exception {

    JsonObject reply = new JsonObject();
    if (!serviceManagementMap.containsKey(serviceName)) {
      throw new NotFoundException(String.format("Invalid service name %s", serviceName));
    }
    MasterServiceManager serviceManager = serviceManagementMap.get(serviceName);
    if (serviceManager.isServiceEnabled()) {
      int actualInstance = serviceManagementMap.get(serviceName).getInstances();
      reply.addProperty("provisioned", actualInstance);
      reply.addProperty("requested", getSystemServiceInstanceCount(serviceName));
      responder.sendJson(HttpResponseStatus.OK, reply);
    } else {
      throw new ForbiddenException(String.format("Service %s is not enabled", serviceName));
    }
  }

  /**
   * Sets the number of instances of CDAP Services
   */
  @Path("/system/services/{service-name}/instances")
  @PUT
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void setServiceInstance(HttpRequest request, HttpResponder responder,
                                 @PathParam("service-name") final String serviceName) throws Exception {
    if (!serviceManagementMap.containsKey(serviceName)) {
      throw new NotFoundException(String.format("Invalid service name %s", serviceName));
    }

    MasterServiceManager serviceManager = serviceManagementMap.get(serviceName);
    int instances = getInstances(request);
    if (!serviceManager.isServiceEnabled()) {
      throw new ForbiddenException(String.format("Service %s is not enabled", serviceName));
    }

    int currentInstances = getSystemServiceInstanceCount(serviceName);
    if (instances < serviceManager.getMinInstances() || instances > serviceManager.getMaxInstances()) {
      String response = String.format("Instance count should be between [%s,%s]", serviceManager.getMinInstances(),
                                      serviceManager.getMaxInstances());
      throw new BadRequestException(response);
    } else if (instances == currentInstances) {
      responder.sendStatus(HttpResponseStatus.OK);
      return;
    }

    serviceStore.setServiceInstance(serviceName, instances);
    if (serviceManager.setInstances(instances)) {
      responder.sendStatus(HttpResponseStatus.OK);
    } else {
      throw new BadRequestException("Operation did not succeed");
    }
  }

  // Return the status of CDAP services in JSON format
  @Path("/system/services/status")
  @GET
  public void getBootStatus(HttpRequest request, HttpResponder responder) {
    Map<String, String> result = new HashMap<>();
    for (String service : serviceManagementMap.keySet()) {
      MasterServiceManager masterServiceManager = serviceManagementMap.get(service);
      if (masterServiceManager.isServiceEnabled() && masterServiceManager.canCheckStatus()) {
        String status = masterServiceManager.isServiceAvailable() ? STATUSOK : STATUSNOTOK;
        result.put(service, status);
      }
    }
    responder.sendJson(HttpResponseStatus.OK, result);
  }

  @Path("/system/services/{service-name}/status")
  @GET
  public void monitor(HttpRequest request, HttpResponder responder,
                      @PathParam("service-name") String serviceName) throws Exception {
    if (!serviceManagementMap.containsKey(serviceName)) {
      throw new NotFoundException(String.format("Invalid service name %s", serviceName));
    }
    MasterServiceManager masterServiceManager = serviceManagementMap.get(serviceName);
    if (!masterServiceManager.isServiceEnabled()) {
      throw new ForbiddenException(String.format("Service %s is not enabled", serviceName));
    }
    if (masterServiceManager.canCheckStatus() && masterServiceManager.isServiceAvailable()) {
      JsonObject json = new JsonObject();
      json.addProperty("status", STATUSOK);
      responder.sendJson(HttpResponseStatus.OK, json);
    } else if (masterServiceManager.canCheckStatus()) {
      JsonObject json = new JsonObject();
      json.addProperty("status", STATUSNOTOK);
      responder.sendJson(HttpResponseStatus.OK, json);
    } else {
      throw new BadRequestException("Operation not valid for this service");
    }
  }

  @Path("/system/services")
  @GET
  public void getServiceSpec(HttpRequest request, HttpResponder responder) throws Exception {
    List<SystemServiceMeta> response = Lists.newArrayList();
    SortedSet<String> services = new TreeSet<>(serviceManagementMap.keySet());
    List<String> serviceList = new ArrayList<>(services);
    for (String service : serviceList) {
      MasterServiceManager serviceManager = serviceManagementMap.get(service);
      if (serviceManager.isServiceEnabled()) {
        String logs = serviceManager.isLogAvailable() ? Constants.Monitor.STATUS_OK : Constants.Monitor.STATUS_NOTOK;
        String canCheck = serviceManager.canCheckStatus() ? (
          serviceManager.isServiceAvailable() ? STATUSOK : STATUSNOTOK) : NOTAPPLICABLE;
        //TODO: Add metric name for Event Rate monitoring
        response.add(new SystemServiceMeta(service, serviceManager.getDescription(), canCheck, logs,
                                           serviceManager.getMinInstances(), serviceManager.getMaxInstances(),
                                           getSystemServiceInstanceCount(service), serviceManager.getInstances()));
      }
    }
    responder.sendJson(HttpResponseStatus.OK, response);
  }

  /**
   * Send request to restart all instances for a CDAP system service.
   */
  @Path("/system/services/{service-name}/restart")
  @POST
  public void restartAllServiceInstances(HttpRequest request, HttpResponder responder,
                                         @PathParam("service-name") String serviceName) throws Exception {
    restartInstances(responder, serviceName, -1, true);
  }

  /**
   * Send request to restart single instance identified by <instance-id>
   */
  @Path("/system/services/{service-name}/instances/{instance-id}/restart")
  @POST
  public void restartServiceInstance(HttpRequest request, HttpResponder responder,
                                     @PathParam("service-name") String serviceName,
                                     @PathParam("instance-id") int instanceId) throws Exception {
    restartInstances(responder, serviceName, instanceId, false);
  }

  /**
   * Send request to get the status of latest restart instances request for a CDAP system service.
   */
  @Path("/system/services/{service-name}/latest-restart")
  @GET
  public void getLatestRestartServiceInstanceStatus(HttpRequest request, HttpResponder responder,
                                                    @PathParam("service-name") String serviceName) throws Exception {
    try {
      responder.sendJson(HttpResponseStatus.OK, serviceStore.getLatestRestartInstancesRequest(serviceName));
    } catch (IllegalStateException ex) {
      throw new NotFoundException(String.format("No restart instances request found or %s", serviceName));
    }
  }

  /**
   * Update log levels for this service.
   */
  @Path("system/services/{service-name}/loglevels")
  @PUT
  public void updateServiceLogLevels(HttpRequest request, HttpResponder responder,
                                     @PathParam("service-name") String serviceName) throws Exception {
    if (!serviceManagementMap.containsKey(serviceName)) {
      throw new NotFoundException(String.format("Invalid service name %s", serviceName));
    }

    MasterServiceManager masterServiceManager = serviceManagementMap.get(serviceName);
    if (!masterServiceManager.isServiceEnabled()) {
      throw new ForbiddenException(String.format("Failed to update log levels for service %s " +
                                                   "because the service is not enabled", serviceName));
    }

    try {
      // we are decoding the body to Map<String, String> instead of Map<String, LogEntry.Level> here since Gson will
      // serialize invalid enum values to null, which is allowed for log level, instead of throw an Exception.
      masterServiceManager.updateServiceLogLevels(transformLogLevelsMap(decodeArguments(request)));
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IllegalStateException ise) {
      throw new ServiceUnavailableException(String.format("Failed to update log levels for service %s " +
                                                            "because the service may not be ready yet", serviceName));
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage());
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid Json in the body");
    }
  }

  /**
   * Reset the log levels of the service.
   * All loggers will be reset to the level when the service started.
   */
  @Path("system/services/{service-name}/resetloglevels")
  @POST
  public void resetServiceLogLevels(HttpRequest request, HttpResponder responder,
                                     @PathParam("service-name") String serviceName) throws Exception {
    if (!serviceManagementMap.containsKey(serviceName)) {
      throw new NotFoundException(String.format("Invalid service name %s", serviceName));
    }

    MasterServiceManager masterServiceManager = serviceManagementMap.get(serviceName);
    if (!masterServiceManager.isServiceEnabled()) {
      throw new ForbiddenException(String.format("Failed to reset log levels for service %s " +
                                                   "because the service is not enabled", serviceName));
    }

    try {
      Set<String> loggerNames = parseBody(request, SET_STRING_TYPE);
      masterServiceManager.resetServiceLogLevels(loggerNames == null ? Collections.<String>emptySet() : loggerNames);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IllegalStateException ise) {
      throw new ServiceUnavailableException(String.format("Failed to reset log levels for service %s " +
                                                            "because the service may not be ready yet", serviceName));
    }  catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid Json in the body");
    }
  }

  private int getSystemServiceInstanceCount(String serviceName) throws Exception {
    Integer count = serviceStore.getServiceInstance(serviceName);

    //In standalone mode, this count will be null. And thus we just return the actual instance count.
    if (count == null) {
      return serviceManagementMap.get(serviceName).getInstances();
    } else {
      return count;
    }
  }

  private void restartInstances(HttpResponder responder, String serviceName, int instanceId,
                                boolean restartAll) throws Exception {
    long startTimeMs = System.currentTimeMillis();
    boolean isSuccess = true;
    if (!serviceManagementMap.containsKey(serviceName)) {
      throw new NotFoundException(String.format("Invalid service name %s", serviceName));
    }
    MasterServiceManager masterServiceManager = serviceManagementMap.get(serviceName);

    try {
      if (!masterServiceManager.isServiceEnabled()) {
        String message = String.format("Failed to restart instance for %s because the service is not enabled.",
                                       serviceName);
        LOG.debug(message);
        isSuccess = false;
        throw new ForbiddenException(message);
      }

      if (restartAll) {
        masterServiceManager.restartAllInstances();
      } else {
        if (instanceId < 0 || instanceId >= masterServiceManager.getInstances()) {
          throw new IllegalArgumentException();
        }
        masterServiceManager.restartInstances(instanceId);
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IllegalStateException ise) {
      String message = String.format("Failed to restart instance for %s because the service may not be ready yet",
                                     serviceName);
      LOG.debug(message, ise);
      isSuccess = false;
      throw new ServiceUnavailableException(message);
    } catch (IllegalArgumentException iex) {
      String message = String.format("Failed to restart instance %d for service: %s because invalid instance id",
                                     instanceId, serviceName);
      LOG.debug(message, iex);

      isSuccess = false;
      throw new BadRequestException(message);
    } catch (Exception ex) {
      LOG.warn(String.format("Exception when trying to restart instances for service %s", serviceName), ex);

      isSuccess = false;
      throw new Exception(String.format("Error restarting instance %d for service: %s", instanceId, serviceName));
    } finally {
      long endTimeMs = System.currentTimeMillis();
      if (restartAll) {
        serviceStore.setRestartAllInstancesRequest(serviceName, startTimeMs, endTimeMs, isSuccess);
      } else {
        serviceStore.setRestartInstanceRequest(serviceName, startTimeMs, endTimeMs, isSuccess, instanceId);
      }
    }
  }
}
