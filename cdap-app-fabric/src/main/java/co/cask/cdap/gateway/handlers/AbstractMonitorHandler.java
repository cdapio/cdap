/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.twill.MasterServiceManager;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.SystemServiceMeta;
import co.cask.http.HttpResponder;
import com.google.common.collect.Lists;
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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Monitor Handler returns the status of different discoverable services
 */
public class AbstractMonitorHandler extends AbstractAppFabricHttpHandler {
  private final Map<String, MasterServiceManager> serviceManagementMap;
  private static final String STATUSOK = Constants.Monitor.STATUS_OK;
  private static final String STATUSNOTOK = Constants.Monitor.STATUS_NOTOK;
  private static final String NOTAPPLICABLE = "NA";
  private static final Gson GSON = new Gson();
  private final ServiceStore serviceStore;

  @Inject
  public AbstractMonitorHandler(Authenticator authenticator, Map<String, MasterServiceManager> serviceMap,
                                ServiceStore serviceStore) throws Exception {
    super(authenticator);
    this.serviceManagementMap = serviceMap;
    this.serviceStore = serviceStore;
  }


  public void getServiceInstance(final HttpRequest request, final HttpResponder responder,
                                 String serviceName) throws Exception {
    JsonObject reply = new JsonObject();
    if (!serviceManagementMap.containsKey(serviceName)) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Invalid service name %s", serviceName));
      return;
    }
    MasterServiceManager serviceManager = serviceManagementMap.get(serviceName);
    if (serviceManager.isServiceEnabled()) {
      int actualInstance = serviceManagementMap.get(serviceName).getInstances();
      reply.addProperty("provisioned", actualInstance);
      reply.addProperty("requested", getSystemServiceInstanceCount(serviceName));
      responder.sendJson(HttpResponseStatus.OK, reply);
    } else {
      responder.sendString(HttpResponseStatus.FORBIDDEN, String.format("Service %s is not enabled", serviceName));
    }
  }

  public void setServiceInstance(final HttpRequest request, final HttpResponder responder,
                                 final String serviceName) {
    try {
      if (!serviceManagementMap.containsKey(serviceName)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Invalid Service Name");
        return;
      }

      MasterServiceManager serviceManager = serviceManagementMap.get(serviceName);
      int instance = getInstances(request);
      if (!serviceManager.isServiceEnabled()) {
        responder.sendString(HttpResponseStatus.FORBIDDEN, String.format("Service %s is not enabled", serviceName));
        return;
      }

      Integer currentInstance = getSystemServiceInstanceCount(serviceName);
      if (instance < serviceManager.getMinInstances() || instance > serviceManager.getMaxInstances()) {
        String response = String.format("Instance count should be between [%s,%s]", serviceManager.getMinInstances(),
                                        serviceManager.getMaxInstances());
        responder.sendString(HttpResponseStatus.BAD_REQUEST, response);
        return;
      } else if (instance == currentInstance) {
        responder.sendStatus(HttpResponseStatus.OK);
        return;
      }

      serviceStore.setServiceInstance(serviceName, instance);
      if (serviceManager.setInstances(instance)) {
        responder.sendStatus(HttpResponseStatus.OK);
      } else {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Operation did not succeed");
      }
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           String.format("Error updating instances for service: %s", serviceName));
    }
  }


  public void getBootStatus(final HttpRequest request, final HttpResponder responder) {
    Map<String, String> result = new HashMap<String, String>();
    for (String service : serviceManagementMap.keySet()) {
      MasterServiceManager masterServiceManager = serviceManagementMap.get(service);
      if (masterServiceManager.isServiceEnabled() && masterServiceManager.canCheckStatus()) {
        String status = masterServiceManager.isServiceAvailable() ? STATUSOK : STATUSNOTOK;
        result.put(service, status);
      }
    }
    responder.sendJson(HttpResponseStatus.OK, result);
  }

  public void monitor(final HttpRequest request, final HttpResponder responder,
                      final String serviceName) {
    if (!serviceManagementMap.containsKey(serviceName)) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Invalid service name %s", serviceName));
      return;
    }
    MasterServiceManager masterServiceManager = serviceManagementMap.get(serviceName);
    if (!masterServiceManager.isServiceEnabled()) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, String.format("Service %s is not enabled", serviceName));
      return;
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
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Operation not valid for this service");
    }
  }

  public void getServiceSpec(final HttpRequest request, final HttpResponder responder) throws Exception {
    List<SystemServiceMeta> response = Lists.newArrayList();
    SortedSet<String> services = new TreeSet<String>(serviceManagementMap.keySet());
    List<String> serviceList = new ArrayList<String>(services);
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

  private int getSystemServiceInstanceCount(String serviceName) throws Exception {
    Integer count = serviceStore.getServiceInstance(serviceName);

    //In standalone mode, this count will be null. And thus we just return the actual instance count.
    if (count == null) {
      return serviceManagementMap.get(serviceName).getInstances();
    } else {
      return count;
    }
  }

}
