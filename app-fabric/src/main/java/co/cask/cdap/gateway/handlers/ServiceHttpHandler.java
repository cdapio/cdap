/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.zookeeper.coordination.ServiceDiscoveredCodec;
import co.cask.cdap.data2.OperationException;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NotRunningProgramLiveInfo;
import co.cask.cdap.proto.ProgramLiveInfo;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ServiceInstances;
import co.cask.cdap.proto.ServiceMeta;
import co.cask.http.HttpResponder;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.discovery.ServiceDiscovered;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
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
  private final ProgramRuntimeService runtimeService;
  private static final Gson GSON = new GsonBuilder()
                                      .registerTypeAdapter(new TypeToken<ServiceDiscovered>() { }.getType(),
                                                            new ServiceDiscoveredCodec())
                                      .create();

  private static final Logger LOG = LoggerFactory.getLogger(ServiceHttpHandler.class);

  @Inject
  public ServiceHttpHandler(Authenticator authenticator, StoreFactory storeFactory,
                            ProgramRuntimeService runtimeService) {
    super(authenticator);
    this.store = storeFactory.create();
    this.runtimeService = runtimeService;
  }

  /**
   * Return the list of user twill apps for an application.
   */
  @Path("/apps/{app-id}/services")
  @GET
  public void listServices(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") String appId) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      ApplicationSpecification spec = store.getApplication(Id.Application.from(accountId, appId));
      if (spec != null) {
        List<ProgramRecord> services = Lists.newArrayList();
        for (Map.Entry<String, ServiceSpecification> entry : spec.getServices().entrySet()) {
          ServiceSpecification specification = entry.getValue();
          services.add(new ProgramRecord(ProgramType.SERVICE, appId, specification.getName(),
                                         specification.getName(), specification.getDescription()));
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
  @Path("/apps/{app-id}/services/{service-id}")
  @GET
  public void getService(HttpRequest request, HttpResponder responder,
                         @PathParam("app-id") String appId,
                         @PathParam("service-id") String serviceId) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      ServiceSpecification spec = getServiceSpecification(accountId, appId, serviceId);
      if (spec != null) {
        responder.sendJson(HttpResponseStatus.OK, new ServiceMeta(
          spec.getName(), spec.getName(), spec.getDescription(), spec.getRunnables().keySet()));
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
  @Path("/apps/{app-id}/services/{service-id}/runnables/{runnable-name}/instances")
  public void getInstances(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") String appId,
                           @PathParam("service-id") String serviceId,
                           @PathParam("runnable-name") String runnableName) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      Id.Program programId = Id.Program.from(accountId, appId, serviceId);
      if (!store.programExists(programId, ProgramType.SERVICE)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Runnable not found");
        return;
      }
      RuntimeSpecification specification = getRuntimeSpecification(accountId, appId, serviceId, runnableName);
      if (specification == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        responder.sendJson(HttpResponseStatus.OK, new ServiceInstances(
          specification.getResourceSpecification().getInstances(),
          getRunnableCount(accountId, appId, serviceId, runnableName
        )));
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
  @Path("/apps/{app-id}/services/{service-id}/runnables/{runnable-name}/instances")
  public void setInstances(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") String appId,
                           @PathParam("service-id") String serviceId,
                           @PathParam("runnable-name") String runnableName) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      Id.Program programId = Id.Program.from(accountId, appId, serviceId);
      if (!store.programExists(programId, ProgramType.SERVICE)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Runnable not found");
        return;
      }

      int instances = getInstances(request);
      if (instances < 1) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Instance count should be greater than 0");
        return;
      }

      int oldInstances = store.getServiceRunnableInstances(programId, runnableName);
      if (oldInstances != instances) {
        store.setServiceRunnableInstances(programId, runnableName, instances);
        ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId.getAccountId(),
                                                                        programId.getApplicationId(),
                                                                        programId.getId(),
                                                                        ProgramType.SERVICE);
        if (runtimeInfo != null) {
          runtimeInfo.getController().command(ProgramOptionConstants.RUNNABLE_INSTANCES,
                                              ImmutableMap.of("runnable", runnableName,
                                                              "newInstances", String.valueOf(instances),
                                                              "oldInstances", String.valueOf(oldInstances))).get();
        }
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable throwable) {
      if (respondIfElementNotFound(throwable, responder)) {
        return;
      }
      LOG.error("Got exception : ", throwable);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/apps/{app-id}/services/{service-id}/live-info")
  public void liveInfo(HttpRequest request, HttpResponder responder,
                       @PathParam("app-id") String appId,
                       @PathParam("service-id") String serviceId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      responder.sendJson(HttpResponseStatus.OK,
                         runtimeService.getLiveInfo(Id.Program.from(accountId,
                                                                    appId,
                                                                    serviceId),
                                                    ProgramType.SERVICE));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @Nullable
  private ServiceSpecification getServiceSpecification(String accountId, String id,
                                                       String serviceName) throws OperationException {
    ApplicationSpecification applicationSpecification = store.getApplication(Id.Application.from(accountId, id));
    if (applicationSpecification == null) {
      return null;
    }
    Map<String, ServiceSpecification> serviceSpecs = applicationSpecification.getServices();
    if (serviceSpecs.containsKey(serviceName)) {
      return serviceSpecs.get(serviceName);
    } else {
      return null;
    }
  }

  @Nullable
  private RuntimeSpecification getRuntimeSpecification(String accountId, String appId, String serviceName,
                                                        String runnableName) throws OperationException {
    ServiceSpecification specification = getServiceSpecification(accountId, appId, serviceName);
    if (specification != null) {
      Map<String, RuntimeSpecification> runtimeSpecs =  specification.getRunnables();
      return runtimeSpecs.containsKey(runnableName) ? runtimeSpecs.get(runnableName) : null;
    } else {
      return null;
    }
  }

  private int getRunnableCount(String accountId, String appId, String serviceName, String runnable) throws Exception {
    Id.Program programID = Id.Program.from(accountId, appId, serviceName);
    ProgramLiveInfo info = runtimeService.getLiveInfo(programID, ProgramType.SERVICE);
    int count = 0;
    if (info instanceof NotRunningProgramLiveInfo) {
      return count;
    } else if (info instanceof Containers) {
      Containers containers = (Containers) info;
      for (Containers.ContainerInfo container : containers.getContainers()) {
        if (container.getName().equals(runnable)) {
          count++;
        }
      }
      return count;
    } else {
      //Not running on YARN
      return store.getServiceRunnableInstances(programID, runnable);
    }
  }

  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(String accountId, String appId,
                                                            String flowId, ProgramType typeId) {
    ProgramType type = ProgramType.valueOf(typeId.name());
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(type).values();
    Preconditions.checkNotNull(runtimeInfos, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND),
                               accountId, flowId);

    Id.Program programId = Id.Program.from(accountId, appId, flowId);

    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (programId.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }
}
