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
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.OperationException;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NotRunningProgramLiveInfo;
import co.cask.cdap.proto.ProgramLiveInfo;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ServiceInstances;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
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
 *  {@link HttpHandler} for User Services.
 */
@Path(Constants.Gateway.API_VERSION_2)
public class ServiceHttpHandler extends AbstractAppFabricHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceHttpHandler.class);

  private final Store store;
  private final ProgramRuntimeService runtimeService;
  private final ProgramLifecycleHttpHandler programLifecycleHttpHandler;

  @Inject
  public ServiceHttpHandler(Authenticator authenticator, StoreFactory storeFactory,
                            ProgramRuntimeService runtimeService,
                            ProgramLifecycleHttpHandler programLifecycleHttpHandler) {
    super(authenticator);
    this.programLifecycleHttpHandler = programLifecycleHttpHandler;
    this.store = storeFactory.create();
    this.runtimeService = runtimeService;
  }

  /**
   * Returns a list of Services associated with an account.
   */
  @GET
  @Path("/services")
  public void getAllServices(HttpRequest request, HttpResponder responder) {
    programLifecycleHttpHandler.getAllServices(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE);
  }

  /**
   * Return the list of user Services in an application.
   */
  @Path("/apps/{app-id}/services")
  @GET
  public void getServicesByApp(HttpRequest request, HttpResponder responder, @PathParam("app-id") String appId) {
    programLifecycleHttpHandler.getProgramsByApp(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId,
                                                 ProgramType.SERVICE);
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

      ServiceSpecification specification = getServiceSpecification(accountId, appId, serviceId);
      if (specification == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      // If the runnable name is the same as the service name, then uses the service spec, otherwise use the worker spec
      int instances;
      if (specification.getName().equals(runnableName)) {
        instances = specification.getInstances();
      } else {
        ServiceWorkerSpecification workerSpec = specification.getWorkers().get(runnableName);
        if (workerSpec == null) {
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
          return;
        }
        instances = workerSpec.getInstances();
      }

      responder.sendJson(HttpResponseStatus.OK,
                         new ServiceInstances(instances, getRunnableCount(accountId, appId, serviceId, runnableName)));

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

      // If the runnable name is the same as the service name, it's setting the service instances
      // TODO: This REST API is bad, need to update (CDAP-388)
      int oldInstances = (runnableName.equals(serviceId)) ? store.getServiceInstances(programId)
                                                          : store.getServiceWorkerInstances(programId, runnableName);
      if (oldInstances != instances) {
        if (runnableName.equals(serviceId)) {
          store.setServiceInstances(programId, instances);
        } else {
          store.setServiceWorkerInstances(programId, runnableName, instances);
        }

        ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId.getAccountId(),
                                                                        programId.getApplicationId(),
                                                                        programId.getId(),
                                                                        ProgramType.SERVICE, runtimeService);
        if (runtimeInfo != null) {
          runtimeInfo.getController().command(ProgramOptionConstants.INSTANCES,
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
  public void serviceLiveInfo(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") String appId,
                              @PathParam("service-id") String serviceId) {
    getLiveInfo(request, responder, appId, serviceId, ProgramType.SERVICE, runtimeService);
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

  private int getRunnableCount(String accountId, String appId, String serviceName, String runnable) throws Exception {
    Id.Program programId = Id.Program.from(accountId, appId, serviceName);
    ProgramLiveInfo info = runtimeService.getLiveInfo(programId, ProgramType.SERVICE);
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
      //Not running on YARN, get it from store
      // If the runnable name is the same as the service name, get the instances from service spec.
      // Otherwise get it from worker spec.
      // TODO: This is due to the improper REST API design that treats everything in service as Runnable
      if (runnable.equals(programId.getId())) {
        return store.getServiceInstances(programId);
      } else {
        return store.getServiceWorkerInstances(programId, runnable);
      }
    }
  }
}
