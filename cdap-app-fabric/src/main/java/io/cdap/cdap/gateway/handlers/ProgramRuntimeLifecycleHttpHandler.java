/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramRuntimeLifecycleService;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.BatchRunnable;
import io.cdap.cdap.proto.BatchRunnableInstances;
import io.cdap.cdap.proto.Containers;
import io.cdap.cdap.proto.Instances;
import io.cdap.cdap.proto.NotRunningProgramLiveInfo;
import io.cdap.cdap.proto.ProgramLiveInfo;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ServiceInstances;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link io.cdap.http.HttpHandler} to manage program runtime lifecycle for v3 REST APIs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class ProgramRuntimeLifecycleHttpHandler extends AbstractProgramLifecycleHttpHandler {

  private final ProgramRuntimeLifecycleService runtimeLifecycleService;
  private final ProgramRuntimeService runtimeService;

  @Inject
  public ProgramRuntimeLifecycleHttpHandler(ProgramLifecycleService lifecycleService, Store store,
      ProgramRuntimeLifecycleService runtimeLifecycleService, ProgramRuntimeService runtimeService,
      NamespaceQueryAdmin namespaceQueryAdmin) {
    super(lifecycleService, store, namespaceQueryAdmin);
    this.runtimeLifecycleService = runtimeLifecycleService;
    this.runtimeService = runtimeService;
  }

  private static final Type BATCH_RUNNABLES_TYPE = new TypeToken<List<BatchRunnable>>() {
  }.getType();

  /**
   * Returns the number of instances for all program runnables that are passed into the data. The
   * data is an array of Json objects where each object must contain the following three elements:
   * appId, programType, and programId (flow name, service name). Retrieving instances only applies
   * to flows, and user services. For flows, another parameter, "runnableId", must be provided. This
   * corresponds to the flowlet/runnable for which to retrieve the instances.
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1", "runnableId": "Runnable1"},
   *  {"appId": "App1", "programType": "Mapreduce", "programId": "Mapreduce2"}]
   * </code></pre>
   * </p><p>
   * The response will be an array of JsonObjects each of which will contain the three input
   * parameters as well as 3 fields:
   * <ul>
   * <li>"provisioned" which maps to the number of instances actually provided for the input runnable;</li>
   * <li>"requested" which maps to the number of instances the user has requested for the input runnable; and</li>
   * <li>"statusCode" which maps to the http status code for the data in that JsonObjects (200, 400, 404).</li>
   * </ul>
   * </p><p>
   * If an error occurs in the input (for the example above, Flowlet1 does not exist), then all JsonObjects for
   * which the parameters have a valid instances will have the provisioned and requested fields status code fields
   * but all JsonObjects for which the parameters are not valid will have an error message and statusCode.
   * </p><p>
   * For example, if there is no Flowlet1 in the above data, then the response could be 200 OK with the following data:
   * </p>
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1", "runnableId": "Runnable1",
   *   "statusCode": 200, "provisioned": 2, "requested": 2},
   *  {"appId": "App1", "programType": "Mapreduce", "programId": "Mapreduce2", "statusCode": 400,
   *   "error": "Program type 'Mapreduce' is not a valid program type to get instances"}]
   * </code></pre>
   */
  @POST
  @Path("/instances")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void getInstances(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId) throws IOException, BadRequestException {

    List<BatchRunnable> runnables = validateAndGetBatchInput(request, BATCH_RUNNABLES_TYPE);

    // cache app specs to perform fewer store lookups
    Map<ApplicationId, ApplicationSpecification> appSpecs = new HashMap<>();

    List<BatchRunnableInstances> output = new ArrayList<>(runnables.size());
    for (BatchRunnable runnable : runnables) {
      // cant get instances for things that are not flows, services, or workers
      if (!canHaveInstances(runnable.getProgramType())) {
        output.add(
            new BatchRunnableInstances(runnable, HttpResponseStatus.BAD_REQUEST.code(),
                String.format("Program type '%s' is not a valid program type to get instances",
                    runnable.getProgramType().getPrettyName())));
        continue;
      }

      ApplicationId appId = new ApplicationId(namespaceId, runnable.getAppId());
      try {
        appId = new ApplicationId(namespaceId, runnable.getAppId(),
            getLatestAppVersion(new NamespaceId(namespaceId), runnable.getAppId()));
      } catch (ApplicationNotFoundException e) {
        output.add(new BatchRunnableInstances(runnable, HttpResponseStatus.NOT_FOUND.code(),
            String.format("App: %s not found", appId)));
        continue;
      }

      // populate spec cache if this is the first time we've seen the appid.
      if (!appSpecs.containsKey(appId)) {
        appSpecs.put(appId, store.getApplication(appId));
      }

      ApplicationSpecification spec = appSpecs.get(appId);
      ProgramId programId = appId.program(runnable.getProgramType(), runnable.getProgramId());
      output.add(getProgramInstances(runnable, spec, programId));
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(output));
  }

  /**
   * Return the number of instances of a service.
   */
  @GET
  @Path("/apps/{app-id}/services/{service-id}/instances")
  public void getServiceInstances(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("service-id") String serviceId) throws Exception {
    try {
      ProgramId programId = validateAndGetNamespace(namespaceId)
          .app(appId, getLatestAppVersion(new NamespaceId(namespaceId), appId))
          .service(serviceId);
      lifecycleService.ensureProgramExists(programId);
      int instances = store.getServiceInstances(programId);
      responder.sendJson(HttpResponseStatus.OK,
          GSON.toJson(new ServiceInstances(instances, getInstanceCount(programId, serviceId))));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    }
  }

  /**
   * Set instances of a service.
   */
  @PUT
  @Path("/apps/{app-id}/services/{service-id}/instances")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void setServiceInstances(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("service-id") String serviceId) throws Exception {
    try {
      ProgramId programId = new ApplicationId(namespaceId, appId,
          getLatestAppVersion(new NamespaceId(namespaceId), appId))
          .program(ProgramType.SERVICE, serviceId);
      Store.ensureProgramExists(programId, store.getApplication(programId.getParent()));

      int instances = getInstances(request);
      runtimeLifecycleService.setInstances(programId, instances);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable throwable) {
      if (respondIfElementNotFound(throwable, responder)) {
        return;
      }
      throw throwable;
    }
  }

  /**
   * Returns number of instances of a worker.
   */
  @GET
  @Path("/apps/{app-id}/workers/{worker-id}/instances")
  public void getWorkerInstances(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("worker-id") String workerId) throws Exception {
    try {
      ProgramId programId = validateAndGetNamespace(namespaceId)
          .app(appId, getLatestAppVersion(new NamespaceId(namespaceId), appId))
          .worker(workerId);
      lifecycleService.ensureProgramExists(programId);
      int count = store.getWorkerInstances(programId);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(new Instances(count)));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      if (respondIfElementNotFound(e, responder)) {
        return;
      }
      throw e;
    }
  }

  /**
   * Sets the number of instances of a worker.
   */
  @PUT
  @Path("/apps/{app-id}/workers/{worker-id}/instances")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void setWorkerInstances(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("worker-id") String workerId) throws Exception {
    int instances = getInstances(request);
    try {
      runtimeLifecycleService.setInstances(new ApplicationId(namespaceId, appId,
          getLatestAppVersion(new NamespaceId(namespaceId), appId))
          .program(ProgramType.WORKER, workerId), instances);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      if (respondIfElementNotFound(e, responder)) {
        return;
      }
      throw e;
    }
  }

  @GET
  @Path("/apps/{app-id}/{program-category}/{program-id}/live-info")
  @SuppressWarnings("unused")
  public void liveInfo(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId, @PathParam("program-category") String programCategory,
      @PathParam("program-id") String programId)
      throws BadRequestException, ApplicationNotFoundException {
    ProgramType type = getProgramType(programCategory);
    ProgramId program = new ApplicationId(namespaceId, appId,
        getLatestAppVersion(new NamespaceId(namespaceId), appId))
        .program(type, programId);
    getLiveInfo(responder, program, runtimeService);
  }

  /**
   * Update the log level for a running program according to the request body. Currently supported
   * program types are {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}. The request body
   * is expected to contain a map of log levels, where key is loggername, value is one of the valid
   * {@link org.apache.twill.api.logging.LogEntry.Level} or null.
   *
   */
  @PUT
  @Path("/apps/{app-name}/{program-type}/{program-name}/runs/{run-id}/loglevels")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void updateProgramLogLevels(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace,
      @PathParam("app-name") String appName,
      @PathParam("program-type") String type,
      @PathParam("program-name") String programName,
      @PathParam("run-id") String runId) throws Exception {
    RunRecordDetail run = getRunRecordDetailFromId(namespace, appName, type, programName, runId);
    updateLogLevels(request, responder, namespace, appName, run.getVersion(), type, programName,
        runId);
  }

  /**
   * Update the log level for a running program according to the request body.
   * Deprecated : Run-id is sufficient to identify a program run.
   */
  @Deprecated
  @PUT
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runs/{run-id}/loglevels")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void updateProgramLogLevelsVersioned(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace,
      @PathParam("app-name") String appName,
      @PathParam("app-version") String appVersion,
      @PathParam("program-type") String type,
      @PathParam("program-name") String programName,
      @PathParam("run-id") String runId) throws Exception {
    updateLogLevels(request, responder, namespace, appName, appVersion, type, programName, runId);
  }

  /**
   * Reset the log level for a running program back to where it starts. Currently supported program
   * types are {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}. The request body can
   * either be empty, which will reset all loggers for the program, or contain a list of logger
   * names, which will reset for these particular logger names for the program.
   */
  @POST
  @Path("/apps/{app-name}/{program-type}/{program-name}/runs/{run-id}/resetloglevels")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void resetProgramLogLevels(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace,
      @PathParam("app-name") String appName,
      @PathParam("program-type") String type,
      @PathParam("program-name") String programName,
      @PathParam("run-id") String runId) throws Exception {
    RunRecordDetail run = getRunRecordDetailFromId(namespace, appName, type, programName, runId);
    resetLogLevels(request, responder, namespace, appName, run.getVersion(), type, programName,
        runId);
  }

  /**
   * Reset the log level for a running program back to where it starts.
   *
   * Deprecated : Run-id is sufficient to identify a program run.
   */
  @POST
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runs/{run-id}/resetloglevels")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void resetProgramLogLevelsVersioned(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace,
      @PathParam("app-name") String appName,
      @PathParam("app-version") String appVersion,
      @PathParam("program-type") String type,
      @PathParam("program-name") String programName,
      @PathParam("run-id") String runId) throws Exception {
    resetLogLevels(request, responder, namespace, appName, appVersion, type, programName, runId);
  }

  private void getLiveInfo(HttpResponder responder, ProgramId programId,
      ProgramRuntimeService runtimeService) {
    try {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(runtimeService.getLiveInfo(programId)));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    }
  }

  /**
   * Returns the number of instances currently running for different runnables for different
   * programs
   */
  private int getInstanceCount(ProgramId programId, String runnableId) {
    ProgramLiveInfo info = runtimeService.getLiveInfo(programId);
    int count = 0;
    if (info instanceof NotRunningProgramLiveInfo) {
      return count;
    }
    if (info instanceof Containers) {
      Containers containers = (Containers) info;
      for (Containers.ContainerInfo container : containers.getContainers()) {
        if (container.getName().equals(runnableId)) {
          count++;
        }
      }
      return count;
    }
    // TODO: CDAP-1091: For standalone mode, returning the requested instances instead of provisioned only for services.
    // Doing this only for services to keep it consistent with the existing contract for flowlets right now.
    // The get instances contract for both flowlets and services should be re-thought and fixed as part of CDAP-1091
    if (programId.getType() == ProgramType.SERVICE) {
      return getRequestedServiceInstances(programId);
    }

    // Not running on YARN default 1
    return 1;
  }

  /**
   * Get requested and provisioned instances for a program type. The program type passed here should
   * be one that can have instances (flows, services, ...) Requires caller to do this validation.
   */
  private BatchRunnableInstances getProgramInstances(BatchRunnable runnable,
      ApplicationSpecification spec,
      ProgramId programId) {
    int requested;
    String programName = programId.getProgram();
    ProgramType programType = programId.getType();
    if (programType == ProgramType.WORKER) {
      if (!spec.getWorkers().containsKey(programName)) {
        return new BatchRunnableInstances(runnable, HttpResponseStatus.NOT_FOUND.code(),
            "Worker: " + programName + " not found");
      }
      requested = spec.getWorkers().get(programName).getInstances();

    } else if (programType == ProgramType.SERVICE) {
      if (!spec.getServices().containsKey(programName)) {
        return new BatchRunnableInstances(runnable, HttpResponseStatus.NOT_FOUND.code(),
            "Service: " + programName + " not found");
      }
      requested = spec.getServices().get(programName).getInstances();

    } else {
      return new BatchRunnableInstances(runnable, HttpResponseStatus.BAD_REQUEST.code(),
          "Instances not supported for program type + " + programType);
    }
    int provisioned = getInstanceCount(programId, programName);
    // use the pretty name of program types to be consistent
    return new BatchRunnableInstances(runnable, HttpResponseStatus.OK.code(), provisioned,
        requested);
  }

  private int getRequestedServiceInstances(ProgramId serviceId) {
    // Not running on YARN, get it from store
    return store.getServiceInstances(serviceId);
  }

  private boolean canHaveInstances(ProgramType programType) {
    return EnumSet.of(ProgramType.SERVICE, ProgramType.WORKER).contains(programType);
  }

  private void resetLogLevels(FullHttpRequest request, HttpResponder responder, String namespace,
      String appName,
      String appVersion, String type, String programName,
      String runId) throws Exception {
    ProgramType programType = getProgramType(type);
    try {
      Set<String> loggerNames = parseBody(request, SET_STRING_TYPE);
      runtimeLifecycleService.resetProgramLogLevels(
          new ApplicationId(namespace, appName, appVersion).program(programType, programName),
          loggerNames == null ? Collections.emptySet() : loggerNames, runId);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid JSON in body");
    } catch (SecurityException e) {
      throw new UnauthorizedException("Unauthorized to reset the log levels");
    }
  }

  private void updateLogLevels(FullHttpRequest request, HttpResponder responder, String namespace,
      String appName,
      String appVersion, String type, String programName,
      String runId) throws Exception {
    ProgramType programType = getProgramType(type);
    try {
      // we are decoding the body to Map<String, String> instead of Map<String, LogEntry.Level> here since Gson will
      // serialize invalid enum values to null, which is allowed for log level, instead of throw an Exception.
      runtimeLifecycleService.updateProgramLogLevels(
          new ApplicationId(namespace, appName, appVersion).program(programType, programName),
          transformLogLevelsMap(decodeArguments(request)), runId);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid JSON in body");
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage());
    } catch (SecurityException e) {
      throw new UnauthorizedException("Unauthorized to update the log levels");
    }
  }
}
