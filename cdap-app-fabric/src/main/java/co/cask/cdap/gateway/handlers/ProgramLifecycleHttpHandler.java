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

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.schedule.ScheduledRuntime;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.NotRunningProgramLiveInfo;
import co.cask.cdap.proto.ProgramLiveInfo;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ServiceInstances;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * {@link co.cask.http.HttpHandler} to manage program lifecycle for v3 REST APIs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class ProgramLifecycleHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramLifecycleHttpHandler.class);

  /**
   * Json serializer.
   */
  private static final Gson GSON = new Gson();

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  private final WorkflowClient workflowClient;

  /**
   * Factory for handling the location - can do both in either Distributed or Local mode.
   */
  private final LocationFactory locationFactory;

  /**
   * App fabric output directory.
   */
  private final String appFabricDir;

  /**
   * Configuration object passed from higher up.
   */
  private final CConfiguration configuration;

  /**
   * Runtime program service for running and managing programs.
   */
  private final ProgramRuntimeService runtimeService;

  private final DiscoveryServiceClient discoveryServiceClient;

  private final QueueAdmin queueAdmin;

  private final StreamAdmin streamAdmin;

  private final Scheduler scheduler;

  private final PreferencesStore preferencesStore;

  /**
   * Convenience class for representing the necessary components for retrieving status
   */
  private class StatusMap {
    private String status = null;
    private String error = null;
    private Integer statusCode = null;

    private StatusMap(String status, String error, int statusCode) {
      this.status = status;
      this.error = error;
      this.statusCode = statusCode;
    }

    public StatusMap() { }

    public int getStatusCode() {
      return statusCode;
    }

    public String getError() {
      return error;
    }

    public String getStatus() {
      return status;
    }

    public void setStatusCode(int statusCode) {
      this.statusCode = statusCode;
    }

    public void setError(String error) {
      this.error = error;
    }

    public void setStatus(String status) {
      this.status = status;
    }
  }

  @Inject
  public ProgramLifecycleHttpHandler(Authenticator authenticator, StoreFactory storeFactory,
                                     WorkflowClient workflowClient, LocationFactory locationFactory,
                                     CConfiguration configuration, ProgramRuntimeService runtimeService,
                                     DiscoveryServiceClient discoveryServiceClient, QueueAdmin queueAdmin,
                                     StreamAdmin streamAdmin, Scheduler scheduler, PreferencesStore preferencesStore) {
    super(authenticator);
    this.store = storeFactory.create();
    this.workflowClient = workflowClient;
    this.locationFactory = locationFactory;
    this.configuration = configuration;
    this.runtimeService = runtimeService;
    this.appFabricDir = this.configuration.get(Constants.AppFabric.OUTPUT_DIR);
    this.discoveryServiceClient = discoveryServiceClient;
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.scheduler = scheduler;
    this.preferencesStore = preferencesStore;
  }

  /**
   * Returns status of a type specified by the type{flows,workflows,mapreduce,spark,procedures,services,schedules}.
   */
  @GET
  @Path("/apps/{app-id}/{type}/{id}/status")
  public void getStatus(HttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") String namespaceId,
                        @PathParam("app-id") String appId,
                        @PathParam("type") String type,
                        @PathParam("id") String id) {

    if (type.equals("schedules")) {
      getScheduleStatus(responder, appId, namespaceId, id);
      return;
    }

    try {
      Id.Program program = Id.Program.from(namespaceId, appId, id);
      ProgramType programType = ProgramType.valueOfCategoryName(type);
      StatusMap statusMap = getStatus(program, programType);
      // If status is null, then there was an error
      if (statusMap.getStatus() == null) {
        responder.sendString(HttpResponseStatus.valueOf(statusMap.getStatusCode()), statusMap.getError());
        return;
      }
      Map<String, String> status = ImmutableMap.of("status", statusMap.getStatus());
      responder.sendJson(HttpResponseStatus.OK, status);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private void getScheduleStatus(HttpResponder responder, String appId, String namespaceId, String scheduleName) {
    try {
      ApplicationSpecification appSpec = store.getApplication(Id.Application.from(namespaceId, appId));
      if (appSpec == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "App: " + appId + " not found");
        return;
      }

      ScheduleSpecification scheduleSpec = appSpec.getSchedules().get(scheduleName);
      if (scheduleSpec == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Schedule: " + scheduleName + " not found");
        return;
      }

      String programName = scheduleSpec.getProgram().getProgramName();
      Id.Program programId = Id.Program.from(namespaceId, appId, programName);
      JsonObject json = new JsonObject();
      json.addProperty("status", scheduler.scheduleState(programId, scheduleSpec.getProgram().getProgramType(),
                                                         scheduleName).toString());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/apps/{app-id}/{type}/{id}/{action}")
  public void performAction(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId,
                            @PathParam("type") String type,
                            @PathParam("id") String id,
                            @PathParam("action") String action) {
    if (type.equals("schedules")) {
      suspendResumeSchedule(responder, namespaceId, appId, id, action);
      return;
    }

    if (!isValidAction(action)) {
      responder.sendStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
      return;
    }

    ProgramType programType = ProgramType.valueOfCategoryName(type);
    if ("debug".equals(action) && !isDebugAllowed(programType)) {
      responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
      return;
    }
    startStopProgram(request, responder, namespaceId, appId, programType, id, action);
  }

  private void suspendResumeSchedule(HttpResponder responder, String namespaceId, String appId, String scheduleName,
                                     String action) {
    try {

      if (!action.equals("suspend") && !action.equals("resume")) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Schedule can only be suspended or resumed.");
        return;
      }

      ApplicationSpecification appSpec = store.getApplication(Id.Application.from(namespaceId, appId));
      if (appSpec == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "App: " + appId + " not found");
        return;
      }

      ScheduleSpecification scheduleSpec = appSpec.getSchedules().get(scheduleName);
      if (scheduleSpec == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Schedule: " + scheduleName + " not found");
        return;
      }

      String programName = scheduleSpec.getProgram().getProgramName();
      Id.Program programId = Id.Program.from(namespaceId, appId, programName);
      Scheduler.ScheduleState state = scheduler.scheduleState(programId, scheduleSpec.getProgram().getProgramType(),
                                                              scheduleName);
      switch (state) {
        case NOT_FOUND:
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
          break;
        case SCHEDULED:
          if (action.equals("suspend")) {
            scheduler.suspendSchedule(programId, scheduleSpec.getProgram().getProgramType(), scheduleName);
            responder.sendJson(HttpResponseStatus.OK, "OK");
          } else {
            // attempt to resume already resumed schedule
            responder.sendJson(HttpResponseStatus.CONFLICT, "Already resumed");
          }
          break;
        case SUSPENDED:
          if (action.equals("suspend")) {
            // attempt to suspend already suspended schedule
            responder.sendJson(HttpResponseStatus.CONFLICT, "Schedule already suspended");
          } else {
            scheduler.resumeSchedule(programId, scheduleSpec.getProgram().getProgramType(), scheduleName);
            responder.sendJson(HttpResponseStatus.OK, "OK");
          }
          break;
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns program runs based on options it returns either currently running or completed or failed.
   * Default it returns all.
   */
  @GET
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/runs")
  public void runnableHistory(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @PathParam("app-id") String appId,
                              @PathParam("runnable-type") String runnableType,
                              @PathParam("runnable-id") String runnableId,
                              @QueryParam("status") String status,
                              @QueryParam("start") String startTs,
                              @QueryParam("end") String endTs,
                              @QueryParam("limit") @DefaultValue("100") final int resultLimit) {
    ProgramType type = ProgramType.valueOfCategoryName(runnableType);
    if (type == null || type == ProgramType.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }
    long start = (startTs == null || startTs.isEmpty()) ? Long.MIN_VALUE : Long.parseLong(startTs);
    long end = (endTs == null || endTs.isEmpty()) ? Long.MAX_VALUE : Long.parseLong(endTs);
    getRuns(responder, namespaceId, appId, runnableId, status, start, end, resultLimit);
  }

  /**
   * Get runnable runtime args.
   */
  @GET
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/runtimeargs")
  public void getRunnableRuntimeArgs(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-id") String appId,
                                     @PathParam("runnable-type") String runnableType,
                                     @PathParam("runnable-id") String runnableId) {
    ProgramType type = ProgramType.valueOfCategoryName(runnableType);
    if (type == null || type == ProgramType.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    Id.Program id = Id.Program.from(namespaceId, appId, runnableId);

    try {
      if (!store.programExists(id, type)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Runnable not found");
        return;
      }
      Map<String, String> runtimeArgs = preferencesStore.getProperties(id.getNamespaceId(), appId,
                                                                       runnableType, runnableId);
      responder.sendJson(HttpResponseStatus.OK, runtimeArgs);
    } catch (Throwable e) {
      LOG.error("Error getting runtime args {}", e.getMessage(), e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Save runnable runtime args.
   */
  @PUT
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/runtimeargs")
  public void saveRunnableRuntimeArgs(HttpRequest request, HttpResponder responder,
                                      @PathParam("namespace-id") String namespaceId,
                                      @PathParam("app-id") String appId,
                                      @PathParam("runnable-type") String runnableType,
                                      @PathParam("runnable-id") String runnableId) {
    ProgramType type = ProgramType.valueOfCategoryName(runnableType);
    if (type == null || type == ProgramType.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    Id.Program id = Id.Program.from(namespaceId, appId, runnableId);


    try {
      if (!store.programExists(id, type)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Runnable not found");
        return;
      }
      Map<String, String> args = decodeArguments(request);
      preferencesStore.setProperties(namespaceId, appId, runnableType, runnableId, args);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Throwable e) {
      LOG.error("Error getting runtime args {}", e.getMessage(), e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}")
  public void runnableSpecification(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId, @PathParam("app-id") String appId,
                                    @PathParam("runnable-type") String runnableType,
                                    @PathParam("runnable-id") String runnableId) {

    ProgramType type = getProgramType(runnableType);
    if (type == null) {
      responder.sendString(HttpResponseStatus.METHOD_NOT_ALLOWED, String.format("Program type '%s' not supported",
                                                                                runnableType));
      return;
    }

    try {
      Id.Program id = Id.Program.from(namespaceId, appId, runnableId);
      ProgramSpecification specification = getProgramSpecification(id, type);
      if (specification == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        responder.sendJson(HttpResponseStatus.OK, specification);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns the status for all programs that are passed into the data. The data is an array of Json objects
   * where each object must contain the following three elements: appId, programType, and programId
   * (flow name, service name, etc.).
   * <p/>
   * Example input:
   * [{"appId": "App1", "programType": "Service", "programId": "Service1"},
   * {"appId": "App1", "programType": "Procedure", "programId": "Proc2"},
   * {"appId": "App2", "programType": "Flow", "programId": "Flow1"}]
   * <p/>
   * The response will be an array of JsonObjects each of which will contain the three input parameters
   * as well as 2 fields, "status" which maps to the status of the program and "statusCode" which maps to the
   * status code for the data in that JsonObjects. If an error occurs in the
   * input (i.e. in the example above, App2 does not exist), then all JsonObjects for which the parameters
   * have a valid status will have the status field but all JsonObjects for which the parameters do not have a valid
   * status will have an error message and statusCode.
   * <p/>
   * For example, if there is no App2 in the data above, then the response would be 200 OK with following possible data:
   * [{"appId": "App1", "programType": "Service", "programId": "Service1", "statusCode": 200, "status": "RUNNING"},
   * {"appId": "App1", "programType": "Procedure", "programId": "Proc2"}, "statusCode": 200, "status": "STOPPED"},
   * {"appId":"App2", "programType":"Flow", "programId":"Flow1", "statusCode":404, "error": "App: App2 not found"}]
   *
   * @param request
   * @param responder
   */
  @POST
  @Path("/status")
  public void getStatuses(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId) {
    try {
      List<BatchEndpointStatus> args = statusFromBatchArgs(decodeArrayArguments(request, responder));
      // if args is null, then there was an error in decoding args and response was already sent
      if (args == null) {
        return;
      }
      for (BatchEndpointStatus requestedObj : args) {
        Id.Program progId = Id.Program.from(namespaceId, requestedObj.getAppId(), requestedObj.getProgramId());
        ProgramType programType = ProgramType.valueOfPrettyName(requestedObj.getProgramType());
        // get th statuses
        StatusMap statusMap = getStatus(progId, programType);
        if (statusMap.getStatus() != null) {
          requestedObj.setStatusCode(HttpResponseStatus.OK.getCode());
          requestedObj.setStatus(statusMap.getStatus());
        } else {
          requestedObj.setStatusCode(statusMap.getStatusCode());
          requestedObj.setError(statusMap.getError());
        }
        // set the program type to the pretty name in case the request originally didn't have pretty name
        requestedObj.setProgramType(programType.getPrettyName());
      }
      responder.sendJson(HttpResponseStatus.OK, args);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns the number of instances for all program runnables that are passed into the data. The data is an array of
   * Json objects where each object must contain the following three elements: appId, programType, and programId
   * (flow name, service name, or procedure name). Retrieving instances only applies to flows, procedures, and user
   * services. For flows and procedures, another parameter, "runnableId", must be provided. This corresponds to the
   * flowlet/runnable for which to retrieve the instances. This does not apply to procedures.
   *
   * Example input:
   * [{"appId": "App1", "programType": "Service", "programId": "Service1", "runnableId": "Runnable1"},
   *  {"appId": "App1", "programType": "Procedure", "programId": "Proc2"},
   *  {"appId": "App2", "programType": "Flow", "programId": "Flow1", "runnableId": "Flowlet1"}]
   *
   * The response will be an array of JsonObjects each of which will contain the three input parameters
   * as well as 3 fields:
   * "provisioned" which maps to the number of instances actually provided for the input runnable,
   * "requested" which maps to the number of instances the user has requested for the input runnable,
   * "statusCode" which maps to the http status code for the data in that JsonObjects. (200, 400, 404)
   * If an error occurs in the input (i.e. in the example above, Flowlet1 does not exist), then all JsonObjects for
   * which the parameters have a valid instances will have the provisioned and requested fields status code fields
   * but all JsonObjects for which the parameters are not valid will have an error message and statusCode.
   *
   * E.g. given the above data, if there is no Flowlet1, then the response would be 200 OK with following possible data:
   * [{"appId": "App1", "programType": "Service", "programId": "Service1", "runnableId": "Runnable1",
   *   "statusCode": 200, "provisioned": 2, "requested": 2},
   *  {"appId": "App1", "programType": "Procedure", "programId": "Proc2", "statusCode": 200, "provisioned": 1,
   *   "requested": 3},
   *  {"appId": "App2", "programType": "Flow", "programId": "Flow1", "runnableId": "Flowlet1", "statusCode": 404,
   *   "error": "Runnable": Flowlet1 not found"}]
   *
   * @param request
   * @param responder
   */
  @POST
  @Path("/instances")
  public void getInstances(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) {
    try {
      List<BatchEndpointInstances> args = instancesFromBatchArgs(decodeArrayArguments(request, responder));
      // if args is null then the response has already been sent
      if (args == null) {
        return;
      }
      for (BatchEndpointInstances requestedObj : args) {
        String appId = requestedObj.getAppId();
        ApplicationSpecification spec = store.getApplication(Id.Application.from(namespaceId, appId));
        if (spec == null) {
          addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(), "App: " + appId + " not found");
          continue;
        }

        String programTypeStr = requestedObj.getProgramType();
        ProgramType programType = ProgramType.valueOfPrettyName(programTypeStr);
        // cant get instances for things that are not flows, services, or procedures
        if (!canHaveInstances(programType)) {
          addCodeError(requestedObj, HttpResponseStatus.BAD_REQUEST.getCode(),
                       "Program type: " + programType + " is not a valid program type to get instances");
          continue;
        }

        populateRunnableInstances(requestedObj, namespaceId, appId, spec, programType, requestedObj.getProgramId());
      }
      responder.sendJson(HttpResponseStatus.OK, args);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (JsonSyntaxException e) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /*
  Note: Cannot combine the following get all programs methods into one because then API path will clash with /apps path
   */

  /**
   * Returns a list of flows associated with a namespace.
   */
  @GET
  @Path("/flows")
  public void getAllFlows(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId) {
    programList(responder, namespaceId, ProgramType.FLOW, null, store);
  }

  /**
   * Returns a list of procedures associated with a namespace.
   */
  @GET
  @Path("/procedures")
  public void getAllProcedures(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId) {
    programList(responder, namespaceId, ProgramType.PROCEDURE, null, store);
  }

  /**
   * Returns a list of map/reduces associated with a namespace.
   */
  @GET
  @Path("/mapreduce")
  public void getAllMapReduce(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId) {
    programList(responder, namespaceId, ProgramType.MAPREDUCE, null, store);
  }

  /**
   * Returns a list of spark jobs associated with a namespace.
   */
  @GET
  @Path("/spark")
  public void getAllSpark(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId) {
    programList(responder, namespaceId, ProgramType.SPARK, null, store);
  }

  /**
   * Returns a list of workflows associated with a namespace.
   */
  @GET
  @Path("/workflows")
  public void getAllWorkflows(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId) {
    programList(responder, namespaceId, ProgramType.WORKFLOW, null, store);
  }

  /**
   * Returns a list of services associated with a namespace.
   */
  @GET
  @Path("/services")
  public void getAllServices(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId) {
    programList(responder, namespaceId, ProgramType.SERVICE, null, store);
  }

  @GET
  @Path("/workers")
  public void getAllWorkers(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) {
    programList(responder, namespaceId, ProgramType.WORKER, null, store);
  }

  /**
   * Returns a list of programs associated with an application within a namespace.
   */
  @GET
  @Path("/apps/{app-id}/{program-category}")
  public void getProgramsByApp(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("app-id") String appId,
                               @PathParam("program-category") String programCategory) {
    ProgramType type = getProgramType(programCategory);
    if (type == null) {
      responder.sendString(HttpResponseStatus.METHOD_NOT_ALLOWED, String.format("Program type '%s' not supported",
                                                                                programCategory));
      return;
    }
    programList(responder, namespaceId, type, appId, store);
  }

  /********************** Flow/Flowlet APIs ***********************************************************/
  /**
   * Returns number of instances for a flowlet within a flow.
   */
  @GET
  @Path("/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/instances")
  public void getFlowletInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId, @PathParam("flow-id") String flowId,
                                  @PathParam("flowlet-id") String flowletId) {
    try {
      int count = store.getFlowletInstances(Id.Program.from(namespaceId, appId, flowId), flowletId);
      responder.sendJson(HttpResponseStatus.OK, new Instances(count));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      if (respondIfElementNotFound(e, responder)) {
        return;
      }
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Increases number of instance for a flowlet within a flow.
   */
  @PUT
  @Path("/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/instances")
  public void setFlowletInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId, @PathParam("flow-id") String flowId,
                                  @PathParam("flowlet-id") String flowletId) {
    int instances = 0;
    try {
      instances = getInstances(request);
      if (instances < 1) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Instance count should be greater than 0");
        return;
      }
    } catch (Throwable th) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid instance count.");
      return;
    }

    try {
      Id.Program programID = Id.Program.from(namespaceId, appId, flowId);
      int oldInstances = store.getFlowletInstances(programID, flowletId);
      if (oldInstances != instances) {
        store.setFlowletInstances(programID, flowletId, instances);
        ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(namespaceId, appId, flowId, ProgramType.FLOW,
                                                                        runtimeService);
        if (runtimeInfo != null) {
          runtimeInfo.getController().command(ProgramOptionConstants.INSTANCES,
                                              ImmutableMap.of("flowlet", flowletId,
                                                              "newInstances", String.valueOf(instances),
                                                              "oldInstances", String.valueOf(oldInstances))).get();
        }
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      if (respondIfElementNotFound(e, responder)) {
        return;
      }
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Changes input stream for a flowlet connection.
   */
  @PUT
  @Path("/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/connections/{stream-id}")
  public void changeFlowletStreamConnection(HttpRequest request, HttpResponder responder,
                                            @PathParam("namespace-id") String namespaceId,
                                            @PathParam("app-id") String appId,
                                            @PathParam("flow-id") String flowId,
                                            @PathParam("flowlet-id") String flowletId,
                                            @PathParam("stream-id") String streamId) throws IOException {
    try {
      Map<String, String> arguments = decodeArguments(request);
      String oldStreamId = arguments.get("oldStreamId");
      if (oldStreamId == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "oldStreamId param is required");
        return;
      }

      StreamSpecification stream = store.getStream(Id.Namespace.from(namespaceId), streamId);
      if (stream == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream specified with streamId param does not exist");
        return;
      }

      Id.Program programID = Id.Program.from(namespaceId, appId, flowId);
      store.changeFlowletSteamConnection(programID, flowletId, oldStreamId, streamId);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      if (respondIfElementNotFound(e, responder)) {
        return;
      }
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/apps/{app-id}/{program-category}/{program-id}/live-info")
  @SuppressWarnings("unused")
  public void liveInfo(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                       @PathParam("app-id") String appId, @PathParam("program-category") String programCategory,
                       @PathParam("program-id") String programId) {
    ProgramType type = getProgramType(programCategory);
    if (type == null) {
      responder.sendString(HttpResponseStatus.METHOD_NOT_ALLOWED, String.format("Live-info not supported for program" +
                                                                                  " type '%s'", programCategory));
      return;
    }
    getLiveInfo(request, responder, namespaceId, appId, programId, ProgramType.valueOfCategoryName(programCategory),
                runtimeService);
  }

  /**
   * Deletes queues.
   */
  @DELETE
  @Path("/apps/{app-id}/flows/{flow-id}/queues")
  public void deleteFlowQueues(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("app-id") String appId,
                               @PathParam("flow-id") String flowId) {
    Id.Program programId = Id.Program.from(namespaceId, appId, flowId);
    try {
      ProgramStatus status = getProgramStatus(programId, ProgramType.FLOW);
      if (status.getStatus().equals(HttpResponseStatus.NOT_FOUND.toString())) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else if (status.getStatus().equals("RUNNING")) {
        responder.sendString(HttpResponseStatus.FORBIDDEN, "Flow is running, please stop it first.");
      } else {
        queueAdmin.dropAllForFlow(namespaceId, appId, flowId);
        // delete process metrics that are used to calculate the queue size (process.events.pending metric name)
        deleteProcessMetricsForFlow(appId, flowId);
        responder.sendStatus(HttpResponseStatus.OK);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**************************** Workflow/schedule APIs *****************************************************/
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-name}/current")
  public void workflowStatus(HttpRequest request, final HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-id") String appId, @PathParam("workflow-name") String workflowName) {

    try {
      workflowClient.getWorkflowStatus(namespaceId, appId, workflowName,
                                       new WorkflowClient.Callback() {
                                         @Override
                                         public void handle(WorkflowClient.Status status) {
                                           if (status.getCode() == WorkflowClient.Status.Code.NOT_FOUND) {
                                             responder.sendStatus(HttpResponseStatus.NOT_FOUND);
                                           } else if (status.getCode() == WorkflowClient.Status.Code.OK) {
                                             responder.sendByteArray(HttpResponseStatus.OK,
                                                                     status.getResult().getBytes(),
                                                                     ImmutableMultimap.of(
                                                                       HttpHeaders.Names.CONTENT_TYPE,
                                                                       "application/json; charset=utf-8"));

                                           } else {
                                             responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                                 status.getResult());
                                           }
                                         }
                                       });
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns next scheduled runtime of a workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/nextruntime")
  public void getScheduledRunTime(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId, @PathParam("workflow-id") String workflowId) {
    try {
      Id.Program id = Id.Program.from(namespaceId, appId, workflowId);
      List<ScheduledRuntime> runtimes = scheduler.nextScheduledRuntime(id, SchedulableProgramType.WORKFLOW);

      JsonArray array = new JsonArray();
      for (ScheduledRuntime runtime : runtimes) {
        JsonObject object = new JsonObject();
        object.addProperty("id", runtime.getScheduleId());
        object.addProperty("time", runtime.getTime());
        array.add(object);
      }
      responder.sendJson(HttpResponseStatus.OK, array);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Get Workflow schedules
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/schedules")
  public void getWorkflowSchedules(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId,
                                   @PathParam("app-id") String appId,
                                   @PathParam("workflow-id") String workflowId) {
    ApplicationSpecification appSpec = store.getApplication(Id.Application.from(namespaceId, appId));
    if (appSpec == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "App:" + appId + " not found");
      return;
    }

    List<ScheduleSpecification> specList = Lists.newArrayList();
    for (Map.Entry<String, ScheduleSpecification> entry : appSpec.getSchedules().entrySet()) {
      ScheduleSpecification spec = entry.getValue();
      if (spec.getProgram().getProgramName().equals(workflowId) &&
        spec.getProgram().getProgramType() == SchedulableProgramType.WORKFLOW) {
        specList.add(entry.getValue());
      }
    }
    responder.sendJson(HttpResponseStatus.OK, specList);
  }

  /**
   * Return the number of instances for the given runnable of a service.
   */
  @GET
  @Path("/apps/{app-id}/services/{service-id}/runnables/{runnable-name}/instances")
  public void getServiceInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId,
                                  @PathParam("service-id") String serviceId,
                                  @PathParam("runnable-name") String runnableName) {
    try {
      Id.Program programId = Id.Program.from(namespaceId, appId, serviceId);
      if (!store.programExists(programId, ProgramType.SERVICE)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Runnable not found");
        return;
      }

      ServiceSpecification specification = (ServiceSpecification) getProgramSpecification(programId,
                                                                                          ProgramType.SERVICE);
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
                         new ServiceInstances(instances, getRunnableCount(namespaceId, appId, ProgramType.SERVICE,
                                                                          serviceId, runnableName)));

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
  public void setServiceInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId,
                                  @PathParam("service-id") String serviceId,
                                  @PathParam("runnable-name") String runnableName) {

    try {
      Id.Program programId = Id.Program.from(namespaceId, appId, serviceId);
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

        ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId.getNamespaceId(),
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

  @DELETE
  @Path("/queues")
  public synchronized void deleteQueues(HttpRequest request, HttpResponder responder,
                                        @PathParam("namespace-id") String namespaceId) {
    // synchronized to avoid a potential race condition here:
    // 1. the check for state returns that all flows are STOPPED
    // 2. The API deletes queues because
    // Between 1. and 2., a flow is started using the /namespaces/{namespace-id}/apps/{app-id}/flows/{flow-id}/start API
    // Averting this race condition by synchronizing this method. The resource that needs to be locked here is
    // runtimeService. This should work because the method that is used to start a flow - startStopProgram - is also
    // synchronized on this.
    // This synchronization works in HA mode because even in HA mode there is only one leader at a time.
    try {
      List<ProgramRecord> flows = listPrograms(Id.Namespace.from(namespaceId), ProgramType.FLOW, store);
      for (ProgramRecord flow : flows) {
        String appId = flow.getApp();
        String flowId = flow.getId();
        Id.Program programId = Id.Program.from(namespaceId, appId, flowId);
        ProgramStatus status = getProgramStatus(programId, ProgramType.FLOW);
        if (!"STOPPED".equals(status.getStatus())) {
          responder.sendString(HttpResponseStatus.FORBIDDEN,
                               String.format("Flow '%s' from application '%s' in namespace '%s' is running, " +
                                               "please stop it first.", flowId, appId, namespaceId));
          return;
        }
      }
      queueAdmin.dropAllInNamespace(namespaceId);
      // delete process metrics that are used to calculate the queue size (process.events.pending metric name)
      // TODO: CDAP-1184 Implement metrics deletion once we have v3 APIs for deleting metrics
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Exception e) {
      LOG.error("Error while deleting queues in namespace " + namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Populates requested and provisioned instances for a program type.
   * The program type passed here should be one that can have instances (flows, services or procedures)
   * Requires caller to do this validation.
   */
  private void populateRunnableInstances(BatchEndpointInstances requestedObj, String namespaceId, String appId,
                                         ApplicationSpecification spec, ProgramType programType,
                                         String programId) {
    int requested;
    String runnableId;
    if (programType == ProgramType.PROCEDURE) {
      // the "runnable" for procedures has the same id as the procedure name
      runnableId = programId;
      if (!spec.getProcedures().containsKey(programId)) {
        addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(),
                     "Procedure: " + programId + " not found");
        return;
      }
      requested = store.getProcedureInstances(Id.Program.from(namespaceId, appId, programId));

    } else {
      // services and flows must have runnable id
      if (requestedObj.getRunnableId() == null) {
        addCodeError(requestedObj, HttpResponseStatus.BAD_REQUEST.getCode(),
                     "Must provide a string runnableId for flows/services");
        return;
      }

      runnableId = requestedObj.getRunnableId();
      if (programType == ProgramType.FLOW) {
        FlowSpecification flowSpec = spec.getFlows().get(programId);
        if (flowSpec == null) {
          addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(), "Flow: " + programId + " not found");
          return;
        }

        FlowletDefinition flowletDefinition = flowSpec.getFlowlets().get(runnableId);
        if (flowletDefinition == null) {
          addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(),
                       "Flowlet: " + runnableId + " not found");
          return;
        }
        requested = flowletDefinition.getInstances();

      } else {
        // Services
        ServiceSpecification serviceSpec = spec.getServices().get(programId);
        if (serviceSpec == null) {
          addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(),
                       "Service: " + programId + " not found");
          return;
        }

        if (serviceSpec.getName().equals(runnableId)) {
          // If runnable name is the same as the service name, returns the service http server instances
          requested = serviceSpec.getInstances();
        } else {
          // Otherwise, get it from the worker
          ServiceWorkerSpecification workerSpec = serviceSpec.getWorkers().get(runnableId);
          if (workerSpec == null) {
            addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(),
                         "Runnable: " + runnableId + " not found");
            return;
          }
          requested = workerSpec.getInstances();
        }
      }
    }
    // use the pretty name of program types to be consistent
    requestedObj.setProgramType(programType.getPrettyName());
    int provisioned = getRunnableCount(namespaceId, appId, programType, programId, runnableId);
    requestedObj.setStatusCode(HttpResponseStatus.OK.getCode());
    requestedObj.setRequested(requested);
    requestedObj.setProvisioned(provisioned);
  }

  /**
   * Returns a map where the pairs map from status to program status (e.g. {"status" : "RUNNING"}) or
   * in case of an error in the input (e.g. invalid id, program not found), a map from statusCode to integer and
   * error to error message (e.g. {"statusCode": 404, "error": "Program not found"})
   *
   * @param id The Program Id to get the status of
   * @param type The Type of the Program to get the status of
   * @throws RuntimeException if failed to determine the program status
   */
  private StatusMap getStatus(final Id.Program id, final ProgramType type) {
    // invalid type does not exist
    if (type == null) {
      return new StatusMap(null, "Invalid program type provided", HttpResponseStatus.BAD_REQUEST.getCode());
    }

    try {
      // check that app exists
      ApplicationSpecification appSpec = store.getApplication(id.getApplication());
      if (appSpec == null) {
        return new StatusMap(null, "App: " + id.getApplicationId() + " not found",
                             HttpResponseStatus.NOT_FOUND.getCode());
      }

      // For program type other than MapReduce
      if (type != ProgramType.MAPREDUCE) {
        return getProgramStatus(id, type, new StatusMap());
      }

      // must do it this way to allow anon function in workflow to modify status
      // check that mapreduce exists
      if (!appSpec.getMapReduce().containsKey(id.getId())) {
        return new StatusMap(null, "Program: " + id.getId() + " not found", HttpResponseStatus.NOT_FOUND.getCode());
      }

      // See if the MapReduce is part of a workflow
      String workflowName = getWorkflowName(id.getId());
      if (workflowName == null) {
        // Not from workflow, treat it as simple program status
        return getProgramStatus(id, type, new StatusMap());
      }

      // MapReduce is part of a workflow. Query the status of the workflow instead
      final SettableFuture<StatusMap> statusFuture = SettableFuture.create();
      workflowClient.getWorkflowStatus(id.getNamespaceId(), id.getApplicationId(),
                                       workflowName, new WorkflowClient.Callback() {
        @Override
        public void handle(WorkflowClient.Status status) {
          StatusMap result = new StatusMap();

          if (status.getCode().equals(WorkflowClient.Status.Code.OK)) {
            result.setStatus("RUNNING");
            result.setStatusCode(HttpResponseStatus.OK.getCode());
          } else {
            //mapreduce name might follow the same format even when its not part of the workflow.
            try {
              // getProgramStatus returns program status or http response status NOT_FOUND
              getProgramStatus(id, type, result);
            } catch (Exception e) {
              LOG.error("Exception raised when getting program status for {} {}", id, type, e);
              // error occurred so say internal server error
              result.setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode());
              result.setError(e.getMessage());
            }
          }

          // This would make all changes in the result statusMap available to the other thread that doing
          // the take() call.
          statusFuture.set(result);
        }
      }
      );
      // wait for status to come back in case we are polling mapreduce status in workflow
      // status map contains either a status or an error
      return Futures.getUnchecked(statusFuture);
    } catch (Exception e) {
      LOG.error("Exception raised when getting program status for {} {}", id, type, e);
      return new StatusMap(null, "Failed to get program status", HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode());
    }
  }

  private StatusMap getProgramStatus(Id.Program id, ProgramType type, StatusMap statusMap) {
    // getProgramStatus returns program status or http response status NOT_FOUND
    String progStatus = getProgramStatus(id, type).getStatus();
    if (progStatus.equals(HttpResponseStatus.NOT_FOUND.toString())) {
      statusMap.setStatusCode(HttpResponseStatus.NOT_FOUND.getCode());
      statusMap.setError("Program not found");
    } else {
      statusMap.setStatus(progStatus);
      statusMap.setStatusCode(HttpResponseStatus.OK.getCode());
    }
    return statusMap;
  }

  /**
   * 'protected' only to support v2 webapp APIs
   */
  protected ProgramStatus getProgramStatus(Id.Program id, ProgramType type) {
    try {
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(id, type);

      if (runtimeInfo == null) {
        if (type != ProgramType.WEBAPP) {
          //Runtime info not found. Check to see if the program exists.
          ProgramSpecification spec = getProgramSpecification(id, type);
          if (spec == null) {
            // program doesn't exist
            return new ProgramStatus(id.getApplicationId(), id.getId(), HttpResponseStatus.NOT_FOUND.toString());
          } else {
            // program exists and not running. so return stopped.
            return new ProgramStatus(id.getApplicationId(), id.getId(), ProgramController.State.STOPPED.toString());
          }
        } else {
          // TODO: Fetching webapp status is a hack. This will be fixed when webapp spec is added.
          Location webappLoc = null;
          try {
            webappLoc = Programs.programLocation(locationFactory, appFabricDir, id, ProgramType.WEBAPP);
          } catch (FileNotFoundException e) {
            // No location found for webapp, no need to log this exception
          }

          if (webappLoc != null && webappLoc.exists()) {
            // webapp exists and not running. so return stopped.
            return new ProgramStatus(id.getApplicationId(), id.getId(), ProgramController.State.STOPPED.toString());
          } else {
            // webapp doesn't exist
            return new ProgramStatus(id.getApplicationId(), id.getId(), HttpResponseStatus.NOT_FOUND.toString());
          }
        }
      }

      String status = controllerStateToString(runtimeInfo.getController().getState());
      return new ProgramStatus(id.getApplicationId(), id.getId(), status);
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw Throwables.propagate(throwable);
    }
  }

  /**
   * Temporarily protected. Should be made private when all v3 APIs (webapp in this case) have been implemented.
   */
  protected ProgramRuntimeService.RuntimeInfo findRuntimeInfo(Id.Program identifier, ProgramType type) {
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(type).values();
    Preconditions.checkNotNull(runtimeInfos, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND),
                               identifier.getNamespaceId(), identifier.getApplicationId());
    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (identifier.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }

  /**
   * Get workflow name from mapreduceId.
   * Format of mapreduceId: WorkflowName_mapreduceName, if the mapreduce is a part of workflow.
   *
   * @param mapreduceId id of the mapreduce job in CDAP
   * @return workflow name if exists null otherwise
   */
  private String getWorkflowName(String mapreduceId) {
    String [] splits = mapreduceId.split("_");
    if (splits.length > 1) {
      return splits[0];
    } else {
      return null;
    }
  }

  @Nullable
  private ProgramSpecification getProgramSpecification(Id.Program id, ProgramType type) throws Exception {
    ApplicationSpecification appSpec;
    try {
      appSpec = store.getApplication(id.getApplication());
      if (appSpec == null) {
        return null;
      }

      String runnableId = id.getId();
      ProgramSpecification programSpec;
      if (type == ProgramType.FLOW && appSpec.getFlows().containsKey(runnableId)) {
        programSpec = appSpec.getFlows().get(id.getId());
      } else if (type == ProgramType.PROCEDURE && appSpec.getProcedures().containsKey(runnableId)) {
        programSpec = appSpec.getProcedures().get(id.getId());
      } else if (type == ProgramType.MAPREDUCE && appSpec.getMapReduce().containsKey(runnableId)) {
        programSpec = appSpec.getMapReduce().get(id.getId());
      } else if (type == ProgramType.SPARK && appSpec.getSpark().containsKey(runnableId)) {
        programSpec = appSpec.getSpark().get(id.getId());
      } else if (type == ProgramType.WORKFLOW && appSpec.getWorkflows().containsKey(runnableId)) {
        programSpec = appSpec.getWorkflows().get(id.getId());
      } else if (type == ProgramType.SERVICE && appSpec.getServices().containsKey(runnableId)) {
        programSpec = appSpec.getServices().get(id.getId());
      } else if (type == ProgramType.WORKER && appSpec.getWorkers().containsKey(runnableId)) {
        programSpec = appSpec.getWorkers().get(id.getId());
      } else {
        programSpec = null;
      }
      return programSpec;
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new Exception(throwable.getMessage());
    }
  }

  /** NOTE: This was a temporary hack done to map the status to something that is
   * UI friendly. Internal states of program controller are reasonable and hence
   * no point in changing them.
   */
  private String controllerStateToString(ProgramController.State state) {
    if (state == ProgramController.State.ALIVE) {
      return "RUNNING";
    }
    if (state == ProgramController.State.ERROR) {
      return "FAILED";
    }
    return state.toString();
  }

  private synchronized void startStopProgram(HttpRequest request, HttpResponder responder, String namespaceId,
                                             String appId, ProgramType runnableType, String runnableId,
                                             String action) {
    if (runnableType == null || (runnableType == ProgramType.WORKFLOW && "stop".equals(action))) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      LOG.trace("{} call from AppFabricHttpHandler for app {}, flow type {} id {}",
                action, appId, runnableType, runnableId);
      runnableStartStop(request, responder, namespaceId, appId, runnableId, runnableType, action);
    }
  }

  /**
   * Protected temporarily until all v2 APIs are migrated (webapp APIs in this case).
   */
  protected void runnableStartStop(HttpRequest request, HttpResponder responder, String namespaceId, String appId,
                                   String runnableId, ProgramType type, String action) {
    try {
      Id.Program id = Id.Program.from(namespaceId, appId, runnableId);
      AppFabricServiceStatus status = null;
      if ("start".equals(action)) {
        status = start(id, type, decodeArguments(request), false);
      } else if ("debug".equals(action)) {
        status = start(id, type, decodeArguments(request), true);
      } else if ("stop".equals(action)) {
        status = stop(id, type);
      }
      if (status == AppFabricServiceStatus.INTERNAL_ERROR) {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        return;
      }

      responder.sendString(status.getCode(), status.getMessage());
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Starts a Program.
   */
  private AppFabricServiceStatus start(final Id.Program id, ProgramType type,
                                       Map<String, String> overrides, boolean debug) {

    try {
      Program program = store.loadProgram(id, type);
      if (program == null) {
        return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
      }

      if (isRunning(id, type)) {
        return AppFabricServiceStatus.PROGRAM_ALREADY_RUNNING;
      }

      Map<String, String> userArgs = preferencesStore.getResolvedProperties(id.getNamespaceId(), id.getApplicationId(),
                                                                            type.getCategoryName(), id.getId());
      if (overrides != null) {
        for (Map.Entry<String, String> entry : overrides.entrySet()) {
          userArgs.put(entry.getKey(), entry.getValue());
        }
      }

      BasicArguments userArguments = new BasicArguments(userArgs);
      ProgramRuntimeService.RuntimeInfo runtimeInfo =
        runtimeService.run(program, new SimpleProgramOptions(id.getId(), new BasicArguments(), userArguments, debug));

      final ProgramController controller = runtimeInfo.getController();
      final String runId = controller.getRunId().getId();

      controller.addListener(new AbstractListener() {

        @Override
        public void init(ProgramController.State state) {
          store.setStart(id, runId, TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
          if (state == ProgramController.State.STOPPED) {
            stopped();
          }
          if (state == ProgramController.State.ERROR) {
            error(controller.getFailureCause());
          }
        }
        @Override
        public void stopped() {
          store.setStop(id, runId,
                        TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                        ProgramController.State.STOPPED);
        }

        @Override
        public void error(Throwable cause) {
          LOG.info("Program stopped with error {}, {}", id, runId, cause);
          store.setStop(id, runId,
                        TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                        ProgramController.State.ERROR);
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      return AppFabricServiceStatus.OK;
    } catch (DatasetInstantiationException e) {
      return new AppFabricServiceStatus(HttpResponseStatus.UNPROCESSABLE_ENTITY, e.getMessage());
    } catch (Throwable throwable) {
      LOG.error(throwable.getMessage(), throwable);
      if (throwable instanceof FileNotFoundException) {
        return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
      }
      return AppFabricServiceStatus.INTERNAL_ERROR;
    }
  }

  private boolean isRunning(Id.Program id, ProgramType type) {
    String programStatus = getStatus(id, type).getStatus();
    return programStatus != null && !"STOPPED".equals(programStatus);
  }

  /**
   * Stops a Program.
   */
  private AppFabricServiceStatus stop(Id.Program identifier, ProgramType type) {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(identifier, type);
    if (runtimeInfo == null) {
      try {
        ProgramStatus status = getProgramStatus(identifier, type);
        if (status.getStatus().equals(HttpResponseStatus.NOT_FOUND.toString())) {
          return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
        } else if (ProgramController.State.STOPPED.toString().equals(status.getStatus())) {
          return AppFabricServiceStatus.PROGRAM_ALREADY_STOPPED;
        } else {
          return AppFabricServiceStatus.RUNTIME_INFO_NOT_FOUND;
        }
      } catch (Exception e) {
        if (e instanceof FileNotFoundException) {
          return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
        }
        return AppFabricServiceStatus.INTERNAL_ERROR;
      }
    }

    try {
      Preconditions.checkNotNull(runtimeInfo, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND));
      ProgramController controller = runtimeInfo.getController();
      controller.stop().get();
      return AppFabricServiceStatus.OK;
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      return AppFabricServiceStatus.INTERNAL_ERROR;
    }
  }

  private void getRuns(HttpResponder responder, String namespaceId, String appId, String runnableId, String status,
                       long start, long end, int limit) {
    try {
      Id.Program programId = Id.Program.from(namespaceId, appId, runnableId);
      try {
        ProgramRunStatus runStatus = (status == null) ? ProgramRunStatus.ALL :
          ProgramRunStatus.valueOf(status.toUpperCase());
        responder.sendJson(HttpResponseStatus.OK, store.getRuns(programId, runStatus, start, end, limit));
      } catch (IllegalArgumentException e) {
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                             "Supported options for status of runs are running/completed/failed");
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Deserializes and parses the HttpRequest data into a list of JsonObjects. Checks the HttpRequest data to see that
   * the input has valid fields corresponding to the /instances and /status endpoints. If the input data is empty or
   * the data is not of the form of an array of json objects, it sends an appropriate response through the responder
   * and returns null.
   *
   * @param request The HttpRequest to parse
   * @param responder The HttpResponder used to send responses in case of errors
   * @return List of JsonObjects from the request data
   * @throws java.io.IOException Thrown in case of Exceptions when reading the http request data
   */
  @Nullable
  private List<BatchEndpointArgs> decodeArrayArguments(HttpRequest request, HttpResponder responder)
    throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Cannot read request");
      return null;
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      List<BatchEndpointArgs> input = GSON.fromJson(reader, new TypeToken<List<BatchEndpointArgs>>() { }.getType());
      for (int i = 0; i < input.size(); ++i) {
        BatchEndpointArgs requestedObj;
        try {
          requestedObj = input.get(i);
        } catch (ClassCastException e) {
          responder.sendString(HttpResponseStatus.BAD_REQUEST, "All elements in array must be valid JSON Objects");
          return null;
        }
        // make sure the following args exist
        if (requestedObj.getAppId() == null || requestedObj.getProgramId() == null ||
          requestedObj.getProgramType() == null) {
          responder.sendJson(HttpResponseStatus.BAD_REQUEST,
                             "Must provide appId, programType, and programId as strings for each object");
          return null;
        }
        // invalid type
        try {
          if (ProgramType.valueOfPrettyName(requestedObj.getProgramType()) == null) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST,
                               "Invalid program type provided: " + requestedObj.getProgramType());
            return null;
          }
        } catch (IllegalArgumentException e) {
          responder.sendJson(HttpResponseStatus.BAD_REQUEST,
                             "Invalid program type provided: " + requestedObj.getProgramType());
          return null;
        }

      }
      return input;
    } catch (JsonSyntaxException e) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Invalid Json object provided");
      return null;
    } finally {
      reader.close();
    }
  }

  /**
   * Convenience class for representing the necessary components in the batch endpoint.
   */
  private class BatchEndpointArgs {
    private String appId = null;
    private String programType = null;
    private String programId = null;
    private String runnableId = null;
    private String error = null;
    private Integer statusCode = null;

    private BatchEndpointArgs(String appId, String programType, String programId, String runnableId, String error,
                              Integer statusCode) {
      this.appId = appId;
      this.programType = programType;
      this.programId = programId;
      this.runnableId = runnableId;
      this.error = error;
      this.statusCode = statusCode;
    }

    public BatchEndpointArgs(BatchEndpointArgs arg) {
      this(arg.appId, arg.programType, arg.programId, arg.runnableId, arg.error, arg.statusCode);
    }

    public String getRunnableId() {
      return runnableId;
    }

    public void setRunnableId(String runnableId) {
      this.runnableId = runnableId;
    }

    public void setError(String error) {
      this.error = error;
    }

    public void setStatusCode(Integer statusCode) {
      this.statusCode = statusCode;
    }

    public int getStatusCode() {
      return statusCode;
    }

    public String getError() {
      return error;
    }

    public String getProgramId() {
      return programId;
    }

    public String getProgramType() {
      return programType;
    }

    public String getAppId() {
      return appId;
    }

    public void setProgramType(String programType) {
      this.programType = programType;
    }
  }

  private class BatchEndpointInstances extends BatchEndpointArgs {
    private Integer requested = null;
    private Integer provisioned = null;

    public BatchEndpointInstances(BatchEndpointArgs arg) {
      super(arg);
    }

    public Integer getProvisioned() {
      return provisioned;
    }

    public void setProvisioned(Integer provisioned) {
      this.provisioned = provisioned;
    }

    public Integer getRequested() {
      return requested;
    }

    public void setRequested(Integer requested) {
      this.requested = requested;
    }
  }

  private class BatchEndpointStatus extends BatchEndpointArgs {
    private String status = null;

    public BatchEndpointStatus(BatchEndpointArgs arg) {
      super(arg);
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }
  }

  private List<BatchEndpointInstances> instancesFromBatchArgs(List<BatchEndpointArgs> args) {
    if (args == null) {
      return null;
    }
    List<BatchEndpointInstances> retVal = new ArrayList<BatchEndpointInstances>(args.size());
    for (BatchEndpointArgs arg: args) {
      retVal.add(new BatchEndpointInstances(arg));
    }
    return retVal;
  }

  private List<BatchEndpointStatus> statusFromBatchArgs(List<BatchEndpointArgs> args) {
    if (args == null) {
      return null;
    }
    List<BatchEndpointStatus> retVal = new ArrayList<BatchEndpointStatus>(args.size());
    for (BatchEndpointArgs arg: args) {
      retVal.add(new BatchEndpointStatus(arg));
    }
    return retVal;
  }

  /**
   * Adds the status code and error to the JsonObject. The JsonObject will have 2 new properties:
   * 'statusCode': code, 'error': error
   *
   * @param object The JsonObject to add the code and error to
   * @param code The status code to add
   * @param error The error message to add
   */
  private void addCodeError(BatchEndpointArgs object, int code, String error) {
    object.setStatusCode(code);
    object.setError(error);
  }

  /**
   * Returns the number of instances currently running for different runnables for different programs
   *
   * @param namespaceId
   * @param appId
   * @param programType
   * @param programId
   * @param runnableId
   * @return
   */
  private int getRunnableCount(String namespaceId, String appId, ProgramType programType,
                               String programId, String runnableId) {
    Id.Program id = Id.Program.from(namespaceId, appId, programId);
    ProgramLiveInfo info = runtimeService.getLiveInfo(id, programType);
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
    if (programType == ProgramType.SERVICE) {
      return getRequestedServiceInstances(id, runnableId);
    }

    // Not running on YARN default 1
    return 1;
  }

  private int getRequestedServiceInstances(Id.Program serviceId, String runnableId) {
    // Not running on YARN, get it from store
    // If the runnable name is the same as the service name, get the instances from service spec.
    // Otherwise get it from worker spec.
    // TODO: This is due to the improper REST API design that treats everything in service as Runnable
    if (runnableId.equals(serviceId.getId())) {
      return store.getServiceInstances(serviceId);
    } else {
      return store.getServiceWorkerInstances(serviceId, runnableId);
    }
  }

  private boolean isValidAction(String action) {
    return "start".equals(action) || "stop".equals(action) || "debug".equals(action);
  }

  private boolean isDebugAllowed(ProgramType programType) {
    return EnumSet.of(ProgramType.FLOW, ProgramType.SERVICE, ProgramType.PROCEDURE).contains(programType);
  }

  private boolean canHaveInstances(ProgramType programType) {
    return EnumSet.of(ProgramType.FLOW, ProgramType.SERVICE, ProgramType.PROCEDURE).contains(programType);
  }

  @Nullable
  private ProgramType getProgramType(String programType) {
    try {
      return ProgramType.valueOfCategoryName(programType);
    } catch (Exception e) {
      return null;
    }
  }

  // deletes the process metrics for a flow
  private void deleteProcessMetricsForFlow(String application, String flow) throws IOException {
    ServiceDiscovered discovered = discoveryServiceClient.discover(Constants.Service.METRICS);
    Discoverable discoverable = new RandomEndpointStrategy(discovered).pick(3L, TimeUnit.SECONDS);

    if (discoverable == null) {
      LOG.error("Fail to get any metrics endpoint for deleting metrics.");
      throw new IOException("Can't find Metrics endpoint");
    }

    LOG.debug("Deleting metrics for flow {}.{}", application, flow);
    String url = String.format("http://%s:%d%s/metrics/system/apps/%s/flows/%s?prefixEntity=process",
                               discoverable.getSocketAddress().getHostName(),
                               discoverable.getSocketAddress().getPort(),
                               Constants.Gateway.API_VERSION_2,
                               application, flow);

    long timeout = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

    SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
      .setUrl(url)
      .setRequestTimeoutInMs((int) timeout)
      .build();

    try {
      client.delete().get(timeout, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.error("exception making metrics delete call", e);
      Throwables.propagate(e);
    } finally {
      client.close();
    }
  }
}
