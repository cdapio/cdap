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
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.mapreduce.MRJobInfoFetcher;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.PropertiesResolver;
import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.MRJobInfo;
import co.cask.cdap.proto.NotRunningProgramLiveInfo;
import co.cask.cdap.proto.ProgramLiveInfo;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.ServiceInstances;
import co.cask.cdap.proto.codec.ScheduleSpecificationCodec;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.Location;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
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
   * App fabric output directory.
   */
  private final String appFabricDir;
  private final ProgramLifecycleService lifecycleService;
  private final QueueAdmin queueAdmin;
  private final PreferencesStore preferencesStore;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final PropertiesResolver propertiesResolver;
  private final AdapterService adapterService;
  private final MetricStore metricStore;
  private final MRJobInfoFetcher mrJobInfoFetcher;

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

  /**
   * Json serializer.
   */
  protected static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(ScheduleSpecification.class, new ScheduleSpecificationCodec())
    .create();

  /**
   * Store manages non-runtime lifecycle.
   */
  protected final Store store;

  /**
   * Runtime program service for running and managing programs.
   */
  protected final ProgramRuntimeService runtimeService;

  /**
   * Scheduler provides ability to schedule/un-schedule the jobs.
   */
  protected final Scheduler scheduler;

  @Inject
  public ProgramLifecycleHttpHandler(Store store, CConfiguration cConf, ProgramRuntimeService runtimeService,
                                     ProgramLifecycleService lifecycleService,
                                     QueueAdmin queueAdmin,
                                     Scheduler scheduler, PreferencesStore preferencesStore,
                                     NamespacedLocationFactory namespacedLocationFactory,
                                     MRJobInfoFetcher mrJobInfoFetcher,
                                     PropertiesResolver propertiesResolver, AdapterService adapterService,
                                     MetricStore metricStore) {
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.store = store;
    this.runtimeService = runtimeService;
    this.lifecycleService = lifecycleService;
    this.metricStore = metricStore;
    this.appFabricDir = cConf.get(Constants.AppFabric.OUTPUT_DIR);
    this.queueAdmin = queueAdmin;
    this.scheduler = scheduler;
    this.preferencesStore = preferencesStore;
    this.mrJobInfoFetcher = mrJobInfoFetcher;
    this.propertiesResolver = propertiesResolver;
    this.adapterService = adapterService;
  }

  /**
   * Relays job-level and task-level information about a particular MapReduce program run.
   */
  @GET
  @Path("/apps/{app-id}/mapreduce/{mapreduce-id}/runs/{run-id}/info")
  public void getMapReduceInfo(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("app-id") String appId,
                               @PathParam("mapreduce-id") String mapreduceId,
                               @PathParam("run-id") String runId) {
    try {
      Id.Program programId = Id.Program.from(namespaceId, appId, ProgramType.MAPREDUCE, mapreduceId);
      Id.Run run = new Id.Run(programId, runId);
      ApplicationSpecification appSpec = store.getApplication(programId.getApplication());
      if (appSpec == null) {
        throw new NotFoundException(programId.getApplication());
      }
      if (!appSpec.getMapReduce().containsKey(mapreduceId)) {
        throw new NotFoundException(programId);
      }
      RunRecord runRecord = store.getRun(programId, runId);
      if (runRecord == null) {
        throw new NotFoundException(run);
      }

      MRJobInfo mrJobInfo = mrJobInfoFetcher.getMRJobInfo(run);

      mrJobInfo.setState(runRecord.getStatus().name());
      // Multiple startTs / endTs by 1000, to be consistent with Task-level start/stop times returned by JobClient
      // in milliseconds. RunRecord returns seconds value.
      mrJobInfo.setStartTime(TimeUnit.SECONDS.toMillis(runRecord.getStartTs()));
      Long stopTs = runRecord.getStopTs();
      if (stopTs != null) {
        mrJobInfo.setStopTime(TimeUnit.SECONDS.toMillis(stopTs));
      }

      // JobClient (in DistributedMRJobInfoFetcher) can return NaN as some of the values, and GSON otherwise fails
      Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
      responder.sendJson(HttpResponseStatus.OK, mrJobInfo, mrJobInfo.getClass(), gson);
    } catch (NotFoundException e) {
      LOG.warn("NotFoundException while getting MapReduce Run info.", e);
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    } catch (Exception e) {
      LOG.error("Failed to get run history for runId: {}", runId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Returns status of a type specified by the type{flows,workflows,mapreduce,spark,services,schedules}.
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
      ProgramType programType = ProgramType.valueOfCategoryName(type);
      Id.Program program = Id.Program.from(namespaceId, appId, programType, id);
      StatusMap statusMap = getStatus(program);
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
      ProgramType programType = ProgramType.valueOfSchedulableType(scheduleSpec.getProgram().getProgramType());
      Id.Program programId = Id.Program.from(namespaceId, appId, programType, programName);
      JsonObject json = new JsonObject();
      json.addProperty("status", scheduler.scheduleState(programId, programId.getType().getSchedulableType(),
                                                         scheduleName).toString());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Stops the particular run of the Workflow or MapReduce program.
   */
  @POST
  @Path("/apps/{app-id}/{type}/{id}/runs/{run-id}/stop")
  public void performRunLevelStop(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId,
                                  @PathParam("type") String type,
                                  @PathParam("id") String id,
                                  @PathParam("run-id") String runId)
    throws BadRequestException, NotFoundException {
    try {
      ProgramType programType = ProgramType.valueOfCategoryName(type);
      Id.Program program = Id.Program.from(namespaceId, appId, programType, id);
      AppFabricServiceStatus status = stop(program, runId);
      responder.sendString(status.getCode(), status.getMessage());
     } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    }
  }

  @POST
  @Path("/apps/{app-id}/{type}/{id}/{action}")
  public void performAction(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId,
                            @PathParam("type") String type,
                            @PathParam("id") String id,
                            @PathParam("action") String action) throws NotFoundException {
    // If the app is an Application Template, then don't allow any action.
    // Operations are only allowed through Adapter Lifecycle management.
    if (adapterService.getApplicationTemplateInfo(appId) != null) {
      responder.sendString(HttpResponseStatus.FORBIDDEN,
                           "Operations on Application Templates are allowed only through Adapters.");
      return;
    }

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
    Id.Program programId = Id.Program.from(namespaceId, appId, programType, id);
    startStopProgram(request, responder, programId, action);
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
      ProgramType programType = ProgramType.valueOfSchedulableType(scheduleSpec.getProgram().getProgramType());
      Id.Program programId = Id.Program.from(namespaceId, appId, programType, programName);
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
    } catch (NotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    } catch (Throwable e) {
      LOG.error("Got exception when performing action '{}' on schedule '{}' for app '{}'",
                action, scheduleName, appId, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns program runs based on options it returns either currently running or completed or failed.
   * Default it returns all.
   */
  @GET
  @Path("/apps/{app-id}/{program-type}/{program-id}/runs")
  public void programHistory(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-id") String appId,
                             @PathParam("program-type") String programType,
                             @PathParam("program-id") String programId,
                             @QueryParam("status") String status,
                             @QueryParam("start") String startTs,
                             @QueryParam("end") String endTs,
                             @QueryParam("limit") @DefaultValue("100") final int resultLimit) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    if (type == null || type == ProgramType.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }
    long start = (startTs == null || startTs.isEmpty()) ? 0 : Long.parseLong(startTs);
    long end = (endTs == null || endTs.isEmpty()) ? Long.MAX_VALUE : Long.parseLong(endTs);
    getRuns(responder, Id.Program.from(namespaceId, appId, type, programId), status, start, end, resultLimit);
  }

  /**
   * Returns run record for a particular run of a program.
   */
  @GET
  @Path("/apps/{app-id}/{program-type}/{program-id}/runs/{run-id}")
  public void programRunRecord(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-id") String appId,
                             @PathParam("program-type") String programType,
                             @PathParam("program-id") String programId,
                             @PathParam("run-id") String runid) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    if (type == null || type == ProgramType.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    try {
      RunRecord runRecord = store.getRun(Id.Program.from(namespaceId, appId, type, programId), runid);
      if (runRecord != null) {
        responder.sendJson(HttpResponseStatus.OK, runRecord);
        return;
      }
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Get program runtime args.
   */
  @GET
  @Path("/apps/{app-id}/{program-type}/{program-id}/runtimeargs")
  public void getProgramRuntimeArgs(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-id") String appId,
                                    @PathParam("program-type") String programType,
                                    @PathParam("program-id") String programId) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    if (type == null || type == ProgramType.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    Id.Program id = Id.Program.from(namespaceId, appId, type, programId);

    try {
      if (!store.programExists(id)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Program not found");
        return;
      }
      Map<String, String> runtimeArgs = preferencesStore.getProperties(id.getNamespaceId(), appId,
                                                                       programType, programId);
      responder.sendJson(HttpResponseStatus.OK, runtimeArgs);
    } catch (Throwable e) {
      LOG.error("Error getting runtime args {}", e.getMessage(), e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Save program runtime args.
   */
  @PUT
  @Path("/apps/{app-id}/{program-type}/{program-id}/runtimeargs")
  public void saveProgramRuntimeArgs(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-id") String appId,
                                     @PathParam("program-type") String programType,
                                     @PathParam("program-id") String programId) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    if (type == null || type == ProgramType.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    Id.Program id = Id.Program.from(namespaceId, appId, type, programId);

    try {
      if (!store.programExists(id)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Program not found");
        return;
      }
      Map<String, String> args = decodeArguments(request);
      preferencesStore.setProperties(namespaceId, appId, programType, programId, args);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Throwable e) {
      LOG.error("Error getting runtime args {}", e.getMessage(), e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/apps/{app-id}/{program-type}/{program-id}")
  public void programSpecification(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId, @PathParam("app-id") String appId,
                                   @PathParam("program-type") String programType,
                                   @PathParam("program-id") String programId) {

    ProgramType type = getProgramType(programType);
    if (type == null) {
      responder.sendString(HttpResponseStatus.METHOD_NOT_ALLOWED,
                           String.format("Program type '%s' not supported", programType));
      return;
    }

    try {
      Id.Program id = Id.Program.from(namespaceId, appId, type, programId);
      ProgramSpecification specification = getProgramSpecification(id);
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
   * Returns the status for all programs that are passed into the data. The data is an array of JSON objects
   * where each object must contain the following three elements: appId, programType, and programId
   * (flow name, service name, etc.).
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1"},
   * {"appId": "App1", "programType": "Mapreduce", "programId": "MapReduce2"},
   * {"appId": "App2", "programType": "Flow", "programId": "Flow1"}]
   * </code></pre>
   * </p><p>
   * The response will be an array of JsonObjects each of which will contain the three input parameters
   * as well as 2 fields, "status" which maps to the status of the program and "statusCode" which maps to the
   * status code for the data in that JsonObjects.
   * </p><p>
   * If an error occurs in the input (for the example above, App2 does not exist), then all JsonObjects for which the
   * parameters have a valid status will have the status field but all JsonObjects for which the parameters do not have
   * a valid status will have an error message and statusCode.
   * </p><p>
   * For example, if there is no App2 in the data above, then the response would be 200 OK with following possible data:
   * </p>
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1", "statusCode": 200, "status": "RUNNING"},
   * {"appId": "App1", "programType": "Mapreduce", "programId": "Mapreduce2", "statusCode": 200, "status": "STOPPED"},
   * {"appId":"App2", "programType":"Flow", "programId":"Flow1", "statusCode":404, "error": "App: App2 not found"}]
   * </code></pre>
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
        ProgramType programType = ProgramType.valueOfPrettyName(requestedObj.getProgramType());
        Id.Program progId = Id.Program.from(namespaceId, requestedObj.getAppId(), programType,
                                            requestedObj.getProgramId());
        // get th statuses
        StatusMap statusMap = getStatus(progId);
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
   * (flow name, service name). Retrieving instances only applies to flows, and user
   * services. For flows, another parameter, "runnableId", must be provided. This corresponds to the
   * flowlet/runnable for which to retrieve the instances.
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1", "runnableId": "Runnable1"},
   *  {"appId": "App1", "programType": "Mapreduce", "programId": "Mapreduce2"},
   *  {"appId": "App2", "programType": "Flow", "programId": "Flow1", "runnableId": "Flowlet1"}]
   * </code></pre>
   * </p><p>
   * The response will be an array of JsonObjects each of which will contain the three input parameters
   * as well as 3 fields:
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
   *  {"appId": "App1", "programType": "Mapreduce", "programId": "Mapreduce2", "statusCode": 200, "provisioned": 1,
   *   "requested": 3},
   *  {"appId": "App2", "programType": "Flow", "programId": "Flow1", "runnableId": "Flowlet1", "statusCode": 404,
   *   "error": "Program": Flowlet1 not found"}]
   * </code></pre>
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
        Id.Application appId = Id.Application.from(namespaceId, requestedObj.getAppId());
        ApplicationSpecification spec = store.getApplication(appId);
        if (spec == null) {
          addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(), "App: " + appId + " not found");
          continue;
        }

        ProgramType programType = ProgramType.valueOfPrettyName(requestedObj.getProgramType());
        // cant get instances for things that are not flows or services
        if (!canHaveInstances(programType)) {
          addCodeError(requestedObj, HttpResponseStatus.BAD_REQUEST.getCode(),
                       "Program type: " + programType + " is not a valid program type to get instances");
          continue;
        }

        Id.Program programId = Id.Program.from(appId, programType, requestedObj.getProgramId());
        populateProgramInstances(requestedObj, spec, programId);
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
    programList(responder, namespaceId, ProgramType.FLOW, store);
  }

  /**
   * Returns a list of map/reduces associated with a namespace.
   */
  @GET
  @Path("/mapreduce")
  public void getAllMapReduce(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId) {
    programList(responder, namespaceId, ProgramType.MAPREDUCE, store);
  }

  /**
   * Returns a list of spark jobs associated with a namespace.
   */
  @GET
  @Path("/spark")
  public void getAllSpark(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId) {
    programList(responder, namespaceId, ProgramType.SPARK, store);
  }

  /**
   * Returns a list of workflows associated with a namespace.
   */
  @GET
  @Path("/workflows")
  public void getAllWorkflows(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId) {
    programList(responder, namespaceId, ProgramType.WORKFLOW, store);
  }

  /**
   * Returns a list of services associated with a namespace.
   */
  @GET
  @Path("/services")
  public void getAllServices(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId) {
    programList(responder, namespaceId, ProgramType.SERVICE, store);
  }

  @GET
  @Path("/workers")
  public void getAllWorkers(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) {
    programList(responder, namespaceId, ProgramType.WORKER, store);
  }

  /**
   * Returns number of instances of a worker.
   */
  @GET
  @Path("/apps/{app-id}/workers/{worker-id}/instances")
  public void getWorkerInstances(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("app-id") String appId,
                                 @PathParam("worker-id") String workerId) {
    try {
      int count = store.getWorkerInstances(Id.Program.from(namespaceId, appId, ProgramType.WORKER, workerId));
      responder.sendJson(HttpResponseStatus.OK, new Instances(count));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      if (respondIfElementNotFound(e, responder)) {
        return;
      }
      LOG.error("Got exception: ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Sets the number of instances of a worker.
   */
  @PUT
  @Path("/apps/{app-id}/workers/{worker-id}/instances")
  public void setWorkerInstances(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("app-id") String appId,
                                 @PathParam("worker-id") String workerId) {
    int instances;
    try {
      try {
        instances = getInstances(request);
      } catch (IllegalArgumentException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid instance value in request");
        return;
      } catch (JsonSyntaxException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in request");
        return;
      }
      if (instances < 1) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Instance count should be greater than 0");
        return;
      }
    } catch (Throwable th) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid instance count.");
      return;
    }

    try {
      Id.Program programId = Id.Program.from(namespaceId, appId, ProgramType.WORKER, workerId);
      int oldInstances = store.getWorkerInstances(programId);
      if (oldInstances != instances) {
        store.setWorkerInstances(programId, instances);
        ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(namespaceId, appId, workerId,
                                                                        ProgramType.WORKER, runtimeService);
        if (runtimeInfo != null) {
          runtimeInfo.getController().command(ProgramOptionConstants.INSTANCES,
                                              ImmutableMap.of(programId.getId(), String.valueOf(instances))).get();
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
      int count = store.getFlowletInstances(Id.Program.from(namespaceId, appId, ProgramType.FLOW, flowId), flowletId);
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
  public synchronized void setFlowletInstances(HttpRequest request, HttpResponder responder,
                                               @PathParam("namespace-id") String namespaceId,
                                               @PathParam("app-id") String appId, @PathParam("flow-id") String flowId,
                                               @PathParam("flowlet-id") String flowletId) {
    int instances;
    try {
      try {
        instances = getInstances(request);
      } catch (IllegalArgumentException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid instance value in request");
        return;
      } catch (JsonSyntaxException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in request");
        return;
      }
      if (instances < 1) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Instance count should be greater than 0");
        return;
      }
    } catch (Throwable th) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid instance count.");
      return;
    }

    try {
      Id.Program programId = Id.Program.from(namespaceId, appId, ProgramType.FLOW, flowId);
      int oldInstances = store.getFlowletInstances(programId, flowletId);
      if (oldInstances != instances) {
        FlowSpecification flowSpec = store.setFlowletInstances(programId, flowletId, instances);
        ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(namespaceId, appId, flowId, ProgramType.FLOW,
                                                                        runtimeService);
        if (runtimeInfo != null) {
          runtimeInfo.getController()
            .command(ProgramOptionConstants.INSTANCES,
                     ImmutableMap.of("flowlet", flowletId,
                                     "newInstances", String.valueOf(instances),
                                     "oldFlowSpec", GSON.toJson(flowSpec, FlowSpecification.class))).get();
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

  @GET
  @Path("/apps/{app-id}/{program-category}/{program-id}/live-info")
  @SuppressWarnings("unused")
  public void liveInfo(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                       @PathParam("app-id") String appId, @PathParam("program-category") String programCategory,
                       @PathParam("program-id") String programId) {
    ProgramType type = getProgramType(programCategory);
    if (type == null) {
      responder.sendString(HttpResponseStatus.METHOD_NOT_ALLOWED,
                           String.format("Live-info not supported for program type '%s'", programCategory));
      return;
    }
    Id.Program program =
      Id.Program.from(namespaceId, appId, ProgramType.valueOfCategoryName(programCategory), programId);
    getLiveInfo(responder, program, runtimeService);
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
    Id.Program programId = Id.Program.from(namespaceId, appId, ProgramType.FLOW, flowId);
    try {
      ProgramStatus status = getProgramStatus(programId);
      if (status.getStatus().equals(HttpResponseStatus.NOT_FOUND.toString())) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else if (status.getStatus().equals("RUNNING")) {
        responder.sendString(HttpResponseStatus.FORBIDDEN, "Flow is running, please stop it first.");
      } else {
        queueAdmin.dropAllForFlow(Id.Flow.from(programId.getApplication(), programId.getId()));
        FlowUtils.deleteFlowPendingMetrics(metricStore, namespaceId, appId, flowId);
        responder.sendStatus(HttpResponseStatus.OK);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Return the number of instances of a service.
   */
  @GET
  @Path("/apps/{app-id}/services/{service-id}/instances")
  public void getServiceInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId,
                                  @PathParam("service-id") String serviceId)  {
    try {
      Id.Program programId = Id.Program.from(namespaceId, appId, ProgramType.SERVICE, serviceId);
      if (!store.programExists(programId)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Service not found");
        return;
      }

      ServiceSpecification specification = (ServiceSpecification) getProgramSpecification(programId);
      if (specification == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      int instances = specification.getInstances();
      responder.sendJson(HttpResponseStatus.OK,
                         new ServiceInstances(instances, getInstanceCount(programId, serviceId)));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Set instances of a service.
   */
  @PUT
  @Path("/apps/{app-id}/services/{service-id}/instances")
  public void setServiceInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId,
                                  @PathParam("service-id") String serviceId) {
    try {
      Id.Program programId = Id.Program.from(namespaceId, appId, ProgramType.SERVICE, serviceId);
      if (!store.programExists(programId)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Service not found");
        return;
      }

      int instances;
      try {
        instances = getInstances(request);
      } catch (IllegalArgumentException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid instance value in request");
        return;
      } catch (JsonSyntaxException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in request");
        return;
      }
      if (instances < 1) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Instance count should be greater than 0");
        return;
      }

      int oldInstances = store.getServiceInstances(programId);
      if (oldInstances != instances) {
        store.setServiceInstances(programId, instances);
        ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(namespaceId, appId, serviceId,
                                                                        ProgramType.SERVICE, runtimeService);
        if (runtimeInfo != null) {
          runtimeInfo.getController().command(ProgramOptionConstants.INSTANCES,
                                              ImmutableMap.of(serviceId, String.valueOf(instances))).get();
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
    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    try {
      List<ProgramRecord> flows = listPrograms(namespace, ProgramType.FLOW, store);
      for (ProgramRecord flow : flows) {
        String appId = flow.getApp();
        String flowId = flow.getName();
        Id.Program programId = Id.Program.from(namespace, appId, ProgramType.FLOW, flowId);
        ProgramStatus status = getProgramStatus(programId);
        if (!"STOPPED".equals(status.getStatus())) {
          responder.sendString(HttpResponseStatus.FORBIDDEN,
                               String.format("Flow '%s' from application '%s' in namespace '%s' is running, " +
                                               "please stop it first.", flowId, appId, namespaceId));
          return;
        }
      }
      queueAdmin.dropAllInNamespace(namespace);
      // delete process metrics that are used to calculate the queue size (system.queue.pending metric)
      FlowUtils.deleteFlowPendingMetrics(metricStore, namespaceId, null, null);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Exception e) {
      LOG.error("Error while deleting queues in namespace " + namespace, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Populates requested and provisioned instances for a program type.
   * The program type passed here should be one that can have instances (flows, services, ...)
   * Requires caller to do this validation.
   */
  private void populateProgramInstances(BatchEndpointInstances requestedObj, ApplicationSpecification spec,
                                        Id.Program programId) {
    int requested;
    String programName = programId.getId();
    String runnableId = programName;
    ProgramType programType = programId.getType();
    if (programType == ProgramType.WORKER) {
      if (!spec.getWorkers().containsKey(programName)) {
        addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(),
                     "Worker: " + programName + " not found");
        return;
      }
      requested = spec.getWorkers().get(programName).getInstances();

    } else if (programType == ProgramType.SERVICE) {
      if (!spec.getServices().containsKey(programName)) {
        addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(),
                     "Service: " + programName + " not found");
        return;
      }
      requested = spec.getServices().get(programName).getInstances();

    } else if (programType == ProgramType.FLOW) {
      // flows must have runnable id
      if (requestedObj.getRunnableId() == null) {
        addCodeError(requestedObj, HttpResponseStatus.BAD_REQUEST.getCode(),
                     "Must provide the flowlet id as the runnableId for flows");
        return;
      }
      runnableId = requestedObj.getRunnableId();
      FlowSpecification flowSpec = spec.getFlows().get(programName);
      if (flowSpec == null) {
        addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(), "Flow: " + programName + " not found");
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
      addCodeError(requestedObj, HttpResponseStatus.BAD_REQUEST.getCode(),
                   "Instances not supported for program type + " + programType);
      return;
    }
    int provisioned = getInstanceCount(programId, runnableId);
    // use the pretty name of program types to be consistent
    requestedObj.setProgramType(programType.getPrettyName());
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
   * @throws RuntimeException if failed to determine the program status
   */
  private StatusMap getStatus(final Id.Program id) {
    // invalid type does not exist
    if (id.getType() == null) {
      return new StatusMap(null, "Invalid program type provided", HttpResponseStatus.BAD_REQUEST.getCode());
    }

    try {
      // check that app exists
      ApplicationSpecification appSpec = store.getApplication(id.getApplication());
      if (appSpec == null) {
        return new StatusMap(null, "App: " + id.getApplicationId() + " not found",
                             HttpResponseStatus.NOT_FOUND.getCode());
      }

      return getProgramStatusMap(id, new StatusMap());
    } catch (Exception e) {
      LOG.error("Exception raised when getting program status for {}", id, e);
      return new StatusMap(null, "Failed to get program status", HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode());
    }
  }

  private StatusMap getProgramStatusMap(Id.Program id, StatusMap statusMap) {
    // getProgramStatus returns program status or http response status NOT_FOUND
    String programStatus = getProgramStatus(id).getStatus();
    if (programStatus.equals(HttpResponseStatus.NOT_FOUND.toString())) {
      statusMap.setStatusCode(HttpResponseStatus.NOT_FOUND.getCode());
      statusMap.setError("Program not found");
    } else {
      statusMap.setStatus(programStatus);
      statusMap.setStatusCode(HttpResponseStatus.OK.getCode());
    }
    return statusMap;
  }

  protected ProgramStatus getProgramStatus(Id.Program id) {
    return getProgramStatus(id, null);
  }

  /**
   * 'protected' for the workflow handler to use
   */
  protected ProgramStatus getProgramStatus(Id.Program id, @Nullable String runId) {
    try {
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(id, runId);

      if (runtimeInfo == null) {
        if (id.getType() != ProgramType.WEBAPP) {
          //Runtime info not found. Check to see if the program exists.
          ProgramSpecification spec = getProgramSpecification(id);
          if (spec == null) {
            // program doesn't exist
            return new ProgramStatus(id.getApplicationId(), id.getId(), HttpResponseStatus.NOT_FOUND.toString());
          }
          if (id.getType() == ProgramType.MAPREDUCE && !store.getRuns(id, ProgramRunStatus.RUNNING, 0,
                                                              Long.MAX_VALUE, 1).isEmpty()) {
            // MapReduce program exists and running as a part of Workflow
            return new ProgramStatus(id.getApplicationId(), id.getId(), "RUNNING");
          }
          return new ProgramStatus(id.getApplicationId(), id.getId(), "STOPPED");
        }
        // TODO: Fetching webapp status is a hack. This will be fixed when webapp spec is added.
        Location webappLoc = null;
        try {
          webappLoc = Programs.programLocation(namespacedLocationFactory, appFabricDir, id);
        } catch (FileNotFoundException e) {
          // No location found for webapp, no need to log this exception
        }

        if (webappLoc != null && webappLoc.exists()) {
          // webapp exists and not running. so return stopped.
          return new ProgramStatus(id.getApplicationId(), id.getId(), "STOPPED");
        }

        // webapp doesn't exist
        return new ProgramStatus(id.getApplicationId(), id.getId(), HttpResponseStatus.NOT_FOUND.toString());
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
  @Nullable
  protected ProgramRuntimeService.RuntimeInfo findRuntimeInfo(Id.Program identifier, @Nullable String runId) {
    Map<RunId, ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(identifier.getType());

    if (runId != null) {
      return runtimeInfos.get(RunIds.fromString(runId));
    }

    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos.values()) {
      if (identifier.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }

  @Nullable
  private ProgramSpecification getProgramSpecification(Id.Program id) throws Exception {
    ApplicationSpecification appSpec;
    try {
      appSpec = store.getApplication(id.getApplication());
      if (appSpec == null) {
        return null;
      }

      String programId = id.getId();
      ProgramType type = id.getType();
      ProgramSpecification programSpec;
      if (type == ProgramType.FLOW && appSpec.getFlows().containsKey(programId)) {
        programSpec = appSpec.getFlows().get(id.getId());
      } else if (type == ProgramType.MAPREDUCE && appSpec.getMapReduce().containsKey(programId)) {
        programSpec = appSpec.getMapReduce().get(id.getId());
      } else if (type == ProgramType.SPARK && appSpec.getSpark().containsKey(programId)) {
        programSpec = appSpec.getSpark().get(id.getId());
      } else if (type == ProgramType.WORKFLOW && appSpec.getWorkflows().containsKey(programId)) {
        programSpec = appSpec.getWorkflows().get(id.getId());
      } else if (type == ProgramType.SERVICE && appSpec.getServices().containsKey(programId)) {
        programSpec = appSpec.getServices().get(id.getId());
      } else if (type == ProgramType.WORKER && appSpec.getWorkers().containsKey(programId)) {
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

  private synchronized void startStopProgram(HttpRequest request, HttpResponder responder, Id.Program programId,
                                             String action) throws NotFoundException {
    if (programId.getType() == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      LOG.trace("{} call from AppFabricHttpHandler for program: {}",
                action, programId);
      try {
        AppFabricServiceStatus status;
        if ("start".equals(action)) {
          status = start(programId, decodeArguments(request), false);
        } else if ("debug".equals(action)) {
          status = start(programId, decodeArguments(request), true);
        } else if ("stop".equals(action)) {
          status = stop(programId);
        } else {
          throw new IllegalArgumentException("action must be start, stop, or debug, but is: " + action);
        }
        if (status == AppFabricServiceStatus.INTERNAL_ERROR) {
          responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
          return;
        }

        responder.sendString(status.getCode(), status.getMessage());
      } catch (SecurityException e) {
        responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
      }
    }
  }

  /**
   * Starts a Program.
   */
  private AppFabricServiceStatus start(final Id.Program id, Map<String, String> overrides, boolean debug) {
    try {
      Map<String, String> sysArgs = propertiesResolver.getSystemProperties(id);
      Map<String, String> userArgs = propertiesResolver.getUserProperties(id);
      if (overrides != null) {
        userArgs.putAll(overrides);
      }

      if (isRunning(id) && !isConcurrentRunsAllowed(id.getType())) {
        return AppFabricServiceStatus.PROGRAM_ALREADY_RUNNING;
      }

      ProgramRuntimeService.RuntimeInfo runtimeInfo = lifecycleService.start(id, sysArgs, userArgs, debug);
      return (runtimeInfo != null) ? AppFabricServiceStatus.OK : AppFabricServiceStatus.INTERNAL_ERROR;
    } catch (ProgramNotFoundException e) {
      return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
    } catch (Throwable throwable) {
      LOG.error(throwable.getMessage(), throwable);
      return AppFabricServiceStatus.INTERNAL_ERROR;
    }
  }

  private boolean isRunning(Id.Program id) {
    String programStatus = getStatus(id).getStatus();
    return programStatus != null && !"STOPPED".equals(programStatus);
  }

  private boolean isConcurrentRunsAllowed(ProgramType type) {
    // Concurrent runs are only allowed for the Workflow and MapReduce
    return EnumSet.of(ProgramType.WORKFLOW, ProgramType.MAPREDUCE).contains(type);
  }

  private AppFabricServiceStatus stop(Id.Program id) throws NotFoundException {
    return stop(id, null);
  }

  /**
   * Stops a Program.
   */
  private AppFabricServiceStatus stop(Id.Program identifier, @Nullable String runId) throws NotFoundException {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(identifier, runId);

    if (runtimeInfo == null) {
      throw new NotFoundException(new Id.Run(identifier, runId));
    }

    try {
      ProgramController controller = runtimeInfo.getController();
      controller.stop().get();
      return AppFabricServiceStatus.OK;
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      return AppFabricServiceStatus.INTERNAL_ERROR;
    }
  }

  private void getRuns(HttpResponder responder, Id.Program programId, String status,
                       long start, long end, int limit) {
    try {
      try {
        ProgramRunStatus runStatus = (status == null) ? ProgramRunStatus.ALL :
          ProgramRunStatus.valueOf(status.toUpperCase());
        responder.sendJson(HttpResponseStatus.OK, store.getRuns(programId, runStatus, start, end, limit));
      } catch (IllegalArgumentException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
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
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8)) {
      List<BatchEndpointArgs> input = GSON.fromJson(reader, new TypeToken<List<BatchEndpointArgs>>() { }.getType());
      for (BatchEndpointArgs requestedObj : input) {
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
    }
  }

  /**
   * Convenience class for representing the necessary components in the batch endpoint.
   */
  private class BatchEndpointArgs {
    private String appId;
    private String programType;
    private String programId;
    private String runnableId;
    private String error;
    private Integer statusCode;

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

    @SuppressWarnings("unused") // not used in this class but we need it in the JSON object
    private Integer provisioned = null;
    private Integer requested = null;

    public BatchEndpointInstances(BatchEndpointArgs arg) {
      super(arg);
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
    List<BatchEndpointInstances> retVal = new ArrayList<>(args.size());
    for (BatchEndpointArgs arg: args) {
      retVal.add(new BatchEndpointInstances(arg));
    }
    return retVal;
  }

  private List<BatchEndpointStatus> statusFromBatchArgs(List<BatchEndpointArgs> args) {
    if (args == null) {
      return null;
    }
    List<BatchEndpointStatus> retVal = new ArrayList<>(args.size());
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
   */
  private int getInstanceCount(Id.Program programId, String runnableId) {
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

  private int getRequestedServiceInstances(Id.Program serviceId) {
    // Not running on YARN, get it from store
    return store.getServiceInstances(serviceId);
  }

  private boolean isValidAction(String action) {
    return "start".equals(action) || "stop".equals(action) || "debug".equals(action);
  }

  private boolean isDebugAllowed(ProgramType programType) {
    return EnumSet.of(ProgramType.FLOW, ProgramType.SERVICE, ProgramType.WORKER).contains(programType);
  }

  private boolean canHaveInstances(ProgramType programType) {
    return EnumSet.of(ProgramType.FLOW, ProgramType.SERVICE, ProgramType.WORKER).contains(programType);
  }
}
