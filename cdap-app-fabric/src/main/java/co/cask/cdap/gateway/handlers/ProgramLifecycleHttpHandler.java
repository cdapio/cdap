/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.app.mapreduce.MRJobInfoFetcher;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.MethodNotAllowedException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.NotImplementedException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.BatchProgram;
import co.cask.cdap.proto.BatchProgramResult;
import co.cask.cdap.proto.BatchProgramStart;
import co.cask.cdap.proto.BatchProgramStatus;
import co.cask.cdap.proto.BatchRunnable;
import co.cask.cdap.proto.BatchRunnableInstances;
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
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.FlowId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
  private static final Type BATCH_PROGRAMS_TYPE = new TypeToken<List<BatchProgram>>() { }.getType();
  private static final Type BATCH_RUNNABLES_TYPE = new TypeToken<List<BatchRunnable>>() { }.getType();
  private static final Type BATCH_STARTS_TYPE = new TypeToken<List<BatchProgramStart>>() { }.getType();

  private static final String SCHEDULES = "schedules";
  /**
   * Json serializer/deserializer.
   */
  private static final Gson GSON = ApplicationSpecificationAdapter
    .addTypeAdapters(new GsonBuilder())
    .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
    .create();

  private static final Function<RunRecordMeta, RunRecord> CONVERT_TO_RUN_RECORD =
    new Function<RunRecordMeta, RunRecord>() {
      @Override
      public RunRecord apply(RunRecordMeta input) {
        return new RunRecord(input);
      }
    };

  private final ProgramLifecycleService lifecycleService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final QueueAdmin queueAdmin;
  private final PreferencesStore preferencesStore;
  private final MetricStore metricStore;
  private final MRJobInfoFetcher mrJobInfoFetcher;

  /**
   * Store manages non-runtime lifecycle.
   */
  protected final Store store;

  /**
   * Runtime program service for running and managing programs.
   */
  protected final ProgramRuntimeService runtimeService;

  @Inject
  ProgramLifecycleHttpHandler(Store store, ProgramRuntimeService runtimeService,
                              DiscoveryServiceClient discoveryServiceClient,
                              ProgramLifecycleService lifecycleService,
                              QueueAdmin queueAdmin,
                              PreferencesStore preferencesStore,
                              MRJobInfoFetcher mrJobInfoFetcher,
                              MetricStore metricStore) {
    this.store = store;
    this.runtimeService = runtimeService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.lifecycleService = lifecycleService;
    this.metricStore = metricStore;
    this.queueAdmin = queueAdmin;
    this.preferencesStore = preferencesStore;
    this.mrJobInfoFetcher = mrJobInfoFetcher;
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
                               @PathParam("run-id") String runId) throws IOException, NotFoundException {
    ProgramId programId = new ProgramId(namespaceId, appId, ProgramType.MAPREDUCE, mapreduceId);
    ProgramRunId run = programId.run(runId);
    ApplicationSpecification appSpec = store.getApplication(programId.getParent());
    if (appSpec == null) {
      throw new NotFoundException(programId.getApplication());
    }
    if (!appSpec.getMapReduce().containsKey(mapreduceId)) {
      throw new NotFoundException(programId);
    }
    RunRecordMeta runRecordMeta = store.getRun(programId, runId);
    if (runRecordMeta == null) {
      throw new NotFoundException(run);
    }

    MRJobInfo mrJobInfo = mrJobInfoFetcher.getMRJobInfo(run.toId());

    mrJobInfo.setState(runRecordMeta.getStatus().name());
    // Multiple startTs / endTs by 1000, to be consistent with Task-level start/stop times returned by JobClient
    // in milliseconds. RunRecord returns seconds value.
    mrJobInfo.setStartTime(TimeUnit.SECONDS.toMillis(runRecordMeta.getStartTs()));
    Long stopTs = runRecordMeta.getStopTs();
    if (stopTs != null) {
      mrJobInfo.setStopTime(TimeUnit.SECONDS.toMillis(stopTs));
    }

    // JobClient (in DistributedMRJobInfoFetcher) can return NaN as some of the values, and GSON otherwise fails
    Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
    responder.sendJson(HttpResponseStatus.OK, mrJobInfo, mrJobInfo.getClass(), gson);
  }

  /**
   * Returns status of a type specified by the type{flows,workflows,mapreduce,spark,services,schedules}.
   */
  @GET
  @Path("/apps/{app-id}/{program-type}/{program-id}/status")
  public void getStatus(HttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") String namespaceId,
                        @PathParam("app-id") String appId,
                        @PathParam("program-type") String type,
                        @PathParam("program-id") String programId) throws Exception {
    ApplicationId applicationId = new ApplicationId(namespaceId, appId);
    if (SCHEDULES.equals(type)) {
      JsonObject json = new JsonObject();
      json.addProperty("status", lifecycleService.getScheduleStatus(namespaceId, appId, programId).toString());
      responder.sendJson(HttpResponseStatus.OK, json);
      return;
    }
    ProgramType programType;
    try {
      programType = ProgramType.valueOfCategoryName(type);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    }
    ProgramId program = applicationId.program(programType, programId);
    ProgramStatus programStatus = lifecycleService.getProgramStatus(program);

    Map<String, String> status = ImmutableMap.of("status", programStatus.name());
    responder.sendJson(HttpResponseStatus.OK, status);
  }

  /**
   * Returns status of a type specified by the type{flows,workflows,mapreduce,spark,services,schedules}.
   */
  @GET
  @Path("/apps/{app-id}/versions/{version-id}/{program-type}/{program-id}/status")
  public void getStatus(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("app-id") String appId,
                               @PathParam("version-id") String versionId,
                               @PathParam("program-type") String type,
                               @PathParam("program-id") String programId) throws Exception {
    ApplicationId applicationId = new ApplicationId(namespaceId, appId, versionId);
    if (SCHEDULES.equals(type)) {
      JsonObject json = new JsonObject();
      json.addProperty("status", lifecycleService.getScheduleStatus(namespaceId, appId, programId).toString());
      responder.sendJson(HttpResponseStatus.OK, json);
      return;
    }

    ProgramType programType;
    try {
      programType = ProgramType.valueOfCategoryName(type);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    }
    ProgramId program = applicationId.program(programType, programId);
    ProgramStatus programStatus = lifecycleService.getProgramStatus(program);

    Map<String, String> status = ImmutableMap.of("status", programStatus.name());
    responder.sendJson(HttpResponseStatus.OK, status);
  }

  /**
   * Stops the particular run of the Workflow or MapReduce program.
   */
  @POST
  @Path("/apps/{app-id}/{program-type}/{program-id}/runs/{run-id}/stop")
  public void performRunLevelStop(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("app-id") String appId,
                                  @PathParam("program-type") String type,
                                  @PathParam("program-id") String programId,
                                  @PathParam("run-id") String runId) throws Exception {
    ProgramType programType;
    try {
      programType = ProgramType.valueOfCategoryName(type);
     } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    }
    ProgramId program = new ProgramId(namespaceId, appId, programType, programId);
    lifecycleService.stop(program, runId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/apps/{app-id}/{program-type}/{program-id}/{action}")
  public void performAction(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId,
                            @PathParam("program-type") String type,
                            @PathParam("program-id") String programId,
                            @PathParam("action") String action) throws Exception {
    doPerformAction(request, responder, namespaceId, appId, ApplicationId.DEFAULT_VERSION, type, programId, action);
  }

  @POST
  @Path("/apps/{app-id}/versions/{app-version}/{program-type}/{program-id}/{action}")
  public void performAction(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId,
                            @PathParam("app-version") String appVersion,
                            @PathParam("program-type") String type,
                            @PathParam("program-id") String programId,
                            @PathParam("action") String action) throws Exception {
    doPerformAction(request, responder, namespaceId, appId, appVersion, type, programId, action);
  }

  private void doPerformAction(HttpRequest request, HttpResponder responder, String namespaceId, String appId,
                               String appVersion, String type, String programId, String action) throws Exception {
    ApplicationId applicationId = new ApplicationId(namespaceId, appId, appVersion);
    if ("schedules".equals(type)) {
      lifecycleService.suspendResumeSchedule(namespaceId, appId, programId, action);
      responder.sendJson(HttpResponseStatus.OK, "OK");
      return;
    }

    ProgramType programType;
    try {
      programType = ProgramType.valueOfCategoryName(type);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Unknown program type '%s'", type), e);
    }

    ProgramId program = applicationId.program(programType, programId);
    Map<String, String> args = decodeArguments(request);
    // we have already validated that the action is valid
    switch (action.toLowerCase()) {
      case "start":
        lifecycleService.start(program, args, false);
        break;
      case "debug":
        if (!isDebugAllowed(programType)) {
          throw new NotImplementedException(String.format("debug action is not implemented for program type %s",
                                                          programType));
        }
        lifecycleService.start(program, args, true);
        break;
      case "stop":
        lifecycleService.stop(program);
        break;
      default:
        throw new NotFoundException(String.format("%s action was not found", action));
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Returns program runs based on options it returns either currently running or completed or failed.
   * Default it returns all.
   */
  @GET
  @Path("/apps/{app-name}/{program-type}/{program-name}/runs")
  public void programHistory(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-name") String appName,
                             @PathParam("program-type") String type,
                             @PathParam("program-name") String programName,
                             @QueryParam("status") String status,
                             @QueryParam("start") String startTs,
                             @QueryParam("end") String endTs,
                             @QueryParam("limit") @DefaultValue("100") final int resultLimit)
    throws Exception {
    programHistory(request, responder, namespaceId, appName, ApplicationId.DEFAULT_VERSION, type,
                   programName, status, startTs, endTs, resultLimit);
  }

  /**
   * Returns program runs of an app version based on options it returns either currently running or completed or failed.
   * Default it returns all.
   */
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runs")
  public void programHistory(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-name") String appName,
                             @PathParam("app-version") String appVersion,
                             @PathParam("program-type") String type,
                             @PathParam("program-name") String programName,
                             @QueryParam("status") String status,
                             @QueryParam("start") String startTs,
                             @QueryParam("end") String endTs,
                             @QueryParam("limit") @DefaultValue("100") final int resultLimit)
    throws Exception {
    ProgramType programType = getProgramType(type);
    if (programType == null || programType == ProgramType.WEBAPP) {
      throw new NotFoundException(String.format("Program history is not supported for program type '%s'.",
                                                type));
    }
    long start = (startTs == null || startTs.isEmpty()) ? 0 : Long.parseLong(startTs);
    long end = (endTs == null || endTs.isEmpty()) ? Long.MAX_VALUE : Long.parseLong(endTs);

    ProgramId program = new ApplicationId(namespaceId, appName, appVersion).program(programType, programName);
    ProgramSpecification specification = lifecycleService.getProgramSpecification(program);
    if (specification == null) {
      throw new NotFoundException(program);
    }
    getRuns(responder, program, status, start, end, resultLimit);
  }

  /**
   * Returns run record for a particular run of a program.
   */
  @GET
  @Path("/apps/{app-name}/{program-type}/{program-name}/runs/{run-id}")
  public void programRunRecord(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-name") String appName,
                             @PathParam("program-type") String type,
                             @PathParam("program-name") String programName,
                             @PathParam("run-id") String runid) throws NotFoundException {
    programRunRecord(request, responder, namespaceId, appName, ApplicationId.DEFAULT_VERSION, type, programName, runid);
  }

  /**
   * Returns run record for a particular run of a program of an app version.
   */
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runs/{run-id}")
  public void programRunRecord(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("app-name") String appName,
                               @PathParam("app-version") String appVersion,
                               @PathParam("program-type") String type,
                               @PathParam("program-name") String programName,
                               @PathParam("run-id") String runid) throws NotFoundException {
    ProgramType programType = getProgramType(type);
    if (programType == null || programType == ProgramType.WEBAPP) {
      throw new NotFoundException(String.format("Program run record is not supported for program type '%s'.",
                                                programType));
    }
    ProgramId progId = new ApplicationId(namespaceId, appName, appVersion).program(programType, programName);
    RunRecordMeta runRecordMeta = store.getRun(progId, runid);
    if (runRecordMeta != null) {
      RunRecord runRecord = CONVERT_TO_RUN_RECORD.apply(runRecordMeta);
      responder.sendJson(HttpResponseStatus.OK, runRecord);
      return;
    }
    throw new NotFoundException(progId.run(runid));
  }

  /**
   * Get program runtime args.
   */
  @GET
  @Path("/apps/{app-name}/{program-type}/{program-name}/runtimeargs")
  public void getProgramRuntimeArgs(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-name") String appName,
                                    @PathParam("program-type") String type,
                                    @PathParam("program-name") String programName) throws BadRequestException,
    NotImplementedException, NotFoundException {
    getProgramRuntimeArgs(request, responder, namespaceId, appName, ApplicationId.DEFAULT_VERSION, type, programName);
  }

  /**
   * Get program runtime args.
   */
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runtimeargs")
  public void getProgramRuntimeArgs(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-name") String appName,
                                    @PathParam("app-version") String appVersion,
                                    @PathParam("program-type") String type,
                                    @PathParam("program-name") String programName) throws BadRequestException,
    NotImplementedException, NotFoundException {
    ProgramType programType = getProgramType(type);
    if (programType == null || programType == ProgramType.WEBAPP) {
      throw new NotFoundException(String.format("Getting program runtime arguments is not supported for program " +
                                                  "type '%s'.", type));
    }

    ProgramId programId = new ProgramId(namespaceId, appName, programType, programName);
    if (!store.programExists(programId)) {
      throw new NotFoundException(programId);
    }

    Map<String, String> runtimeArgs = preferencesStore.getProperties(programId.getNamespace(), appName,
                                                                     type, programName);
    responder.sendJson(HttpResponseStatus.OK, runtimeArgs);
  }

  /**
   * Save program runtime args.
   */
  @PUT
  @Path("/apps/{app-name}/{program-type}/{program-name}/runtimeargs")
  public void saveProgramRuntimeArgs(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-name") String appName,
                                    @PathParam("program-type") String type,
                                    @PathParam("program-name") String programName) throws Exception {
    saveProgramRuntimeArgs(request, responder, namespaceId, appName, ApplicationId.DEFAULT_VERSION, type, programName);
  }

  /**
   * Save program runtime args.
   */
  @PUT
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runtimeargs")
  public void saveProgramRuntimeArgs(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-name") String appName,
                                    @PathParam("app-version") String appVersion,
                                    @PathParam("program-type") String type,
                                    @PathParam("program-name") String programName) throws Exception {
    ProgramType programType = getProgramType(type);
    if (programType == null || programType == ProgramType.WEBAPP) {
      throw new NotFoundException(String.format("Saving program runtime arguments is not supported for program " +
                                                  "type '%s'.", programType));
    }

    lifecycleService.saveRuntimeArgs(new ProgramId(namespaceId, appName, programType, programName),
                                     decodeArguments(request));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/apps/{app-name}/{program-type}/{program-name}")
  public void programSpecification(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId, @PathParam("app-name") String appName,
                                   @PathParam("program-type") String type,
                                   @PathParam("program-name") String programName) throws Exception {
    programSpecification(request, responder, namespaceId, appName, ApplicationId.DEFAULT_VERSION, type, programName);
  }


  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}")
  public void programSpecification(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespaceId, @PathParam("app-name") String appName,
                                   @PathParam("app-version") String appVersion,
                                   @PathParam("program-type") String type,
                                   @PathParam("program-name") String programName) throws Exception {
    ProgramType programType = getProgramType(type);
    if (programType == null) {
      throw new MethodNotAllowedException(request.getMethod(), request.getUri());
    }

    ApplicationId application = new ApplicationId(namespaceId, appName, appVersion);
    ProgramId programId = application.program(programType, programName);
    ProgramSpecification specification = lifecycleService.getProgramSpecification(programId);
    if (specification == null) {
      throw new NotFoundException(programId);
    }
    responder.sendJson(HttpResponseStatus.OK, specification);
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
                          @PathParam("namespace-id") String namespaceId) throws Exception {

    List<BatchProgram> programs = validateAndGetBatchInput(request, BATCH_PROGRAMS_TYPE);

    List<BatchProgramStatus> statuses = new ArrayList<>(programs.size());
    for (BatchProgram program : programs) {
      ProgramId programId = new ProgramId(namespaceId, program.getAppId(), program.getProgramType(),
                                          program.getProgramId());
      try {
        ProgramStatus programStatus = lifecycleService.getProgramStatus(programId);
        statuses.add(new BatchProgramStatus(
          program, HttpResponseStatus.OK.getCode(), null, programStatus.name()));
      } catch (NotFoundException e) {
        statuses.add(new BatchProgramStatus(
          program, HttpResponseStatus.NOT_FOUND.getCode(), e.getMessage(), null));
      }
    }
    responder.sendJson(HttpResponseStatus.OK, statuses);
  }

  /**
   * Stops all programs that are passed into the data. The data is an array of JSON objects
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
   * as well as a "statusCode" field which maps to the status code for the data in that JsonObjects.
   * </p><p>
   * If an error occurs in the input (for the example above, App2 does not exist), then all JsonObjects for which the
   * parameters have a valid status will have the status field but all JsonObjects for which the parameters do not have
   * a valid status will have an error message and statusCode.
   * </p><p>
   * For example, if there is no App2 in the data above, then the response would be 200 OK with following possible data:
   * </p>
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1", "statusCode": 200},
   * {"appId": "App1", "programType": "Mapreduce", "programId": "Mapreduce2", "statusCode": 200},
   * {"appId":"App2", "programType":"Flow", "programId":"Flow1", "statusCode":404, "error": "App: App2 not found"}]
   * </code></pre>
   */
  @POST
  @Path("/stop")
  public void stopPrograms(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) throws Exception {

    List<BatchProgram> programs = validateAndGetBatchInput(request, BATCH_PROGRAMS_TYPE);

    List<ListenableFuture<BatchProgramResult>> issuedStops = new ArrayList<>(programs.size());
    for (final BatchProgram program : programs) {
      ProgramId programId = new ProgramId(namespaceId, program.getAppId(), program.getProgramType(),
                                         program.getProgramId());
      try {
        List<ListenableFuture<ProgramController>> stops = lifecycleService.issueStop(programId, null);
        for (ListenableFuture<ProgramController> stop : stops) {
          ListenableFuture<BatchProgramResult> issuedStop = Futures.transform(stop,
            new Function<ProgramController, BatchProgramResult>() {
              @Override
              public BatchProgramResult apply(ProgramController input) {
                return new BatchProgramResult(program, HttpResponseStatus.OK.getCode(), null, input.getRunId().getId());
              }
            });
          issuedStops.add(issuedStop);
        }
      } catch (NotFoundException e) {
        issuedStops.add(Futures.immediateFuture(
          new BatchProgramResult(program, HttpResponseStatus.NOT_FOUND.getCode(), e.getMessage())));
      } catch (BadRequestException e) {
        issuedStops.add(Futures.immediateFuture(
          new BatchProgramResult(program, HttpResponseStatus.BAD_REQUEST.getCode(), e.getMessage())));
      }
    }

    List<BatchProgramResult> output = new ArrayList<>(programs.size());
    // need to keep this index in case there is an exception getting the future, since we won't have the program
    // information in that scenario
    int i = 0;
    for (ListenableFuture<BatchProgramResult> issuedStop : issuedStops) {
      try {
        output.add(issuedStop.get());
      } catch (Throwable t) {
        LOG.warn(t.getMessage(), t);
        output.add(new BatchProgramResult(programs.get(i), HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(),
                                          t.getMessage()));
      }
      i++;
    }
    responder.sendJson(HttpResponseStatus.OK, output);
  }

  /**
   * Starts all programs that are passed into the data. The data is an array of JSON objects
   * where each object must contain the following three elements: appId, programType, and programId
   * (flow name, service name, etc.). In additional, each object can contain an optional runtimeargs element,
   * which is a map of arguments to start the program with.
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1"},
   * {"appId": "App1", "programType": "Mapreduce", "programId": "MapReduce2", "runtimeargs":{"arg1":"val1"}},
   * {"appId": "App2", "programType": "Flow", "programId": "Flow1"}]
   * </code></pre>
   * </p><p>
   * The response will be an array of JsonObjects each of which will contain the three input parameters
   * as well as a "statusCode" field which maps to the status code for the data in that JsonObjects.
   * </p><p>
   * If an error occurs in the input (for the example above, App2 does not exist), then all JsonObjects for which the
   * parameters have a valid status will have the status field but all JsonObjects for which the parameters do not have
   * a valid status will have an error message and statusCode.
   * </p><p>
   * For example, if there is no App2 in the data above, then the response would be 200 OK with following possible data:
   * </p>
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1", "statusCode": 200},
   * {"appId": "App1", "programType": "Mapreduce", "programId": "Mapreduce2", "statusCode": 200},
   * {"appId":"App2", "programType":"Flow", "programId":"Flow1", "statusCode":404, "error": "App: App2 not found"}]
   * </code></pre>
   */
  @POST
  @Path("/start")
  public void startPrograms(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) throws Exception {

    List<BatchProgramStart> programs = validateAndGetBatchInput(request, BATCH_STARTS_TYPE);

    List<BatchProgramResult> output = new ArrayList<>(programs.size());
    for (BatchProgramStart program : programs) {
      ProgramId programId = new ProgramId(namespaceId, program.getAppId(), program.getProgramType(),
                                          program.getProgramId());
      try {
        ProgramController programController = lifecycleService.start(programId, program.getRuntimeargs(), false);
        output.add(new BatchProgramResult(program, HttpResponseStatus.OK.getCode(), null,
                                          programController.getRunId().getId()));
      } catch (NotFoundException e) {
        output.add(new BatchProgramResult(program, HttpResponseStatus.NOT_FOUND.getCode(), e.getMessage()));
      } catch (BadRequestException e) {
        output.add(new BatchProgramResult(program, HttpResponseStatus.BAD_REQUEST.getCode(), e.getMessage()));
      } catch (ConflictException e) {
        output.add(new BatchProgramResult(program, HttpResponseStatus.CONFLICT.getCode(), e.getMessage()));
      }
    }
    responder.sendJson(HttpResponseStatus.OK, output);
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
   *  {"appId": "App1", "programType": "Mapreduce", "programId": "Mapreduce2", "statusCode": 400,
   *   "error": "Program type 'Mapreduce' is not a valid program type to get instances"},
   *  {"appId": "App2", "programType": "Flow", "programId": "Flow1", "runnableId": "Flowlet1", "statusCode": 404,
   *   "error": "Program": Flowlet1 not found"}]
   * </code></pre>
   */
  @POST
  @Path("/instances")
  public void getInstances(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) throws IOException, BadRequestException {

    List<BatchRunnable> runnables = validateAndGetBatchInput(request, BATCH_RUNNABLES_TYPE);

    // cache app specs to perform fewer store lookups
    Map<ApplicationId, ApplicationSpecification> appSpecs = new HashMap<>();

    List<BatchRunnableInstances> output = new ArrayList<>(runnables.size());
    for (BatchRunnable runnable : runnables) {
      // cant get instances for things that are not flows, services, or workers
      if (!canHaveInstances(runnable.getProgramType())) {
        output.add(new BatchRunnableInstances(runnable, HttpResponseStatus.BAD_REQUEST.getCode(),
                   String.format("Program type '%s' is not a valid program type to get instances",
                     runnable.getProgramType().getPrettyName())));
        continue;
      }

      ApplicationId appId = new ApplicationId(namespaceId, runnable.getAppId());

      // populate spec cache if this is the first time we've seen the appid.
      if (!appSpecs.containsKey(appId)) {
        appSpecs.put(appId, store.getApplication(appId));
      }

      ApplicationSpecification spec = appSpecs.get(appId);
      if (spec == null) {
        output.add(new BatchRunnableInstances(runnable, HttpResponseStatus.NOT_FOUND.getCode(),
          String.format("App: %s not found", appId)));
        continue;
      }

      ProgramId programId = appId.program(runnable.getProgramType(), runnable.getProgramId());
      output.add(getProgramInstances(runnable, spec, programId.toId()));
    }
    responder.sendJson(HttpResponseStatus.OK, output);
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
                          @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, lifecycleService.list(new NamespaceId(namespaceId), ProgramType.FLOW));
  }

  /**
   * Returns a list of map/reduces associated with a namespace.
   */
  @GET
  @Path("/mapreduce")
  public void getAllMapReduce(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       lifecycleService.list(new NamespaceId(namespaceId), ProgramType.MAPREDUCE));
  }

  /**
   * Returns a list of spark jobs associated with a namespace.
   */
  @GET
  @Path("/spark")
  public void getAllSpark(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, lifecycleService.list(new NamespaceId(namespaceId), ProgramType.SPARK));
  }

  /**
   * Returns a list of workflows associated with a namespace.
   */
  @GET
  @Path("/workflows")
  public void getAllWorkflows(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       lifecycleService.list(new NamespaceId(namespaceId), ProgramType.WORKFLOW));
  }

  /**
   * Returns a list of services associated with a namespace.
   */
  @GET
  @Path("/services")
  public void getAllServices(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, lifecycleService.list(new NamespaceId(namespaceId), ProgramType.SERVICE));
  }

  @GET
  @Path("/workers")
  public void getAllWorkers(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, lifecycleService.list(new NamespaceId(namespaceId), ProgramType.WORKER));
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
      int count = store.getWorkerInstances(new NamespaceId(namespaceId).app(appId).worker(workerId));
      responder.sendJson(HttpResponseStatus.OK, new Instances(count));
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
  public void setWorkerInstances(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("app-id") String appId,
                                 @PathParam("worker-id") String workerId) throws Exception {
    int instances = getInstances(request);
    try {
      lifecycleService.setInstances(new ProgramId(namespaceId, appId, ProgramType.WORKER, workerId), instances);
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
      int count = store.getFlowletInstances(new ProgramId(namespaceId, appId, ProgramType.FLOW, flowId), flowletId);
      responder.sendJson(HttpResponseStatus.OK, new Instances(count));
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
   * Increases number of instance for a flowlet within a flow.
   */
  @PUT
  @Path("/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/instances")
  public synchronized void setFlowletInstances(HttpRequest request, HttpResponder responder,
                                               @PathParam("namespace-id") String namespaceId,
                                               @PathParam("app-id") String appId, @PathParam("flow-id") String flowId,
                                               @PathParam("flowlet-id") String flowletId) throws Exception {
    int instances = getInstances(request);
    try {
      lifecycleService.setInstances(new ProgramId(namespaceId, appId, ProgramType.FLOW, flowId), instances, flowletId);
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
  public void liveInfo(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                       @PathParam("app-id") String appId, @PathParam("program-category") String programCategory,
                       @PathParam("program-id") String programId) {
    ProgramType type = getProgramType(programCategory);
    if (type == null) {
      responder.sendString(HttpResponseStatus.METHOD_NOT_ALLOWED,
                           String.format("Live-info not supported for program type '%s'", programCategory));
      return;
    }
    ProgramId program =
      new ProgramId(namespaceId, appId, ProgramType.valueOfCategoryName(programCategory), programId);
    getLiveInfo(responder, program, runtimeService);
  }


  private void getLiveInfo(HttpResponder responder, ProgramId programId,
                             ProgramRuntimeService runtimeService) {
    try {
      responder.sendJson(HttpResponseStatus.OK, runtimeService.getLiveInfo(programId));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    }
  }

  /**
   * Deletes queues.
   */
  @DELETE
  @Path("/apps/{app-id}/flows/{flow-id}/queues")
  public void deleteFlowQueues(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("app-id") String appId,
                               @PathParam("flow-id") String flowId) throws Exception {
    FlowId flow = new FlowId(namespaceId, appId, flowId);
    try {
      ProgramStatus status = lifecycleService.getProgramStatus(flow);
      if (ProgramStatus.RUNNING == status) {
        responder.sendString(HttpResponseStatus.FORBIDDEN, "Flow is running, please stop it first.");
      } else {
        queueAdmin.dropAllForFlow(flow);
        FlowUtils.deleteFlowPendingMetrics(metricStore, namespaceId, appId, flowId);
        responder.sendStatus(HttpResponseStatus.OK);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
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
                                  @PathParam("service-id") String serviceId) throws Exception {
    try {
      ProgramId programId = new ProgramId(namespaceId, appId, ProgramType.SERVICE, serviceId);
      if (!store.programExists(programId)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Service not found");
        return;
      }

      ServiceSpecification specification = (ServiceSpecification) lifecycleService.getProgramSpecification(programId);
      if (specification == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      int instances = specification.getInstances();
      responder.sendJson(HttpResponseStatus.OK,
                         new ServiceInstances(instances, getInstanceCount(programId, serviceId)));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    }
  }

  /**
   * Return the availability (i.e. discoverable registration) status of a service.
   */
  @GET
  @Path("/apps/{app-name}/services/{service-name}/available")
  public void getServiceAvailability(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-name") String appName,
                                     @PathParam("service-name") String serviceName) throws Exception {
    getServiceAvailability(request, responder, namespaceId, appName, ApplicationId.DEFAULT_VERSION, serviceName);
  }

  /**
   * Return the availability (i.e. discoverable registration) status of a service.
   */
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/services/{service-name}/available")
  public void getServiceAvailability(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-name") String appName,
                                     @PathParam("app-version") String appVersion,
                                     @PathParam("service-name") String serviceName) throws Exception {
    ServiceId serviceId = new ApplicationId(namespaceId, appName, appVersion).service(serviceName);
    ProgramStatus status = lifecycleService.getProgramStatus(serviceId);
    if (status == ProgramStatus.STOPPED) {
      responder.sendString(HttpResponseStatus.SERVICE_UNAVAILABLE, "Service is stopped. Please start it.");
    } else {
      // Construct discoverable name and return 200 OK if discoverable is present. If not return 503.
      String serviceDiscoverableName = ServiceDiscoverable.getName(serviceId);
      EndpointStrategy endpointStrategy =
        new RandomEndpointStrategy(discoveryServiceClient.discover(serviceDiscoverableName));
      if (endpointStrategy.pick(300L, TimeUnit.MILLISECONDS) == null) {
        LOG.trace("Discoverable endpoint {} not found", serviceDiscoverableName);
        responder.sendString(HttpResponseStatus.SERVICE_UNAVAILABLE,
                             "Service is running but not accepting requests at this time.");
      } else {
        responder.sendString(HttpResponseStatus.OK, "Service is available to accept requests.");
      }
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
                                  @PathParam("service-id") String serviceId)
    throws Exception {
    try {
      ProgramId programId = new ProgramId(namespaceId, appId, ProgramType.SERVICE, serviceId);
      if (!store.programExists(programId)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Service not found");
        return;
      }

      int instances = getInstances(request);
      lifecycleService.setInstances(programId, instances);
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
    NamespaceId namespace = new NamespaceId(namespaceId);
    try {
      List<ProgramRecord> flows = lifecycleService.list(new NamespaceId(namespaceId), ProgramType.FLOW);
      for (ProgramRecord flow : flows) {
        String appId = flow.getApp();
        String flowId = flow.getName();
        ProgramId programId = new ProgramId(namespaceId, appId, ProgramType.FLOW, flowId);
        ProgramStatus status = lifecycleService.getProgramStatus(programId);
        if (ProgramStatus.STOPPED != status) {
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
   * Get requested and provisioned instances for a program type.
   * The program type passed here should be one that can have instances (flows, services, ...)
   * Requires caller to do this validation.
   */
  private BatchRunnableInstances getProgramInstances(BatchRunnable runnable, ApplicationSpecification spec,
                                                     Id.Program programId) {
    int requested;
    String programName = programId.getId();
    String runnableId = programName;
    ProgramType programType = programId.getType();
    if (programType == ProgramType.WORKER) {
      if (!spec.getWorkers().containsKey(programName)) {
        return new BatchRunnableInstances(runnable, HttpResponseStatus.NOT_FOUND.getCode(),
                                          "Worker: " + programName + " not found");
      }
      requested = spec.getWorkers().get(programName).getInstances();

    } else if (programType == ProgramType.SERVICE) {
      if (!spec.getServices().containsKey(programName)) {
        return new BatchRunnableInstances(runnable, HttpResponseStatus.NOT_FOUND.getCode(),
                                          "Service: " + programName + " not found");
      }
      requested = spec.getServices().get(programName).getInstances();

    } else if (programType == ProgramType.FLOW) {
      // flows must have runnable id
      runnableId = runnable.getRunnableId();
      if (runnableId == null) {
        return new BatchRunnableInstances(runnable, HttpResponseStatus.BAD_REQUEST.getCode(),
                                          "Must provide the flowlet id as the runnableId for flows");
      }
      FlowSpecification flowSpec = spec.getFlows().get(programName);
      if (flowSpec == null) {
        return new BatchRunnableInstances(runnable, HttpResponseStatus.NOT_FOUND.getCode(),
                                          "Flow: " + programName + " not found");
      }
      FlowletDefinition flowletDefinition = flowSpec.getFlowlets().get(runnableId);
      if (flowletDefinition == null) {
        return new BatchRunnableInstances(runnable, HttpResponseStatus.NOT_FOUND.getCode(),
                                          "Flowlet: " + runnableId + " not found");
      }
      requested = flowletDefinition.getInstances();

    } else {
      return new BatchRunnableInstances(runnable, HttpResponseStatus.BAD_REQUEST.getCode(),
                                        "Instances not supported for program type + " + programType);
    }
    int provisioned = getInstanceCount(programId.toEntityId(), runnableId);
    // use the pretty name of program types to be consistent
    return new BatchRunnableInstances(runnable, HttpResponseStatus.OK.getCode(), provisioned, requested);
  }

  private void getRuns(HttpResponder responder, ProgramId programId, String status,
                       long start, long end, int limit) throws BadRequestException {
    try {
      ProgramRunStatus runStatus = (status == null) ? ProgramRunStatus.ALL :
        ProgramRunStatus.valueOf(status.toUpperCase());

      Collection<RunRecord> records = Collections2.transform(
        store.getRuns(programId, runStatus, start, end, limit).values(),
        CONVERT_TO_RUN_RECORD
      );

      responder.sendJson(HttpResponseStatus.OK, records);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid status %s. Supported options for status of runs are " +
                                                    "running/completed/failed", status));
    }
  }

  /**
   * Returns the number of instances currently running for different runnables for different programs
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

  private int getRequestedServiceInstances(ProgramId serviceId) {
    // Not running on YARN, get it from store
    return store.getServiceInstances(serviceId);
  }

  private boolean isDebugAllowed(ProgramType programType) {
    return EnumSet.of(ProgramType.FLOW, ProgramType.SERVICE, ProgramType.WORKER).contains(programType);
  }

  private boolean canHaveInstances(ProgramType programType) {
    return EnumSet.of(ProgramType.FLOW, ProgramType.SERVICE, ProgramType.WORKER).contains(programType);
  }

  private <T extends BatchProgram> List<T> validateAndGetBatchInput(HttpRequest request, Type type)
    throws BadRequestException, IOException {

    List<T> programs;
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8)) {
      try {
        programs = GSON.fromJson(reader, type);
        if (programs == null) {
          throw new BadRequestException("Request body is invalid json, please check that it is a json array.");
        }
      } catch (JsonSyntaxException e) {
        throw new BadRequestException("Request body is invalid json: " + e.getMessage());
      }
    }

    // validate input
    for (BatchProgram program : programs) {
      try {
        program.validate();
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(
          "Must provide valid appId, programType, and programId for each object: " + e.getMessage());
      }
    }
    return programs;
  }
}
