/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.ProgramSpecification;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.app.mapreduce.MRJobInfoFetcher;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.NotImplementedException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.internal.app.runtime.schedule.SchedulerException;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.ConstraintCodec;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.BatchProgram;
import io.cdap.cdap.proto.BatchProgramCount;
import io.cdap.cdap.proto.BatchProgramHistory;
import io.cdap.cdap.proto.BatchProgramResult;
import io.cdap.cdap.proto.BatchProgramSchedule;
import io.cdap.cdap.proto.BatchProgramStart;
import io.cdap.cdap.proto.BatchProgramStatus;
import io.cdap.cdap.proto.BatchRunnable;
import io.cdap.cdap.proto.BatchRunnableInstances;
import io.cdap.cdap.proto.Containers;
import io.cdap.cdap.proto.Instances;
import io.cdap.cdap.proto.MRJobInfo;
import io.cdap.cdap.proto.NotRunningProgramLiveInfo;
import io.cdap.cdap.proto.ProgramHistory;
import io.cdap.cdap.proto.ProgramLiveInfo;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.RunCountResult;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.ServiceInstances;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.scheduler.ProgramScheduleService;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
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
 * {@link io.cdap.http.HttpHandler} to manage program lifecycle for v3 REST APIs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class ProgramLifecycleHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramLifecycleHttpHandler.class);
  private static final Type BATCH_PROGRAMS_TYPE = new TypeToken<List<BatchProgram>>() { }.getType();
  private static final Type BATCH_RUNNABLES_TYPE = new TypeToken<List<BatchRunnable>>() { }.getType();
  private static final Type BATCH_STARTS_TYPE = new TypeToken<List<BatchProgramStart>>() { }.getType();

  private static final List<Constraint> NO_CONSTRAINTS = Collections.emptyList();

  private static final String SCHEDULES = "schedules";

  /**
   * Json serializer/deserializer.
   */
  private static final Gson GSON = ApplicationSpecificationAdapter
    .addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(Trigger.class, new TriggerCodec())
    .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
    .registerTypeAdapter(Constraint.class, new ConstraintCodec())
    .create();

  /**
   * Json serde for decoding request. It uses a case insensitive enum adapter.
   */
  private static final Gson DECODE_GSON = ApplicationSpecificationAdapter
    .addTypeAdapters(new GsonBuilder())
    .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
    .registerTypeAdapter(Trigger.class, new TriggerCodec())
    .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
    .registerTypeAdapter(Constraint.class, new ConstraintCodec())
    .create();

  private final ProgramScheduleService programScheduleService;
  private final ProgramLifecycleService lifecycleService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final MRJobInfoFetcher mrJobInfoFetcher;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  /**
   * Runtime program service for running and managing programs.
   */
  private final ProgramRuntimeService runtimeService;

  @Inject
  ProgramLifecycleHttpHandler(Store store, ProgramRuntimeService runtimeService,
                              DiscoveryServiceClient discoveryServiceClient,
                              ProgramLifecycleService lifecycleService,
                              MRJobInfoFetcher mrJobInfoFetcher,
                              NamespaceQueryAdmin namespaceQueryAdmin,
                              ProgramScheduleService programScheduleService) {
    this.store = store;
    this.runtimeService = runtimeService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.lifecycleService = lifecycleService;
    this.mrJobInfoFetcher = mrJobInfoFetcher;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.programScheduleService = programScheduleService;
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
    RunRecordDetail runRecordMeta = store.getRun(run);
    if (runRecordMeta == null) {
      throw new NotFoundException(run);
    }

    MRJobInfo mrJobInfo = mrJobInfoFetcher.getMRJobInfo(Id.Run.fromEntityId(run));

    mrJobInfo.setState(runRecordMeta.getStatus().name());
    // Multiple startTs / endTs by 1000, to be consistent with Task-level start/stop times returned by JobClient
    // in milliseconds. RunRecord returns seconds value.
    // The start time of the MRJob is when the run record has been marked as STARTED
    mrJobInfo.setStartTime(TimeUnit.SECONDS.toMillis(runRecordMeta.getStartTs()));
    Long stopTs = runRecordMeta.getStopTs();
    if (stopTs != null) {
      mrJobInfo.setStopTime(TimeUnit.SECONDS.toMillis(stopTs));
    }

    // JobClient (in DistributedMRJobInfoFetcher) can return NaN as some of the values, and GSON otherwise fails
    Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
    responder.sendJson(HttpResponseStatus.OK, gson.toJson(mrJobInfo, mrJobInfo.getClass()));
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
    getStatus(request, responder, namespaceId, appId, ApplicationId.DEFAULT_VERSION, type, programId);
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
      ScheduleId scheduleId = applicationId.schedule(programId);
      ApplicationSpecification appSpec = store.getApplication(applicationId);
      if (appSpec == null) {
        throw new NotFoundException(applicationId);
      }
      json.addProperty("status", programScheduleService.getStatus(scheduleId).toString());
      responder.sendJson(HttpResponseStatus.OK, json.toString());
      return;
    }

    ProgramType programType = getProgramType(type);
    ProgramId program = applicationId.program(programType, programId);
    ProgramStatus programStatus = lifecycleService.getProgramStatus(program);

    Map<String, String> status = ImmutableMap.of("status", programStatus.name());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(status));
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
    ProgramType programType = getProgramType(type);
    ProgramId program = new ProgramId(namespaceId, appId, programType, programId);
    lifecycleService.stop(program, runId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/apps/{app-id}/{program-type}/{program-id}/{action}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void performAction(FullHttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId,
                            @PathParam("program-type") String type,
                            @PathParam("program-id") String programId,
                            @PathParam("action") String action) throws Exception {
    doPerformAction(request, responder, namespaceId, appId, ApplicationId.DEFAULT_VERSION, type, programId, action);
  }

  @POST
  @Path("/apps/{app-id}/versions/{app-version}/{program-type}/{program-id}/{action}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void performAction(FullHttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId,
                            @PathParam("app-version") String appVersion,
                            @PathParam("program-type") String type,
                            @PathParam("program-id") String programId,
                            @PathParam("action") String action) throws Exception {
    doPerformAction(request, responder, namespaceId, appId, appVersion, type, programId, action);
  }

  private void doPerformAction(FullHttpRequest request, HttpResponder responder, String namespaceId, String appId,
                               String appVersion, String type, String programId, String action) throws Exception {
    ApplicationId applicationId = new ApplicationId(namespaceId, appId, appVersion);
    if (SCHEDULES.equals(type)) {
      ScheduleId scheduleId = applicationId.schedule(programId);
      if (action.equals("disable") || action.equals("suspend")) {
        programScheduleService.suspend(scheduleId);
      } else if (action.equals("enable") || action.equals("resume")) {
        programScheduleService.resume(scheduleId);
      } else {
        throw new BadRequestException(
          "Action for schedules may only be 'enable', 'disable', 'suspend', or 'resume' but is'" + action + "'");
      }
      responder.sendJson(HttpResponseStatus.OK, "OK");
      return;
    }

    ProgramType programType = getProgramType(type);
    ProgramId program = applicationId.program(programType, programId);
    Map<String, String> args = decodeArguments(request);
    // we have already validated that the action is valid
    switch (action.toLowerCase()) {
      case "start":
        lifecycleService.run(program, args, false);
        break;
      case "debug":
        if (!isDebugAllowed(programType)) {
          throw new NotImplementedException(String.format("debug action is not implemented for program type %s",
                                                          programType));
        }
        lifecycleService.run(program, args, true);
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
   * Restarts programs which were killed between startTimeSeconds and endTimeSeconds.
   *
   * @param startTimeSeconds lower bound in millis of the stoppage time for programs (inclusive)
   * @param endTimeSeconds upper bound in millis of the stoppage time for programs (exclusive)
   */
  @PUT
  @Path("apps/{app-id}/versions/{app-version}/restart-programs")
  public void restartStoppedPrograms(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-id") String appId,
                                     @PathParam("app-version") String appVersion,
                                     @QueryParam("start-time-seconds") long startTimeSeconds,
                                     @QueryParam("end-time-seconds") long endTimeSeconds) throws Exception {
    lifecycleService.restart(new ApplicationId(namespaceId, appId, appVersion), startTimeSeconds, endTimeSeconds);
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
                             @QueryParam("limit") @DefaultValue("100") final int resultLimit) throws Exception {
    ProgramType programType = getProgramType(type);

    long start = (startTs == null || startTs.isEmpty()) ? 0 : Long.parseLong(startTs);
    long end = (endTs == null || endTs.isEmpty()) ? Long.MAX_VALUE : Long.parseLong(endTs);

    ProgramId program = new ApplicationId(namespaceId, appName, appVersion).program(programType, programName);
    ProgramRunStatus runStatus = (status == null) ? ProgramRunStatus.ALL :
      ProgramRunStatus.valueOf(status.toUpperCase());

    List<RunRecord> records = lifecycleService.getRunRecords(program, runStatus, start, end, resultLimit);

    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(records));
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
                             @PathParam("run-id") String runid) throws NotFoundException, BadRequestException {
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
                               @PathParam("run-id") String runid) throws NotFoundException, BadRequestException {
    ProgramType programType = getProgramType(type);
    ProgramId progId = new ApplicationId(namespaceId, appName, appVersion).program(programType, programName);
    RunRecordDetail runRecordMeta = store.getRun(progId.run(runid));
    if (runRecordMeta != null) {
      RunRecord runRecord = RunRecord.builder(runRecordMeta).build();
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(runRecord));
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
                                    @PathParam("program-name") String programName) throws Exception {
    ProgramType programType = getProgramType(type);
    ProgramId programId = new ProgramId(namespaceId, appName, programType, programName);
    getProgramIdRuntimeArgs(programId, responder);
  }

  /**
   * Save program runtime args.
   */
  @PUT
  @Path("/apps/{app-name}/{program-type}/{program-name}/runtimeargs")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void saveProgramRuntimeArgs(FullHttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-name") String appName,
                                    @PathParam("program-type") String type,
                                    @PathParam("program-name") String programName) throws Exception {
    ProgramType programType = getProgramType(type);
    ProgramId programId = new ProgramId(namespaceId, appName, programType, programName);
    saveProgramIdRuntimeArgs(programId, request, responder);
  }

  /**
   * Get runtime args of a program with app version.
   */
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runtimeargs")
  public void getProgramRuntimeArgs(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-name") String appName,
                                    @PathParam("app-version") String appVersion,
                                    @PathParam("program-type") String type,
                                    @PathParam("program-name") String programName) throws Exception {
    ProgramType programType = getProgramType(type);
    ProgramId programId = new ApplicationId(namespaceId, appName, appVersion).program(programType, programName);
    getProgramIdRuntimeArgs(programId, responder);
  }

  /**
   * Save runtime args of program with app version.
   */
  @PUT
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runtimeargs")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void saveProgramRuntimeArgs(FullHttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-name") String appName,
                                     @PathParam("app-version") String appVersion,
                                     @PathParam("program-type") String type,
                                     @PathParam("program-name") String programName) throws Exception {
    ProgramType programType = getProgramType(type);
    ProgramId programId = new ApplicationId(namespaceId, appName, appVersion).program(programType, programName);
    saveProgramIdRuntimeArgs(programId, request, responder);
  }

  private void getProgramIdRuntimeArgs(ProgramId programId, HttpResponder responder) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(lifecycleService.getRuntimeArgs(programId)));
  }

  private void saveProgramIdRuntimeArgs(ProgramId programId, FullHttpRequest request,
                                        HttpResponder responder) throws Exception {
    lifecycleService.saveRuntimeArgs(programId, decodeArguments(request));
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
    ApplicationId application = new ApplicationId(namespaceId, appName, appVersion);
    ProgramId programId = application.program(programType, programName);
    ProgramSpecification specification = lifecycleService.getProgramSpecification(programId);
    if (specification == null) {
      throw new NotFoundException(programId);
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(specification));
  }

  /**
   * Update schedules which were suspended between startTimeMillis and endTimeMillis
   * @param startTimeMillis lower bound in millis of the update time for schedules (inclusive)
   * @param endTimeMillis upper bound in millis of the update time for schedules (exclusive)
   */
  @PUT
  @Path("schedules/re-enable")
  public void reEnableSuspendedSchedules(HttpRequest request, HttpResponder responder,
                                         @PathParam("namespace-id") String namespaceId,
                                         @QueryParam("start-time-millis") long startTimeMillis,
                                         @QueryParam("end-time-millis") long endTimeMillis) throws Exception {
    programScheduleService.reEnableSchedules(new NamespaceId(namespaceId), startTimeMillis, endTimeMillis);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Get schedules containing {@link ProgramStatusTrigger} filtered by triggering program, and optionally by
   * triggering program statuses or schedule status
   *  @param triggerNamespaceId namespace of the triggering program in {@link ProgramStatusTrigger}
   * @param triggerAppName application name of the triggering program in {@link ProgramStatusTrigger}
   * @param triggerAppVersion application version of the triggering program in {@link ProgramStatusTrigger}
   * @param triggerProgramType program type of the triggering program in {@link ProgramStatusTrigger}
   * @param triggerProgramName program name of the triggering program in {@link ProgramStatusTrigger}
   * @param triggerProgramStatuses comma separated {@link ProgramStatus} in {@link ProgramStatusTrigger}.
   *                               Schedules with {@link ProgramStatusTrigger} triggered by none of the
   *                               {@link ProgramStatus} in triggerProgramStatuses will be filtered out.
   *                               If not specified, schedules will be returned regardless of triggering program status.
   * @param scheduleStatus status of the schedule. Can only be one of "SCHEDULED" or "SUSPENDED".
   *                       If specified, only schedules with matching status will be returned.
   */
  @GET
  @Path("schedules/trigger-type/program-status")
  public void getProgramStatusSchedules(HttpRequest request, HttpResponder responder,
                                        @QueryParam("trigger-namespace-id") String triggerNamespaceId,
                                        @QueryParam("trigger-app-name") String triggerAppName,
                                        @QueryParam("trigger-app-version") @DefaultValue(ApplicationId.DEFAULT_VERSION)
                                            String triggerAppVersion,
                                        @QueryParam("trigger-program-type") String triggerProgramType,
                                        @QueryParam("trigger-program-name") String triggerProgramName,
                                        @QueryParam("trigger-program-statuses") String triggerProgramStatuses,
                                        @QueryParam("schedule-status") String scheduleStatus) throws Exception {
    if (triggerNamespaceId == null) {
      throw new BadRequestException("Must specify trigger-namespace-id as a query param");
    }
    if (triggerAppName == null) {
      throw new BadRequestException("Must specify trigger-app-name as a query param");
    }
    if (triggerProgramType == null) {
      throw new BadRequestException("Must specify trigger-program-type as a query param");
    }
    if (triggerProgramName == null) {
      throw new BadRequestException("Must specify trigger-program-name as a query param");
    }

    ProgramType programType = getProgramType(triggerProgramType);
    ProgramScheduleStatus programScheduleStatus;
    try {
      programScheduleStatus = scheduleStatus == null ? null : ProgramScheduleStatus.valueOf(scheduleStatus);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid schedule status '%s'. Must be one of %s.",
                                                  scheduleStatus, Joiner.on(',').join(ProgramScheduleStatus.values())),
                                    e);
    }

    ProgramId triggerProgramId = new NamespaceId(triggerNamespaceId)
      .app(triggerAppName, triggerAppVersion)
      .program(programType, triggerProgramName);

    Set<io.cdap.cdap.api.ProgramStatus> queryProgramStatuses = new HashSet<>();
    if (triggerProgramStatuses != null) {
      try {
        for (String status : triggerProgramStatuses.split(",")) {
          queryProgramStatuses.add(io.cdap.cdap.api.ProgramStatus.valueOf(status));
        }
      } catch (Exception e) {
        throw new BadRequestException(String.format("Unable to parse program statuses '%s'. Must be comma separated " +
                                                      "valid ProgramStatus names such as COMPLETED, FAILED, KILLED.",
                                                    triggerProgramStatuses), e);
      }
    } else {
      // Query for schedules with all the statuses if no query status is specified
      Collections.addAll(queryProgramStatuses, io.cdap.cdap.api.ProgramStatus.values());
    }

    List<ScheduleDetail> details = programScheduleService.findTriggeredBy(triggerProgramId, queryProgramStatuses)
      .stream()
      .filter(record -> programScheduleStatus == null || record.getMeta().getStatus().equals(programScheduleStatus))
      .map(ProgramScheduleRecord::toScheduleDetail)
      .collect(Collectors.toList());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(details, Schedulers.SCHEDULE_DETAILS_TYPE));
  }

  @GET
  @Path("apps/{app-name}/schedules/{schedule-name}")
  public void getSchedule(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @PathParam("app-name") String appName,
                          @PathParam("schedule-name") String scheduleName) throws Exception {
    doGetSchedule(responder, namespaceId, appName, ApplicationId.DEFAULT_VERSION, scheduleName);
  }

  @GET
  @Path("apps/{app-name}/versions/{app-version}/schedules/{schedule-name}")
  public void getSchedule(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @PathParam("app-name") String appName,
                          @PathParam("app-version") String appVersion,
                          @PathParam("schedule-name") String scheduleName) throws Exception {
    doGetSchedule(responder, namespaceId, appName, appVersion, scheduleName);
  }

  private void doGetSchedule(HttpResponder responder, String namespace,
                             String app, String version, String scheduleName) throws Exception {
    ScheduleId scheduleId = new ApplicationId(namespace, app, version).schedule(scheduleName);
    ProgramScheduleRecord record = programScheduleService.getRecord(scheduleId);
    ScheduleDetail detail = record.toScheduleDetail();
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(detail, ScheduleDetail.class));
  }

  /**
   * See {@link #getAllSchedules(HttpRequest, HttpResponder, String, String, String, String, String)}
   */
  @GET
  @Path("apps/{app-name}/schedules")
  public void getAllSchedules(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @PathParam("app-name") String appName,
                              @QueryParam("trigger-type") String triggerType,
                              @QueryParam("schedule-status") String scheduleStatus) throws Exception {
    getAllSchedules(request, responder, namespaceId, appName,
                    ApplicationId.DEFAULT_VERSION, triggerType, scheduleStatus);
  }

  /**
   * Get schedules in a given application, optionally filtered by the given
   * {@link io.cdap.cdap.proto.ProtoTrigger.Type}.
   * @param namespaceId namespace of the application to get schedules from
   * @param appName name of the application to get schedules from
   * @param appVersion version of the application to get schedules from
   * @param triggerType trigger type of returned schedules. If not specified, all schedules
*                    are returned regardless of trigger type
   * @param scheduleStatus the status of the schedule, must be values in {@link ProgramScheduleStatus}.
   */
  @GET
  @Path("apps/{app-name}/versions/{app-version}/schedules")
  public void getAllSchedules(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @PathParam("app-name") String appName,
                              @PathParam("app-version") String appVersion,
                              @QueryParam("trigger-type") String triggerType,
                              @QueryParam("schedule-status") String scheduleStatus) throws Exception {
    doGetSchedules(responder, new NamespaceId(namespaceId).app(appName, appVersion), null, triggerType, scheduleStatus);
  }

  /**
   * Get program schedules
   */
  @GET
  @Path("/apps/{app-name}/{program-type}/{program-name}/schedules")
  public void getProgramSchedules(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespace,
                                  @PathParam("app-name") String application,
                                  @PathParam("program-type") String type,
                                  @PathParam("program-name") String program,
                                  @QueryParam("trigger-type") String triggerType,
                                  @QueryParam("schedule-status") String scheduleStatus) throws Exception {
    getProgramSchedules(request, responder, namespace, application, ApplicationId.DEFAULT_VERSION,
                        type, program, triggerType, scheduleStatus);
  }

  /**
   * Get Workflow schedules
   */
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/schedules")
  public void getProgramSchedules(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespace,
                                  @PathParam("app-name") String application,
                                  @PathParam("app-version") String appVersion,
                                  @PathParam("program-type") String type,
                                  @PathParam("program-name") String program,
                                  @QueryParam("trigger-type") String triggerType,
                                  @QueryParam("schedule-status") String scheduleStatus) throws Exception {

    ProgramType programType = getProgramType(type);
    if (programType.getSchedulableType() == null) {
      throw new BadRequestException("Program type " + programType + " cannot have schedule");
    }

    ProgramId programId = new ApplicationId(namespace, application, appVersion).program(programType, program);
    doGetSchedules(responder, new NamespaceId(namespace).app(application, appVersion), programId,
                   triggerType, scheduleStatus);
  }

  private void doGetSchedules(HttpResponder responder, ApplicationId applicationId,
                              @Nullable ProgramId programId, @Nullable String triggerTypeStr,
                              @Nullable String statusStr) throws Exception {
    ApplicationSpecification appSpec = store.getApplication(applicationId);
    if (appSpec == null) {
      throw new NotFoundException(applicationId);
    }
    ProgramScheduleStatus status;
    try {
      status = statusStr == null ? null : ProgramScheduleStatus.valueOf(statusStr);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid schedule status '%s'. Must be one of %s.",
                                                  statusStr, Joiner.on(',').join(ProgramScheduleStatus.values())), e);
    }

    ProtoTrigger.Type triggerType;
    try {
      triggerType = triggerTypeStr == null ? null : ProtoTrigger.Type.valueOfCategoryName(triggerTypeStr);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
    }

    Predicate<ProgramScheduleRecord> predicate = record -> true;
    if (status != null) {
      predicate = predicate.and(record -> record.getMeta().getStatus().equals(status));
    }
    if (triggerType != null) {
      predicate = predicate.and(record -> record.getSchedule().getTrigger().getType().equals(triggerType));
    }

    Collection<ProgramScheduleRecord> schedules;
    if (programId != null) {
      if (!appSpec.getProgramsByType(programId.getType().getApiProgramType()).contains(programId.getProgram())) {
        throw new NotFoundException(programId);
      }
      schedules = programScheduleService.list(programId, predicate);
    } else {
      schedules = programScheduleService.list(applicationId, predicate);
    }

    List<ScheduleDetail> details = schedules.stream()
      .map(ProgramScheduleRecord::toScheduleDetail)
      .collect(Collectors.toList());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(details, Schedulers.SCHEDULE_DETAILS_TYPE));
  }

  /**
   * Returns the previous runtime when the scheduled program ran.
   */
  @GET
  @Path("/apps/{app-name}/{program-type}/{program-name}/previousruntime")
  public void getPreviousScheduledRunTime(HttpRequest request, HttpResponder responder,
                                          @PathParam("namespace-id") String namespaceId,
                                          @PathParam("app-name") String appId,
                                          @PathParam("program-type") String type,
                                          @PathParam("program-name") String program) throws Exception {
    ProgramType programType = getProgramType(type);
    handleScheduleRunTime(responder, new NamespaceId(namespaceId).app(appId).program(programType, program), true);
  }

  /**
   * Returns next scheduled runtime of a workflow.
   */
  @GET
  @Path("/apps/{app-name}/{program-type}/{program-name}/nextruntime")
  public void getNextScheduledRunTime(HttpRequest request, HttpResponder responder,
                                      @PathParam("namespace-id") String namespaceId,
                                      @PathParam("app-name") String appId,
                                      @PathParam("program-type") String type,
                                      @PathParam("program-name") String program) throws Exception {
    ProgramType programType = getProgramType(type);
    handleScheduleRunTime(responder, new NamespaceId(namespaceId).app(appId).program(programType, program), false);
  }

  private void handleScheduleRunTime(HttpResponder responder, ProgramId programId,
                                     boolean previousRuntimeRequested) throws Exception {
    try {
      lifecycleService.ensureProgramExists(programId);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(getScheduledRunTimes(programId, previousRuntimeRequested)));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    }
  }

  /**
   * Returns the previous scheduled run time for all programs that are passed into the data.
   * The data is an array of JSON objects
   * where each object must contain the following three elements: appId, programType, and programId
   * (flow name, service name, etc.).
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Workflow", "programId": "WF1"},
   * {"appId": "App1", "programType": "Workflow", "programId": "WF2"}]
   * </code></pre>
   * </p><p>
   * The response will be an array of JsonObjects each of which will contain the three input parameters
   * as well as a "schedules" field, which is a list of {@link ScheduledRuntime} object.
   * </p><p>
   * If an error occurs in the input (for the example above, App1 does not exist), then all JsonObjects for which the
   * parameters have a valid status will have the status field but all JsonObjects for which the parameters do not have
   * a valid status will have an error message and statusCode.
   */
  @POST
  @Path("/previousruntime")
  public void batchPreviousRunTimes(FullHttpRequest request,
                                    HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId) throws Exception {
    List<BatchProgram> batchPrograms = validateAndGetBatchInput(request, BATCH_PROGRAMS_TYPE);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(batchRunTimes(namespaceId, batchPrograms, true)));
  }

  /**
   * Returns the next scheduled run time for all programs that are passed into the data.
   * The data is an array of JSON objects
   * where each object must contain the following three elements: appId, programType, and programId
   * (flow name, service name, etc.).
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Workflow", "programId": "WF1"},
   * {"appId": "App1", "programType": "Workflow", "programId": "WF2"}]
   * </code></pre>
   * </p><p>
   * The response will be an array of JsonObjects each of which will contain the three input parameters
   * as well as a "schedules" field, which is a list of {@link ScheduledRuntime} object.
   * </p><p>
   * If an error occurs in the input (for the example above, App1 does not exist), then all JsonObjects for which the
   * parameters have a valid status will have the status field but all JsonObjects for which the parameters do not have
   * a valid status will have an error message and statusCode.
   */
  @POST
  @Path("/nextruntime")
  public void batchNextRunTimes(FullHttpRequest request,
                                HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId) throws Exception {
    List<BatchProgram> batchPrograms = validateAndGetBatchInput(request, BATCH_PROGRAMS_TYPE);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(batchRunTimes(namespaceId, batchPrograms, false)));
  }

  /**
   * Fetches scheduled run times for a set of programs.
   *
   * @param namespace namespace of the programs
   * @param programs the list of programs to fetch scheduled run times
   * @param previous {@code true} to get the previous scheduled times; {@code false} to get the next scheduled times
   * @return a list of {@link BatchProgramSchedule} containing the result
   * @throws SchedulerException if failed to fetch schedules
   */
  private List<BatchProgramSchedule> batchRunTimes(String namespace, Collection<? extends BatchProgram> programs,
                                                   boolean previous) throws Exception {
    List<ProgramId> programIds = programs.stream()
      .map(p -> new ProgramId(namespace, p.getAppId(), p.getProgramType(), p.getProgramId()))
      .collect(Collectors.toList());

    Set<ApplicationId> appIds = programIds.stream().map(ProgramId::getParent).collect(Collectors.toSet());
    Map<ApplicationId, ApplicationSpecification> appSpecs = store.getApplications(appIds);

    List<BatchProgramSchedule> result = new ArrayList<>();
    for (ProgramId programId : programIds) {
      ApplicationSpecification spec = appSpecs.get(programId.getParent());
      if (spec == null) {
        result.add(new BatchProgramSchedule(programId, HttpResponseStatus.NOT_FOUND.code(),
                                            new NotFoundException(programId.getParent()).getMessage(), null));
        continue;
      }
      try {
        Store.ensureProgramExists(programId, spec);
        result.add(new BatchProgramSchedule(programId, HttpResponseStatus.OK.code(), null,
                                            getScheduledRunTimes(programId, previous)));
      } catch (NotFoundException e) {
        result.add(new BatchProgramSchedule(programId, HttpResponseStatus.NOT_FOUND.code(), e.getMessage(), null));
      } catch (BadRequestException e) {
        result.add(new BatchProgramSchedule(programId, HttpResponseStatus.BAD_REQUEST.code(), e.getMessage(), null));
      }
    }

    return result;
  }

  /**
   * Returns a list of {@link ScheduledRuntime} for the given program.
   *
   * @param programId the program to fetch schedules for
   * @param previous {@code true} to get the previous scheduled times; {@code false} to get the next scheduled times
   * @return a list of {@link ScheduledRuntime}
   * @throws SchedulerException if failed to fetch the schedule
   */
  private List<ScheduledRuntime> getScheduledRunTimes(ProgramId programId,
                                                      boolean previous) throws Exception {
    if (programId.getType().getSchedulableType() == null) {
      throw new BadRequestException("Program " + programId + " cannot have schedule");
    }

    if (previous) {
      return programScheduleService.getPreviousScheduledRuntimes(programId);
    } else {
      return programScheduleService.getNextScheduledRuntimes(programId);
    }
  }

  @PUT
  @Path("apps/{app-name}/schedules/{schedule-name}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addSchedule(FullHttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @PathParam("app-name") String appName,
                          @PathParam("schedule-name") String scheduleName)
    throws Exception {
    doAddSchedule(request, responder, namespaceId, appName, ApplicationId.DEFAULT_VERSION, scheduleName);
  }

  @PUT
  @Path("apps/{app-name}/versions/{app-version}/schedules/{schedule-name}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addSchedule(FullHttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @PathParam("app-name") String appName,
                          @PathParam("app-version") String appVersion,
                          @PathParam("schedule-name") String scheduleName)
    throws Exception {
    doAddSchedule(request, responder, namespaceId, appName, appVersion, scheduleName);
  }

  private void doAddSchedule(FullHttpRequest request, HttpResponder responder, String namespace, String appName,
                             String appVersion, String scheduleName) throws Exception {

    final ApplicationId applicationId = new ApplicationId(namespace, appName, appVersion);
    ScheduleDetail scheduleFromRequest = readScheduleDetailBody(request, scheduleName);

    if (scheduleFromRequest.getProgram() == null) {
      throw new BadRequestException("No program was specified for the schedule");
    }
    if (scheduleFromRequest.getProgram().getProgramType() == null) {
      throw new BadRequestException("No program type was specified for the schedule");
    }
    if (scheduleFromRequest.getProgram().getProgramName() == null) {
      throw new BadRequestException("No program name was specified for the schedule");
    }
    if (scheduleFromRequest.getTrigger() == null) {
      throw new BadRequestException("No trigger was specified for the schedule");
    }
    ProgramType programType = ProgramType.valueOfSchedulableType(scheduleFromRequest.getProgram().getProgramType());
    String programName = scheduleFromRequest.getProgram().getProgramName();
    ProgramId programId = applicationId.program(programType, programName);

    lifecycleService.ensureProgramExists(programId);

    String description = Objects.firstNonNull(scheduleFromRequest.getDescription(), "");
    Map<String, String> properties = Objects.firstNonNull(scheduleFromRequest.getProperties(), Collections.emptyMap());
    List<? extends Constraint> constraints = Objects.firstNonNull(scheduleFromRequest.getConstraints(), NO_CONSTRAINTS);
    long timeoutMillis =
      Objects.firstNonNull(scheduleFromRequest.getTimeoutMillis(), Schedulers.JOB_QUEUE_TIMEOUT_MILLIS);
    ProgramSchedule schedule = new ProgramSchedule(scheduleName, description, programId, properties,
                                                   scheduleFromRequest.getTrigger(), constraints, timeoutMillis);
    programScheduleService.add(schedule);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("apps/{app-name}/schedules/{schedule-name}/update")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void updateSchedule(FullHttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-name") String appName,
                             @PathParam("schedule-name") String scheduleName) throws Exception {
    doUpdateSchedule(request, responder, namespaceId, appName, ApplicationId.DEFAULT_VERSION, scheduleName);
  }

  @POST
  @Path("apps/{app-name}/versions/{app-version}/schedules/{schedule-name}/update")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void updateSchedule(FullHttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-name") String appName,
                             @PathParam("app-version") String appVersion,
                             @PathParam("schedule-name") String scheduleName) throws Exception {
    doUpdateSchedule(request, responder, namespaceId, appName, appVersion, scheduleName);
  }

  private void doUpdateSchedule(FullHttpRequest request, HttpResponder responder, String namespaceId, String appId,
                                String appVersion, String scheduleName) throws Exception {

    ScheduleId scheduleId = new ApplicationId(namespaceId, appId, appVersion).schedule(scheduleName);
    ScheduleDetail scheduleDetail = readScheduleDetailBody(request, scheduleName);

    programScheduleService.update(scheduleId, scheduleDetail);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private ScheduleDetail readScheduleDetailBody(FullHttpRequest request, String scheduleName)
    throws BadRequestException, IOException {

    JsonElement json;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), Charsets.UTF_8)) {
      // The schedule spec in the request body does not contain the program information
      json = DECODE_GSON.fromJson(reader, JsonElement.class);
    } catch (IOException e) {
      throw new IOException("Error reading request body", e);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Request body is invalid json: " + e.getMessage());
    }
    if (!json.isJsonObject()) {
      throw new BadRequestException("Expected a json object in the request body but received " + GSON.toJson(json));
    }
    ScheduleDetail scheduleDetail;
    try {
      scheduleDetail = DECODE_GSON.fromJson(json, ScheduleDetail.class);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Error parsing request body as a schedule specification: " + e.getMessage());
    }

    // If the schedule name is present in the request body, it should match the name in path params
    if (scheduleDetail.getName() != null && !scheduleName.equals(scheduleDetail.getName())) {
      throw new BadRequestException(String.format(
        "Schedule name in the body of the request (%s) does not match the schedule name in the path parameter (%s)",
        scheduleDetail.getName(), scheduleName));
    }
    return scheduleDetail;
  }

  @DELETE
  @Path("apps/{app-name}/schedules/{schedule-name}")
  public void deleteSchedule(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-name") String appName,
                             @PathParam("schedule-name") String scheduleName) throws Exception {
    doDeleteSchedule(responder, namespaceId, appName, ApplicationId.DEFAULT_VERSION, scheduleName);
  }

  @DELETE
  @Path("apps/{app-name}/versions/{app-version}/schedules/{schedule-name}")
  public void deleteSchedule(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-name") String appName,
                             @PathParam("app-version") String appVersion,
                             @PathParam("schedule-name") String scheduleName) throws Exception {
    doDeleteSchedule(responder, namespaceId, appName, appVersion, scheduleName);
  }

  private void doDeleteSchedule(HttpResponder responder, String namespaceId, String appName,
                                String appVersion, String scheduleName) throws Exception {
    ScheduleId scheduleId = new ApplicationId(namespaceId, appName, appVersion).schedule(scheduleName);
    programScheduleService.delete(scheduleId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Update the log level for a running program according to the request body. Currently supported program types are
   * {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   * The request body is expected to contain a map of log levels, where key is loggername, value is one of the
   * valid {@link org.apache.twill.api.logging.LogEntry.Level} or null.
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
    updateLogLevels(request, responder, namespace, appName, ApplicationId.DEFAULT_VERSION, type, programName,
                    runId);
  }

  /**
   * Update the log level for a running program according to the request body.
   */
  @PUT
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runs/{run-id}/loglevels")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void updateProgramLogLevels(FullHttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespace,
                                     @PathParam("app-name") String appName,
                                     @PathParam("app-version") String appVersion,
                                     @PathParam("program-type") String type,
                                     @PathParam("program-name") String programName,
                                     @PathParam("run-id") String runId) throws Exception {
    updateLogLevels(request, responder, namespace, appName, appVersion, type, programName, runId);
  }

  /**
   * Reset the log level for a running program back to where it starts. Currently supported program types are
   * {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   * The request body can either be empty, which will reset all loggers for the program, or contain a list of
   * logger names, which will reset for these particular logger names for the program.
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
    resetLogLevels(request, responder, namespace, appName, ApplicationId.DEFAULT_VERSION, type, programName,
                   runId);
  }

  /**
   * Reset the log level for a running program back to where it starts.
   */
  @POST
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runs/{run-id}/resetloglevels")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void resetProgramLogLevels(FullHttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespace,
                                    @PathParam("app-name") String appName,
                                    @PathParam("app-version") String appVersion,
                                    @PathParam("program-type") String type,
                                    @PathParam("program-name") String programName,
                                    @PathParam("run-id") String runId) throws Exception {
    resetLogLevels(request, responder, namespace, appName, appVersion, type, programName, runId);
  }

  /**
   * Returns the status for all programs that are passed into the data. The data is an array of JSON objects
   * where each object must contain the following three elements: appId, programType, and programId
   * (flow name, service name, etc.).
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1"},
   * {"appId": "App1", "programType": "Mapreduce", "programId": "MapReduce2"}]
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
   * {"appId": "App1", "programType": "Mapreduce", "programId": "Mapreduce2", "statusCode": 200, "status": "STOPPED"}]
   * </code></pre>
   */
  @POST
  @Path("/status")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void getStatuses(FullHttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId) throws Exception {

    List<BatchProgram> batchPrograms = validateAndGetBatchInput(request, BATCH_PROGRAMS_TYPE);
    List<ProgramId> programs = batchPrograms.stream()
      .map(p -> new ProgramId(namespaceId, p.getAppId(), p.getProgramType(), p.getProgramId()))
      .collect(Collectors.toList());

    Map<ProgramId, ProgramStatus> statuses = lifecycleService.getProgramStatuses(programs);

    List<BatchProgramStatus> result = new ArrayList<>(programs.size());
    for (BatchProgram program : batchPrograms) {
      ProgramId programId = new ProgramId(namespaceId, program.getAppId(), program.getProgramType(),
                                          program.getProgramId());
      ProgramStatus status = statuses.get(programId);
      if (status == null) {
        result.add(new BatchProgramStatus(program, HttpResponseStatus.NOT_FOUND.code(),
                                          new NotFoundException(programId).getMessage(), null));
      } else {
        result.add(new BatchProgramStatus(program, HttpResponseStatus.OK.code(), null, status.name()));
      }
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(result));
  }

  /**
   * Stops all programs that are passed into the data. The data is an array of JSON objects
   * where each object must contain the following three elements: appId, programType, and programId
   * (flow name, service name, etc.).
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1"},
   * {"appId": "App1", "programType": "Mapreduce", "programId": "MapReduce2"}]
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
   * {"appId": "App1", "programType": "Mapreduce", "programId": "Mapreduce2", "statusCode": 200}]
   * </code></pre>
   */
  @POST
  @Path("/stop")
  @AuditPolicy({AuditDetail.REQUEST_BODY, AuditDetail.RESPONSE_BODY})
  public void stopPrograms(FullHttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId,
                           @QueryParam("graceful") long graceful) throws Exception {
    List<BatchProgram> programs = validateAndGetBatchInput(request, BATCH_PROGRAMS_TYPE);

    List<ListenableFuture<BatchProgramResult>> issuedStops = new ArrayList<>(programs.size());
    for (final BatchProgram program : programs) {
      ProgramId programId = new ProgramId(namespaceId, program.getAppId(), program.getProgramType(),
                                         program.getProgramId());
      store.setStopping(null, null, graceful);
      try {
        List<ListenableFuture<ProgramRunId>> stops = lifecycleService.issueStop(programId, null);
        for (ListenableFuture<ProgramRunId> stop : stops) {
          ListenableFuture<BatchProgramResult> issuedStop =
            Futures.transform(stop, (Function<ProgramRunId, BatchProgramResult>) input ->
              new BatchProgramResult(program, HttpResponseStatus.OK.code(), null, input.getRun()));
          issuedStops.add(issuedStop);
        }
      } catch (NotFoundException e) {
        issuedStops.add(Futures.immediateFuture(
          new BatchProgramResult(program, HttpResponseStatus.NOT_FOUND.code(), e.getMessage())));
      } catch (BadRequestException e) {
        issuedStops.add(Futures.immediateFuture(
          new BatchProgramResult(program, HttpResponseStatus.BAD_REQUEST.code(), e.getMessage())));
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
        output.add(new BatchProgramResult(programs.get(i), HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                          t.getMessage()));
      }
      i++;
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(output));
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
   * {"appId": "App1", "programType": "Mapreduce", "programId": "MapReduce2", "runtimeargs":{"arg1":"val1"}}]
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
   * {"appId": "App2", "programType": "Mapreduce", "programId": "Mapreduce2",
   *  "statusCode":404, "error": "App: App2 not found"}]
   * </code></pre>
   */
  @POST
  @Path("/start")
  @AuditPolicy({AuditDetail.REQUEST_BODY, AuditDetail.RESPONSE_BODY})
  public void startPrograms(FullHttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) throws Exception {

    List<BatchProgramStart> programs = validateAndGetBatchInput(request, BATCH_STARTS_TYPE);

    List<BatchProgramResult> output = new ArrayList<>(programs.size());
    for (BatchProgramStart program : programs) {
      ProgramId programId = new ProgramId(namespaceId, program.getAppId(), program.getProgramType(),
                                          program.getProgramId());
      try {
        String runId = lifecycleService.run(programId, program.getRuntimeargs(), false).getId();
        output.add(new BatchProgramResult(program, HttpResponseStatus.OK.code(), null, runId));
      } catch (NotFoundException e) {
        output.add(new BatchProgramResult(program, HttpResponseStatus.NOT_FOUND.code(), e.getMessage()));
      } catch (BadRequestException e) {
        output.add(new BatchProgramResult(program, HttpResponseStatus.BAD_REQUEST.code(), e.getMessage()));
      } catch (ConflictException e) {
        output.add(new BatchProgramResult(program, HttpResponseStatus.CONFLICT.code(), e.getMessage()));
      }
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(output));
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
   *  {"appId": "App1", "programType": "Mapreduce", "programId": "Mapreduce2"}]
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
        output.add(new BatchRunnableInstances(runnable, HttpResponseStatus.BAD_REQUEST.code(),
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
        output.add(new BatchRunnableInstances(runnable, HttpResponseStatus.NOT_FOUND.code(),
          String.format("App: %s not found", appId)));
        continue;
      }

      ProgramId programId = appId.program(runnable.getProgramType(), runnable.getProgramId());
      output.add(getProgramInstances(runnable, spec, programId));
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(output));
  }

  /**
   * Returns the run counts for all program runnables that are passed into the data. The data is an array of
   * Json objects where each object must contain the following three elements: appId, programType, and programId.
   * The max number of programs in the request is 100.
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1"},
   *  {"appId": "App1", "programType": "Workflow", "programId": "testWorkflow"},
   *  {"appId": "App2", "programType": "Workflow", "programId": "DataPipelineWorkflow"}]
   * </code></pre>
   * </p><p>
   * </p><p>
   * The response will be an array of JsonObjects each of which will contain the three input parameters
   * as well as 2 fields, "runCount" which maps to the count of the program and "statusCode" which maps to the
   * status code for the data in that JsonObjects.
   * </p><p>
   * If an error occurs in the input (for the example above, workflow in app1 does not exist),
   * then all JsonObjects for which the parameters have a valid status will have the count field but all JsonObjects
   * for which the parameters do not have a valid status will have an error message and statusCode.
   * </p><p>
   * For example, if there is no workflow in App1 in the data above, then the response would be 200 OK with following
   * possible data:
   * </p>
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1",
   * "statusCode": 200, "runCount": 20},
   * {"appId": "App1", "programType": "Workflow", "programId": "testWorkflow", "statusCode": 404,
   * "error": "Program 'testWorkflow' is not found"},
   *  {"appId": "App2", "programType": "Workflow", "programId": "DataPipelineWorkflow",
   *  "statusCode": 200, "runCount": 300}]
   * </code></pre>
   */
  @POST
  @Path("/runcount")
  public void getRunCounts(FullHttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) throws Exception {
    List<BatchProgram> programs = validateAndGetBatchInput(request, BATCH_PROGRAMS_TYPE);
    if (programs.size() > 100) {
      throw new BadRequestException(String.format("%d programs found in the request, the maximum number " +
                                                    "supported is 100", programs.size()));
    }

    List<ProgramId> programIds =
      programs.stream().map(batchProgram -> new ProgramId(namespaceId, batchProgram.getAppId(),
                                                          batchProgram.getProgramType(),
                                                          batchProgram.getProgramId())).collect(Collectors.toList());
    List<BatchProgramCount> counts = new ArrayList<>(programs.size());
    for (RunCountResult runCountResult : lifecycleService.getProgramRunCounts(programIds)) {
      ProgramId programId = runCountResult.getProgramId();
      Exception exception = runCountResult.getException();
      if (exception == null) {
        counts.add(new BatchProgramCount(programId, HttpResponseStatus.OK.code(), null, runCountResult.getCount()));
      } else if (exception instanceof NotFoundException) {
        counts.add(new BatchProgramCount(programId, HttpResponseStatus.NOT_FOUND.code(),
                                         exception.getMessage(), null));
      } else if (exception instanceof UnauthorizedException) {
        counts.add(new BatchProgramCount(programId, HttpResponseStatus.FORBIDDEN.code(),
                                         exception.getMessage(), null));
      } else {
        counts.add(new BatchProgramCount(programId, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                         exception.getMessage(), null));
      }
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(counts));
  }

  /**
   * Returns the latest runs for all program runnables that are passed into the data. The data is an array of
   * Json objects where each object must contain the following three elements: appId, programType, and programId.
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1"},
   *  {"appId": "App1", "programType": "Workflow", "programId": "testWorkflow"},
   *  {"appId": "App2", "programType": "Workflow", "programId": "DataPipelineWorkflow"}]
   * </code></pre>
   * </p><p>
   * </p><p>
   * The response will be an array of JsonObjects each of which will contain the three input parameters
   * as well as 2 fields, "runs" which is a list of the latest run records and "statusCode" which maps to the
   * status code for the data in that JsonObjects.
   * </p><p>
   * If an error occurs in the input (for the example above, workflow in app1 does not exist),
   * then all JsonObjects for which the parameters have a valid status will have the count field but all JsonObjects
   * for which the parameters do not have a valid status will have an error message and statusCode.
   * </p><p>
   * For example, if there is no workflow in App1 in the data above, then the response would be 200 OK with following
   * possible data:
   * </p>
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1",
   * "statusCode": 200, "runs": [...]},
   * {"appId": "App1", "programType": "Workflow", "programId": "testWorkflow", "statusCode": 404,
   * "error": "Program 'testWorkflow' is not found"},
   *  {"appId": "App2", "programType": "Workflow", "programId": "DataPipelineWorkflow", "runnableId": "Flowlet1",
   *  "statusCode": 200, "runs": [...]}]
   * </code></pre>
   */
  @POST
  @Path("/runs")
  public void getLatestRuns(FullHttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) throws Exception {
    List<BatchProgram> programs = validateAndGetBatchInput(request, BATCH_PROGRAMS_TYPE);
    List<ProgramId> programIds =
      programs.stream().map(batchProgram -> new ProgramId(namespaceId, batchProgram.getAppId(),
                                                          batchProgram.getProgramType(),
                                                          batchProgram.getProgramId())).collect(Collectors.toList());

    List<BatchProgramHistory> response = new ArrayList<>(programs.size());
    List<ProgramHistory> result = lifecycleService.getRunRecords(programIds, ProgramRunStatus.ALL, 0,
                                                                 Long.MAX_VALUE, 1);
    for (ProgramHistory programHistory : result) {
      ProgramId programId = programHistory.getProgramId();
      Exception exception = programHistory.getException();
      BatchProgram batchProgram = new BatchProgram(programId.getApplication(), programId.getType(),
                                                   programId.getProgram());
      if (exception == null) {
        response.add(new BatchProgramHistory(batchProgram, HttpResponseStatus.OK.code(), null,
                                             programHistory.getRuns()));
      } else if (exception instanceof NotFoundException) {
        response.add(new BatchProgramHistory(batchProgram, HttpResponseStatus.NOT_FOUND.code(),
                                             exception.getMessage(), Collections.emptyList()));
      } else if (exception instanceof UnauthorizedException) {
        response.add(new BatchProgramHistory(batchProgram, HttpResponseStatus.FORBIDDEN.code(),
                                             exception.getMessage(), Collections.emptyList()));
      } else {
        response.add(new BatchProgramHistory(batchProgram, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                             exception.getMessage(), Collections.emptyList()));
      }
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(response));
  }

  /**
   * Returns the count of the given program runs
   */
  @GET
  @Path("/apps/{app-name}/{program-type}/{program-name}/runcount")
  public void getProgramRunCount(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("app-name") String appName,
                                 @PathParam("program-type") String type,
                                 @PathParam("program-name") String programName) throws Exception {
    getProgramRunCount(request, responder, namespaceId, appName, ApplicationId.DEFAULT_VERSION, type, programName);
  }

  /**
   * Returns the count of the given program runs
   */
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runcount")
  public void getProgramRunCount(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("app-name") String appName,
                                 @PathParam("app-version") String appVersion,
                                 @PathParam("program-type") String type,
                                 @PathParam("program-name") String programName) throws Exception {
    ProgramType programType = getProgramType(type);
    ProgramId programId = new ApplicationId(namespaceId, appName, appVersion).program(programType, programName);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(lifecycleService.getProgramRunCount(programId)));
  }

  /*
  Note: Cannot combine the following get all programs methods into one because then API path will clash with /apps path
   */

  /**
   * Returns a list of map/reduces associated with a namespace.
   */
  @GET
  @Path("/mapreduce")
  public void getAllMapReduce(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(lifecycleService.list(validateAndGetNamespace(namespaceId), ProgramType.MAPREDUCE)));
  }

  /**
   * Returns a list of spark jobs associated with a namespace.
   */
  @GET
  @Path("/spark")
  public void getAllSpark(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(lifecycleService.list(validateAndGetNamespace(namespaceId), ProgramType.SPARK)));
  }

  /**
   * Returns a list of workflows associated with a namespace.
   */
  @GET
  @Path("/workflows")
  public void getAllWorkflows(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(lifecycleService.list(validateAndGetNamespace(namespaceId), ProgramType.WORKFLOW)));
  }

  /**
   * Returns a list of services associated with a namespace.
   */
  @GET
  @Path("/services")
  public void getAllServices(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(lifecycleService.list(validateAndGetNamespace(namespaceId), ProgramType.SERVICE)));
  }

  @GET
  @Path("/workers")
  public void getAllWorkers(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(lifecycleService.list(validateAndGetNamespace(namespaceId), ProgramType.WORKER)));
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
      ProgramId programId = validateAndGetNamespace(namespaceId).app(appId).worker(workerId);
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

  @GET
  @Path("/apps/{app-id}/{program-category}/{program-id}/live-info")
  @SuppressWarnings("unused")
  public void liveInfo(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                       @PathParam("app-id") String appId, @PathParam("program-category") String programCategory,
                       @PathParam("program-id") String programId) throws BadRequestException {
    ProgramType type = getProgramType(programCategory);
    ProgramId program = new ProgramId(namespaceId, appId, type, programId);
    getLiveInfo(responder, program, runtimeService);
  }


  private void getLiveInfo(HttpResponder responder, ProgramId programId, ProgramRuntimeService runtimeService) {
    try {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(runtimeService.getLiveInfo(programId)));
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
      ProgramId programId = validateAndGetNamespace(namespaceId).app(appId).service(serviceId);
      lifecycleService.ensureProgramExists(programId);
      int instances = store.getServiceInstances(programId);
      responder.sendJson(HttpResponseStatus.OK,
                         GSON.toJson(new ServiceInstances(instances, getInstanceCount(programId, serviceId))));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    }
  }

  /**
   * Return the availability (i.e. discoverable registration) status of a service.
   */
  @GET
  @Path("/apps/{app-name}/{service-type}/{program-name}/available")
  public void getServiceAvailability(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-name") String appName,
                                     @PathParam("service-type") String serviceType,
                                     @PathParam("program-name") String programName) throws Exception {
    getServiceAvailability(request, responder, namespaceId, appName,
                           ApplicationId.DEFAULT_VERSION, serviceType, programName);
  }

  /**
   * Return the availability (i.e. discoverable registration) status of a service.
   */
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{service-type}/{program-name}/available")
  public void getServiceAvailability(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-name") String appName,
                                     @PathParam("app-version") String appVersion,
                                     @PathParam("service-type") String serviceType,
                                     @PathParam("program-name") String programName) throws Exception {
    // Currently we only support services and sparks as the service-type
    ProgramType programType = getProgramType(serviceType);
    if (!ServiceDiscoverable.getUserServiceTypes().contains(programType)) {
      throw new BadRequestException("Only service or spark is support for service availability check");
    }

    ProgramId programId = new ProgramId(new ApplicationId(namespaceId, appName, appVersion), programType, programName);
    ProgramStatus status = lifecycleService.getProgramStatus(programId);
    if (status == ProgramStatus.STOPPED) {
      throw new ServiceUnavailableException(programId.toString(), "Service is stopped. Please start it.");
    }

    // Construct discoverable name and return 200 OK if discoverable is present. If not return 503.
    String discoverableName = ServiceDiscoverable.getName(programId);

    // TODO: CDAP-12959 - Should use the UserServiceEndpointStrategy and discover based on the version
    // and have appVersion nullable for the non versioned endpoint
    EndpointStrategy strategy = new RandomEndpointStrategy(() -> discoveryServiceClient.discover(discoverableName));
    if (strategy.pick(300L, TimeUnit.MILLISECONDS) == null) {
      LOG.trace("Discoverable endpoint {} not found", discoverableName);
      throw new ServiceUnavailableException(programId.toString(),
                                            "Service is running but not accepting requests at this time.");
    }

    responder.sendString(HttpResponseStatus.OK, "Service is available to accept requests.");
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
      ProgramId programId = new ProgramId(namespaceId, appId, ProgramType.SERVICE, serviceId);
      Store.ensureProgramExists(programId, store.getApplication(programId.getParent()));

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

  /**
   * Get requested and provisioned instances for a program type.
   * The program type passed here should be one that can have instances (flows, services, ...)
   * Requires caller to do this validation.
   */
  private BatchRunnableInstances getProgramInstances(BatchRunnable runnable, ApplicationSpecification spec,
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
    return new BatchRunnableInstances(runnable, HttpResponseStatus.OK.code(), provisioned, requested);
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
    return EnumSet.of(ProgramType.SERVICE, ProgramType.WORKER).contains(programType);
  }

  private boolean canHaveInstances(ProgramType programType) {
    return EnumSet.of(ProgramType.SERVICE, ProgramType.WORKER).contains(programType);
  }

  private <T extends BatchProgram> List<T> validateAndGetBatchInput(FullHttpRequest request, Type type)
    throws BadRequestException, IOException {

    List<T> programs;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
      try {
        programs = DECODE_GSON.fromJson(reader, type);
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

  private void updateLogLevels(FullHttpRequest request, HttpResponder responder, String namespace, String appName,
                               String appVersion, String type, String programName,
                               String runId) throws Exception {
    ProgramType programType = getProgramType(type);
    try {
      // we are decoding the body to Map<String, String> instead of Map<String, LogEntry.Level> here since Gson will
      // serialize invalid enum values to null, which is allowed for log level, instead of throw an Exception.
      lifecycleService.updateProgramLogLevels(
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

  private void resetLogLevels(FullHttpRequest request, HttpResponder responder, String namespace, String appName,
                              String appVersion, String type, String programName,
                              String runId) throws Exception {
    ProgramType programType = getProgramType(type);
    try {
      Set<String> loggerNames = parseBody(request, SET_STRING_TYPE);
      lifecycleService.resetProgramLogLevels(
        new ApplicationId(namespace, appName, appVersion).program(programType, programName),
        loggerNames == null ? Collections.emptySet() : loggerNames, runId);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid JSON in body");
    } catch (SecurityException e) {
      throw new UnauthorizedException("Unauthorized to reset the log levels");
    }
  }

  private NamespaceId validateAndGetNamespace(String namespace) throws NamespaceNotFoundException {
    NamespaceId namespaceId = new NamespaceId(namespace);
    try {
      namespaceQueryAdmin.get(namespaceId);
    } catch (NamespaceNotFoundException e) {
      throw e;
    } catch (Exception e) {
      // This can only happen when NamespaceAdmin uses HTTP to interact with namespaces.
      // Within AppFabric, NamespaceAdmin is bound to DefaultNamespaceAdmin which directly interacts with MDS.
      // Hence, this should never happen.
      throw Throwables.propagate(e);
    }
    return namespaceId;
  }

  /**
   * Parses the give program type into {@link ProgramType} object.
   *
   * @param programType the program type to parse.
   *
   * @throws BadRequestException if the given program type is not a valid {@link ProgramType}.
   */
  private ProgramType getProgramType(String programType) throws BadRequestException {
    try {
      return ProgramType.valueOfCategoryName(programType);
    } catch (Exception e) {
      throw new BadRequestException(String.format("Invalid program type '%s'", programType), e);
    }
  }
}
