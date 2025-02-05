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

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.ProgramSpecification;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.app.mapreduce.MRJobInfoFetcher;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.NotImplementedException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.gateway.handlers.util.NamespaceHelper;
import io.cdap.cdap.gateway.handlers.util.ProgramHandlerUtil;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.BatchProgram;
import io.cdap.cdap.proto.BatchProgramCount;
import io.cdap.cdap.proto.BatchProgramHistory;
import io.cdap.cdap.proto.BatchProgramResult;
import io.cdap.cdap.proto.BatchProgramStart;
import io.cdap.cdap.proto.BatchProgramStatus;
import io.cdap.cdap.proto.MRJobInfo;
import io.cdap.cdap.proto.ProgramHistory;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunCountResult;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link io.cdap.http.HttpHandler} to manage program lifecycle for v3 REST APIs.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class ProgramLifecycleHttpHandler extends AbstractAppFabricHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramLifecycleHttpHandler.class);
  private static final Type BATCH_PROGRAMS_TYPE = new TypeToken<List<BatchProgram>>() {
  }.getType();
  private static final Type BATCH_STARTS_TYPE = new TypeToken<List<BatchProgramStart>>() {
  }.getType();

  private final ProgramLifecycleService lifecycleService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final MRJobInfoFetcher mrJobInfoFetcher;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final Store store;

  @Inject
  ProgramLifecycleHttpHandler(Store store,
      DiscoveryServiceClient discoveryServiceClient,
      ProgramLifecycleService lifecycleService,
      MRJobInfoFetcher mrJobInfoFetcher,
      NamespaceQueryAdmin namespaceQueryAdmin) {
    this.store = store;
    this.discoveryServiceClient = discoveryServiceClient;
    this.lifecycleService = lifecycleService;
    this.mrJobInfoFetcher = mrJobInfoFetcher;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
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
    ApplicationReference appRef = new ApplicationReference(namespaceId, appId);
    ProgramReference programRef = appRef.program(ProgramType.MAPREDUCE, mapreduceId);

    // runId is uuid, can be retrieved ignoring version
    RunRecordDetail runRecordMeta = store.getRun(programRef, runId);
    if (runRecordMeta == null || isTetheredRunRecord(runRecordMeta)) {
      throw new NotFoundException(
          String.format("No run record found for program %s and runID: %s", programRef, runId));
    }

    ApplicationId applicationId = appRef.app(runRecordMeta.getVersion());
    ApplicationSpecification appSpec = store.getApplication(applicationId);
    if (appSpec == null) {
      throw new NotFoundException(applicationId);
    }
    if (!appSpec.getMapReduce().containsKey(mapreduceId)) {
      throw new NotFoundException(programRef);
    }

    MRJobInfo mrJobInfo = mrJobInfoFetcher.getMRJobInfo(
        Id.Run.fromEntityId(runRecordMeta.getProgramRunId()));

    mrJobInfo.setState(runRecordMeta.getStatus().name());
    // Multiple startTs / endTs by 1000, to be consistent with Task-level start/stop times returned by JobClient
    // in milliseconds. RunRecord returns seconds value.
    // The start time of the MRJob is when the run record has been marked as STARTED
    mrJobInfo.setStartTime(TimeUnit.SECONDS.toMillis(runRecordMeta.getStartTs()));
    Long stopTs = runRecordMeta.getStopTs();
    if (stopTs != null) {
      mrJobInfo.setStopTime(TimeUnit.SECONDS.toMillis(stopTs));
    }

    // JobClient (in DistributedMRJobInfoFetcher) can return NaN as some values, and GSON otherwise fails
    Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
    responder.sendJson(HttpResponseStatus.OK, gson.toJson(mrJobInfo, mrJobInfo.getClass()));
  }

  /**
   * Returns status of a type specified by the type{flows,workflows,mapreduce,spark,services}.
   */
  @GET
  @Path("/apps/{app-id}/{program-type}/{program-id}/status")
  public void getStatus(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("program-type") String type,
      @PathParam("program-id") String programId) throws Exception {
    getStatusVersioned(request, responder, namespaceId, appId, ApplicationId.DEFAULT_VERSION, type,
        programId);
  }

  /**
   * Returns status of a type specified by the type{flows,workflows,mapreduce,spark,services}.
   * Deprecated: Only allowing program info retrieval for the latest app version (active app).
   */
  @Deprecated
  @GET
  @Path("/apps/{app-id}/versions/{version-id}/{program-type}/{program-id}/status")
  public void getStatusVersioned(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("version-id") String versionId,
      @PathParam("program-type") String type,
      @PathParam("program-id") String programId) throws Exception {
    ApplicationReference appReference = new ApplicationReference(namespaceId, appId);
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    ProgramStatus programStatus;
    if (ApplicationId.DEFAULT_VERSION.equals(versionId)) {
      programStatus = lifecycleService.getProgramStatus(
          appReference.program(programType, programId));
    } else {
      programStatus = lifecycleService.getProgramStatus(
          appReference.app(versionId).program(programType, programId));
    }

    Map<String, String> status = ImmutableMap.of("status", programStatus.name());
    responder.sendJson(HttpResponseStatus.OK, ProgramHandlerUtil.toJson(status));
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
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    ProgramReference program = new ApplicationReference(namespaceId, appId).program(programType,
        programId);

    try {
      RunIds.fromString(runId);
    } catch (Exception e) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }
    lifecycleService.stop(program, runId, null);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Perform an action on the latest version of a program.
   */
  @POST
  @Path("/apps/{app-id}/{program-type}/{program-id}/{action}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void performAction(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("program-type") String type,
      @PathParam("program-id") String programId,
      @PathParam("action") String action) throws Exception {
    doPerformAction(request, responder, namespaceId, appId, ApplicationId.DEFAULT_VERSION, type,
        programId, action);
  }

  /**
   * Perform an action on specific {@code appVersion} of a program.
   */
  @POST
  @Path("/apps/{app-id}/versions/{app-version}/{program-type}/{program-id}/{action}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void performActionVersioned(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("app-version") String appVersion,
      @PathParam("program-type") String type,
      @PathParam("program-id") String programId,
      @PathParam("action") String action) throws Exception {
    doPerformAction(request, responder, namespaceId, appId, appVersion, type, programId, action);
  }

  /**
   * Perform an action on specific {@code appVersion} of a program.
   *
   * @param namespaceId namespace of the program
   * @param appId appId of the program
   * @param appVersion app version of the program
   * @param type type of the program
   * @param programId programId of the program
   * @param action action to be performed. The value can be one of start/stop/debug for other
   *     program types. "debug" can only be applied on type {@link ProgramType#SERVICE} and
   *     {@link ProgramType#WORKER}
   * @throws NotFoundException if action is an unknown action
   * @throws BadRequestException if action is "start" or "debug" and appVersion is not the
   *     latest version
   * @throws NotImplementedException if action is debug and type is not {@link
   *     ProgramType#SERVICE} or {@link ProgramType#WORKER}
   */
  private void doPerformAction(FullHttpRequest request, HttpResponder responder, String namespaceId,
      String appId,
      String appVersion, String type, String programId, String action) throws Exception {
    ApplicationId applicationId = new ApplicationId(namespaceId, appId, appVersion);
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    ProgramReference programReference = new ProgramReference(namespaceId, appId, programType,
        programId);
    ProgramId program = applicationId.program(programType, programId);
    Map<String, String> args = decodeArguments(request);
    // we have already validated that the action is valid
    switch (action.toLowerCase()) {
      case "start":
        if (ApplicationId.DEFAULT_VERSION.equals(appVersion)) {
          lifecycleService.run(programReference, args, false);
        } else {
          lifecycleService.run(program, args, false);
        }
        break;
      case "debug":
        if (!isDebugAllowed(programType)) {
          throw new NotImplementedException(
              String.format("debug action is not implemented for program type %s",
                  programType));
        }
        if (ApplicationId.DEFAULT_VERSION.equals(appVersion)) {
          lifecycleService.run(programReference, args, true);
        } else {
          lifecycleService.run(program, args, true);
        }
        break;
      case "stop":
        if (ApplicationId.DEFAULT_VERSION.equals(appVersion)) {
          lifecycleService.stop(programReference);
        } else {
          lifecycleService.stop(program);
        }
        break;
      default:
        throw new NotFoundException(String.format("%s action was not found", action));
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Restarts programs which were killed between startTimeSeconds and endTimeSeconds.
   *
   * @param startTimeSeconds lower bound in millis of the stoppage time for programs
   *     (inclusive)
   * @param endTimeSeconds upper bound in millis of the stoppage time for programs (exclusive)
   *
   * Deprecated: Only allowing for the latest app version (active app).
   */
  @Deprecated
  @PUT
  @Path("apps/{app-id}/versions/{app-version}/restart-programs")
  public void restartStoppedPrograms(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("app-version") String appVersion,
      @QueryParam("start-time-seconds") long startTimeSeconds,
      @QueryParam("end-time-seconds") long endTimeSeconds) throws Exception {
    if (ApplicationId.DEFAULT_VERSION.equals(appVersion)) {
      lifecycleService.restart(new ApplicationReference(namespaceId, appId), startTimeSeconds,
          endTimeSeconds);
    } else {
      lifecycleService.restart(new ApplicationId(namespaceId, appId, appVersion), startTimeSeconds,
          endTimeSeconds);
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Returns program runs based on options it returns either currently running or completed or
   * failed. Default it returns all.
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
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);

    long start = parseAndValidate(startTs, 0L, "start");
    long end = parseAndValidate(endTs, Long.MAX_VALUE, "end");
    if (end < start) {
      throw new BadRequestException(
          String.format("Invalid end time %d. It must be greater than the start time %d",
              end, start));
    }
    ProgramRunStatus runStatus = (status == null) ? ProgramRunStatus.ALL :
        ProgramRunStatus.valueOf(status.toUpperCase());

    ProgramReference programReference = new ProgramReference(namespaceId, appName, programType,
        programName);
    List<RunRecord> records = lifecycleService.getAllRunRecords(programReference, runStatus, start,
        end, resultLimit,
        record -> !isTetheredRunRecord(record));
    responder.sendJson(HttpResponseStatus.OK, ProgramHandlerUtil.toJson(records));
  }

  /**
   * Returns program runs of an app version based on options it returns either currently running or
   * completed or failed. Default it returns all.
   */
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runs")
  public void programHistoryVersioned(HttpRequest request, HttpResponder responder,
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
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);

    long start = parseAndValidate(startTs, 0L, "start");
    long end = parseAndValidate(endTs, Long.MAX_VALUE, "end");
    if (end < start) {
      throw new BadRequestException(
          String.format("Invalid end time %d. It must be greater than the start time %d",
              end, start));
    }
    ProgramRunStatus runStatus = (status == null) ? ProgramRunStatus.ALL :
        ProgramRunStatus.valueOf(status.toUpperCase());

    List<RunRecord> records;
    if (ApplicationId.DEFAULT_VERSION.equals(appVersion)) {
      ProgramReference programReference = new ProgramReference(namespaceId, appName, programType,
          programName);
      records = lifecycleService.getRunRecords(programReference, runStatus, start, end,
          resultLimit);
    } else {
      ProgramId program = new ApplicationId(namespaceId, appName, appVersion).program(programType,
          programName);
      records = lifecycleService.getRunRecords(program, runStatus, start, end, resultLimit);
    }
    records = records.stream().filter(record -> !isTetheredRunRecord(record))
        .collect(Collectors.toList());
    responder.sendJson(HttpResponseStatus.OK, ProgramHandlerUtil.toJson(records));
  }

  private long parseAndValidate(String strVal, long defaultVal, String paramName)
      throws BadRequestException {
    if (strVal == null || strVal.isEmpty()) {
      return defaultVal;
    }
    try {
      return Long.parseLong(strVal);
    } catch (NumberFormatException e) {
      throw new BadRequestException(String.format("Invalid vaule %s for %s", strVal, paramName), e);
    }
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
      @PathParam("run-id") String runId) throws NotFoundException, BadRequestException {
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    ProgramReference programRef = new ApplicationReference(namespaceId, appName).program(programType,
        programName);
    RunRecordDetail runRecordMeta = store.getRun(programRef, runId);
    if (runRecordMeta == null) {
      throw new NotFoundException(
          String.format("No run record found for program %s and runID: %s", programRef, runId));
    }

    if (!isTetheredRunRecord(runRecordMeta)) {
      RunRecord runRecord = RunRecord.builder(runRecordMeta).build();
      responder.sendJson(HttpResponseStatus.OK, ProgramHandlerUtil.toJson(runRecord));
      return;
    }
    throw new NotFoundException(runRecordMeta.getProgramRunId());
  }

  /**
   * Returns run record for a particular run of a program of an app version.
   * Deprecated: Run id is sufficient to identify a program run.
   */
  @Deprecated
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runs/{run-id}")
  public void programRunRecordVersioned(HttpRequest request,
      HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("app-version") String appVersion,
      @PathParam("program-type") String type,
      @PathParam("program-name") String programName,
      @PathParam("run-id") String runid)
      throws NotFoundException, BadRequestException {
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    ProgramId progId = new ApplicationId(namespaceId, appName, appVersion).program(programType,
        programName);
    RunRecordDetail runRecordMeta = store.getRun(progId.run(runid));
    if (runRecordMeta != null && !isTetheredRunRecord(runRecordMeta)) {
      RunRecord runRecord = RunRecord.builder(runRecordMeta).build();
      responder.sendJson(HttpResponseStatus.OK, ProgramHandlerUtil.toJson(runRecord));
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
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    ProgramId programId = store.getLatestApp(new ApplicationReference(namespaceId, appName))
        .program(programType, programName);
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
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    ProgramId programId = store.getLatestApp(new ApplicationReference(namespaceId, appName)).program(programType,
        programName);
    saveProgramIdRuntimeArgs(programId, request, responder);
  }

  /**
   * Get runtime args of a program with app version.
   *
   * Deprecated : runtime args are versionless.
   */
  @Deprecated
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runtimeargs")
  public void getProgramRuntimeArgsVersioned(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("app-version") String appVersion,
      @PathParam("program-type") String type,
      @PathParam("program-name") String programName) throws Exception {
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    ProgramId programId = new ApplicationId(namespaceId, appName, appVersion).program(programType,
        programName);
    getProgramIdRuntimeArgs(programId, responder);
  }

  /**
   * Save runtime args of program with app version.
   *
   * Deprecated : runtime args are versionless.
   */
  @Deprecated
  @PUT
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runtimeargs")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void saveProgramRuntimeArgsVersioned(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("app-version") String appVersion,
      @PathParam("program-type") String type,
      @PathParam("program-name") String programName) throws Exception {
    String latestVersion = store.getLatestApp(new ApplicationReference(namespaceId, appName)).getVersion();
    if (!appVersion.equals(latestVersion) && !ApplicationId.DEFAULT_VERSION.equals(appVersion)) {
      throw new BadRequestException(
          "Runtime arguments can only be changed on the latest program version");
    }
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    ProgramId programId = new ApplicationId(namespaceId, appName, appVersion).program(programType,
        programName);
    saveProgramIdRuntimeArgs(programId, request, responder);
  }

  private void getProgramIdRuntimeArgs(ProgramId programId, HttpResponder responder)
      throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
        ProgramHandlerUtil.toJson(lifecycleService.getRuntimeArgs(programId)));
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
    programSpecificationVersioned(request, responder, namespaceId, appName,
        ApplicationId.DEFAULT_VERSION,
        type, programName);
  }

  /*
  * Deprecated : program info fetch is only allowed for the latest version.
  * */
  @Deprecated
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}")
  public void programSpecificationVersioned(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("app-version") String appVersion,
      @PathParam("program-type") String type,
      @PathParam("program-name") String programName) throws Exception {
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    ApplicationId application = new ApplicationId(namespaceId, appName, appVersion);
    ProgramId programId = application.program(programType, programName);
    ProgramSpecification specification;
    if (ApplicationId.DEFAULT_VERSION.equals(appVersion)) {
      ProgramReference programReference = new ProgramReference(namespaceId, appName, programType,
          programName);
      specification = lifecycleService.getProgramSpecification(programReference);
    } else {
      specification = lifecycleService.getProgramSpecification(programId);
    }
    if (specification == null) {
      throw new NotFoundException(programId);
    }
    responder.sendJson(HttpResponseStatus.OK, ProgramHandlerUtil.toJson(specification));
  }

  /**
   * Returns the status for all programs that are passed into the data. The data is an array of JSON
   * objects where each object must contain the following three elements: appId, programType, and
   * programId (flow name, service name, etc.).
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1"},
   * {"appId": "App1", "programType": "Mapreduce", "programId": "MapReduce2"}]
   * </code></pre>
   * </p><p>
   * The response will be an array of JsonObjects each of which will contain the three input
   * parameters as well as 2 fields, "status" which maps to the status of the program and
   * "statusCode" which maps to the status code for the data in that JsonObjects.
   * </p><p>
   * If an error occurs in the input (for the example above, App2 does not exist), then all
   * JsonObjects for which the parameters have a valid status will have the status field but all
   * JsonObjects for which the parameters do not have a valid status will have an error message and
   * statusCode.
   * </p><p>
   * For example, if there is no App2 in the data above, then the response would be 200 OK with
   * following possible data:
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
    List<BatchProgram> batchPrograms = ProgramHandlerUtil.validateAndGetBatchInput(request, BATCH_PROGRAMS_TYPE);
    List<ProgramReference> programs = batchPrograms.stream()
        .map(p -> new ProgramReference(namespaceId, p.getAppId(), p.getProgramType(),
            p.getProgramId()))
        .collect(Collectors.toList());

    Map<ProgramId, ProgramStatus> statuses = lifecycleService.getProgramStatuses(programs);

    Map<ProgramReference, ProgramId> programRefsMap =
        statuses.keySet().stream()
            .collect(Collectors.toMap(ProgramId::getProgramReference, p -> p));

    List<BatchProgramStatus> result = new ArrayList<>(programs.size());
    for (ProgramReference program : programs) {
      ProgramId programId = programRefsMap.get(program);
      if (programId == null) {
        result.add(
            new BatchProgramStatus(program.getBatchProgram(), HttpResponseStatus.NOT_FOUND.code(),
                new NotFoundException(program).getMessage(), null));
      } else {
        result.add(new BatchProgramStatus(program.getBatchProgram(), HttpResponseStatus.OK.code(),
            null, statuses.get(programId).name()));
      }
    }
    responder.sendJson(HttpResponseStatus.OK, ProgramHandlerUtil.toJson(result));
  }

  /**
   * Stops all programs that are passed into the data. The data is an array of JSON objects where
   * each object must contain the following three elements: appId, programType, and programId (flow
   * name, service name, etc.).
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1"},
   * {"appId": "App1", "programType": "Mapreduce", "programId": "MapReduce2"}]
   * </code></pre>
   * </p><p>
   * The response will be an array of JsonObjects each of which will contain the three input
   * parameters as well as a "statusCode" field which maps to the status code for the data in that
   * JsonObjects.
   * </p><p>
   * If an error occurs in the input (for the example above, App2 does not exist), then all
   * JsonObjects for which the parameters have a valid status will have the status field but all
   * JsonObjects for which the parameters do not have a valid status will have an error message and
   * statusCode.
   * </p><p>
   * For example, if there is no App2 in the data above, then the response would be 200 OK with
   * following possible data:
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
      @PathParam("namespace-id") String namespaceId) throws Exception {
    List<BatchProgram> programs = ProgramHandlerUtil.validateAndGetBatchInput(request, BATCH_PROGRAMS_TYPE);

    List<BatchProgramResult> issuedStops = new ArrayList<>(programs.size());
    for (final BatchProgram program : programs) {
      ApplicationId applicationId = store.getLatestApp(new ApplicationReference(namespaceId, program.getAppId()));
      ProgramId programId = new ProgramId(applicationId, program.getProgramType(),
          program.getProgramId());
      try {
        Collection<ProgramRunId> stoppedRuns = lifecycleService.issueStop(programId, null, null);
        for (ProgramRunId stoppedRun : stoppedRuns) {
          issuedStops.add(new BatchProgramResult(program, HttpResponseStatus.OK.code(), null,
              stoppedRun.getRun()));
        }
      } catch (NotFoundException e) {
        issuedStops.add(
            new BatchProgramResult(program, HttpResponseStatus.NOT_FOUND.code(), e.getMessage()));
      } catch (BadRequestException e) {
        issuedStops.add(
            new BatchProgramResult(program, HttpResponseStatus.BAD_REQUEST.code(), e.getMessage()));
      }
    }

    responder.sendJson(HttpResponseStatus.OK, ProgramHandlerUtil.toJson(issuedStops));
  }

  /**
   * Starts all programs that are passed into the data. The data is an array of JSON objects where
   * each object must contain the following three elements: appId, programType, and programId (flow
   * name, service name, etc.). In additional, each object can contain an optional runtimeargs
   * element, which is a map of arguments to start the program with.
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1"},
   * {"appId": "App1", "programType": "Mapreduce", "programId": "MapReduce2", "runtimeargs":{"arg1":"val1"}}]
   * </code></pre>
   * </p><p>
   * The response will be an array of JsonObjects each of which will contain the three input
   * parameters as well as a "statusCode" field which maps to the status code for the data in that
   * JsonObjects.
   * </p><p>
   * If an error occurs in the input (for the example above, App2 does not exist), then all
   * JsonObjects for which the parameters have a valid status will have the status field but all
   * JsonObjects for which the parameters do not have a valid status will have an error message and
   * statusCode.
   * </p><p>
   * For example, if there is no App2 in the data above, then the response would be 200 OK with
   * following possible data:
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

    List<BatchProgramStart> programs = ProgramHandlerUtil.validateAndGetBatchInput(request, BATCH_STARTS_TYPE);

    List<BatchProgramResult> output = new ArrayList<>(programs.size());
    for (BatchProgramStart program : programs) {
      ApplicationId applicationId = store.getLatestApp(new ApplicationReference(namespaceId, program.getAppId()));
      ProgramId programId = new ProgramId(applicationId, program.getProgramType(),
          program.getProgramId());
      try {
        String runId = lifecycleService.run(programId, program.getRuntimeargs(), false).getId();
        output.add(new BatchProgramResult(program, HttpResponseStatus.OK.code(), null, runId));
      } catch (NotFoundException e) {
        output.add(
            new BatchProgramResult(program, HttpResponseStatus.NOT_FOUND.code(), e.getMessage()));
      } catch (BadRequestException e) {
        output.add(
            new BatchProgramResult(program, HttpResponseStatus.BAD_REQUEST.code(), e.getMessage()));
      } catch (ConflictException e) {
        output.add(
            new BatchProgramResult(program, HttpResponseStatus.CONFLICT.code(), e.getMessage()));
      }
    }
    responder.sendJson(HttpResponseStatus.OK, ProgramHandlerUtil.toJson(output));
  }

  /**
   * Returns the run counts for all program runnables that are passed into the data. The data is an
   * array of Json objects where each object must contain the following three elements: appId,
   * programType, and programId. The max number of programs in the request is 100.
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1"},
   *  {"appId": "App1", "programType": "Workflow", "programId": "testWorkflow"},
   *  {"appId": "App2", "programType": "Workflow", "programId": "DataPipelineWorkflow"}]
   * </code></pre>
   * </p><p>
   * </p><p>
   * The response will be an array of JsonObjects each of which will contain the three input
   * parameters as well as 2 fields, "runCount" which maps to the count of the program and
   * "statusCode" which maps to the status code for the data in that JsonObjects.
   * </p><p>
   * If an error occurs in the input (for the example above, workflow in app1 does not exist), then
   * all JsonObjects for which the parameters have a valid status will have the count field but all
   * JsonObjects for which the parameters do not have a valid status will have an error message and
   * statusCode.
   * </p><p>
   * For example, if there is no workflow in App1 in the data above, then the response would be 200
   * OK with following possible data:
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
    List<BatchProgram> programs = ProgramHandlerUtil.validateAndGetBatchInput(request, BATCH_PROGRAMS_TYPE);
    if (programs.size() > 100) {
      throw new BadRequestException(
          String.format("%d programs found in the request, the maximum number "
              + "supported is 100", programs.size()));
    }

    List<ProgramReference> programRefs = programs.stream()
        .map(p -> new ProgramReference(namespaceId, p.getAppId(), p.getProgramType(),
            p.getProgramId()))
        .collect(Collectors.toList());

    List<BatchProgramCount> counts = new ArrayList<>(programs.size());
    for (RunCountResult runCountResult : lifecycleService.getProgramTotalRunCounts(programRefs)) {
      ProgramReference programReference = runCountResult.getProgramReference();
      Exception exception = runCountResult.getException();
      if (exception == null) {
        counts.add(new BatchProgramCount(programReference, HttpResponseStatus.OK.code(), null,
            runCountResult.getCount()));
      } else if (exception instanceof NotFoundException) {
        counts.add(new BatchProgramCount(programReference, HttpResponseStatus.NOT_FOUND.code(),
            exception.getMessage(), null));
      } else if (exception instanceof UnauthorizedException) {
        counts.add(new BatchProgramCount(programReference, HttpResponseStatus.FORBIDDEN.code(),
            exception.getMessage(), null));
      } else {
        counts.add(
            new BatchProgramCount(programReference, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                exception.getMessage(), null));
      }
    }
    responder.sendJson(HttpResponseStatus.OK, ProgramHandlerUtil.toJson(counts));
  }

  /**
   * Returns the latest runs for all program runnables that are passed into the data. The data is an
   * array of Json objects where each object must contain the following three elements: appId,
   * programType, and programId.
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Service", "programId": "Service1"},
   *  {"appId": "App1", "programType": "Workflow", "programId": "testWorkflow"},
   *  {"appId": "App2", "programType": "Workflow", "programId": "DataPipelineWorkflow"}]
   * </code></pre>
   * </p><p>
   * </p><p>
   * The response will be an array of JsonObjects each of which will contain the three input
   * parameters as well as 2 fields, "runs" which is a list of the latest run records and
   * "statusCode" which maps to the status code for the data in that JsonObjects.
   * </p><p>
   * If an error occurs in the input (for the example above, workflow in app1 does not exist), then
   * all JsonObjects for which the parameters have a valid status will have the count field but all
   * JsonObjects for which the parameters do not have a valid status will have an error message and
   * statusCode.
   * </p><p>
   * For example, if there is no workflow in App1 in the data above, then the response would be 200
   * OK with following possible data:
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
    List<BatchProgram> programs = ProgramHandlerUtil.validateAndGetBatchInput(request, BATCH_PROGRAMS_TYPE);
    List<ProgramReference> programRefs =
        programs.stream().map(
                p -> new ProgramReference(namespaceId, p.getAppId(), p.getProgramType(),
                    p.getProgramId()))
            .collect(Collectors.toList());

    List<BatchProgramHistory> response = new ArrayList<>(programs.size());
    List<ProgramHistory> result = lifecycleService.getRunRecords(programRefs, ProgramRunStatus.ALL,
        0,
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
        response.add(
            new BatchProgramHistory(batchProgram, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                exception.getMessage(), Collections.emptyList()));
      }
    }
    responder.sendJson(HttpResponseStatus.OK, ProgramHandlerUtil.toJson(response));
  }

  /**
   * Returns the count of the given program runs.
   */
  @GET
  @Path("/apps/{app-name}/{program-type}/{program-name}/runcount")
  public void getProgramRunCount(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("program-type") String type,
      @PathParam("program-name") String programName) throws Exception {
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    ProgramReference programReference = new ProgramReference(namespaceId, appName, programType,
        programName);
    long runCount = lifecycleService.getProgramTotalRunCount(programReference);
    responder.sendJson(HttpResponseStatus.OK, ProgramHandlerUtil.toJson(runCount));
  }

  /**
   * Returns the count of the given program runs.
   */
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/runcount")
  public void getProgramRunCountVersioned(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("app-version") String appVersion,
      @PathParam("program-type") String type,
      @PathParam("program-name") String programName) throws Exception {
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    long runCount;
    if (ApplicationId.DEFAULT_VERSION.equals(appVersion)) {
      ProgramReference programReference = new ProgramReference(namespaceId, appName, programType,
          programName);
      runCount = lifecycleService.getProgramRunCount(programReference);
    } else {
      ProgramId programId = new ApplicationId(namespaceId, appName, appVersion).program(programType,
          programName);
      runCount = lifecycleService.getProgramRunCount(programId);
    }
    responder.sendJson(HttpResponseStatus.OK, ProgramHandlerUtil.toJson(runCount));
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
        ProgramHandlerUtil.toJson(
            lifecycleService.list(NamespaceHelper.validateNamespace(namespaceQueryAdmin,namespaceId),
                ProgramType.MAPREDUCE)));
  }

  /**
   * Returns a list of spark jobs associated with a namespace.
   */
  @GET
  @Path("/spark")
  public void getAllSpark(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
        ProgramHandlerUtil.toJson(
            lifecycleService.list(NamespaceHelper.validateNamespace(namespaceQueryAdmin,namespaceId),
                ProgramType.SPARK)));
  }

  /**
   * Returns a list of workflows associated with a namespace.
   */
  @GET
  @Path("/workflows")
  public void getAllWorkflows(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
        ProgramHandlerUtil.toJson(
            lifecycleService.list(NamespaceHelper.validateNamespace(namespaceQueryAdmin,namespaceId),
                ProgramType.WORKFLOW)));
  }

  /**
   * Returns a list of services associated with a namespace.
   */
  @GET
  @Path("/services")
  public void getAllServices(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
        ProgramHandlerUtil.toJson(
            lifecycleService.list(NamespaceHelper.validateNamespace(namespaceQueryAdmin,namespaceId),
                ProgramType.SERVICE)));
  }

  @GET
  @Path("/workers")
  public void getAllWorkers(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
        ProgramHandlerUtil.toJson(
            lifecycleService.list(NamespaceHelper.validateNamespace(namespaceQueryAdmin,namespaceId),
                ProgramType.WORKER)));
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
    getServiceAvailabilityVersioned(request, responder, namespaceId, appName,
        ApplicationId.DEFAULT_VERSION,
        serviceType, programName);
  }

  /**
   * Return the availability (i.e. discoverable registration) status of a service.
   *
   * Deprecated : Only allow checking availability of the service corresponding to the latest app version.
   */
  @Deprecated
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{service-type}/{program-name}/available")
  public void getServiceAvailabilityVersioned(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("app-version") String appVersion,
      @PathParam("service-type") String serviceType,
      @PathParam("program-name") String programName) throws Exception {
    // Currently, we only support services and sparks as the service-type
    ProgramType programType = ProgramType.valueOfCategoryName(serviceType, BadRequestException::new);
    if (!ServiceDiscoverable.getUserServiceTypes().contains(programType)) {
      throw new BadRequestException(
          "Only service or spark is support for service availability check");
    }

    ProgramId programId = new ProgramId(new ApplicationId(namespaceId, appName, appVersion),
        programType, programName);
    ProgramStatus status;
    if (ApplicationId.DEFAULT_VERSION.equals(appVersion)) {
      ProgramReference programReference = new ProgramReference(namespaceId, appName, programType,
          programName);
      status = lifecycleService.getProgramStatus(programReference);
    } else {
      status = lifecycleService.getProgramStatus(programId);
    }
    if (status == ProgramStatus.STOPPED) {
      throw new ServiceUnavailableException(programId.toString(),
          "Service is stopped. Please start it.");
    }

    // Construct discoverable name and return 200 OK if discoverable is present. If not return 503.
    String discoverableName = ServiceDiscoverable.getName(programId);

    // TODO: CDAP-12959 - Should use the UserServiceEndpointStrategy and discover based on the version
    // and have appVersion nullable for the non versioned endpoint
    EndpointStrategy strategy = new RandomEndpointStrategy(
        () -> discoveryServiceClient.discover(discoverableName));
    if (strategy.pick(300L, TimeUnit.MILLISECONDS) == null) {
      LOG.trace("Discoverable endpoint {} not found", discoverableName);
      throw new ServiceUnavailableException(programId.toString(),
          "Service is running but not accepting requests at this time.");
    }

    responder.sendString(HttpResponseStatus.OK, "Service is available to accept requests.");
  }

  private boolean isDebugAllowed(ProgramType programType) {
    return EnumSet.of(ProgramType.SERVICE, ProgramType.WORKER).contains(programType);
  }

  /**
   * Used to filter out RunRecords initiated by a tethered instance
   */
  private boolean isTetheredRunRecord(RunRecord runRecord) {
    return runRecord.getPeerName() != null;
  }
}
