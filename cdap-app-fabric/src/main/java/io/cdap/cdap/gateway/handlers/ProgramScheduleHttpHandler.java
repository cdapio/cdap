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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.gateway.handlers.util.ProgramHandlerUtil;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.internal.app.runtime.schedule.SchedulerException;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.BatchProgram;
import io.cdap.cdap.proto.BatchProgramSchedule;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.scheduler.ProgramScheduleService;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
 * {@link io.cdap.http.HttpHandler} to manage program schedule for v3 REST APIs.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class ProgramScheduleHttpHandler extends AbstractAppFabricHttpHandler {

  private static final Type BATCH_PROGRAMS_TYPE = new TypeToken<List<BatchProgram>>() {
  }.getType();
  private static final List<Constraint> NO_CONSTRAINTS = Collections.emptyList();

  private final ProgramScheduleService programScheduleService;
  private final ProgramLifecycleService lifecycleService;
  private final Store store;

  /**
   * Constructor for ProgramScheduleHttpHandler.
   * @param programScheduleService ProgramScheduleService
   * @param lifecycleService ProgramLifecycleService
   * @param store Store
   */
  @Inject
  public ProgramScheduleHttpHandler(ProgramScheduleService programScheduleService,
      ProgramLifecycleService lifecycleService,
      Store store) {
    this.programScheduleService = programScheduleService;
    this.lifecycleService = lifecycleService;
    this.store = store;
  }

  /**
   * Returns status of a schedule.
   */
  @GET
  @Path("/apps/{app-id}/schedules/{schedule-name}/status")
  public void getStatus(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("schedule-name") String scheduleName) throws Exception {
    getStatusVersioned(request, responder, namespaceId, appId, ApplicationId.DEFAULT_VERSION, scheduleName);
  }

  /**
   * Returns status of a schedule.
   * Deprecated: Only schedule info retrieval for the latest app version (active app).
   */
  @Deprecated
  @GET
  @Path("/apps/{app-id}/versions/{version-id}/schedules/{schedule-name}/status")
  public void getStatusVersioned(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("version-id") String versionId,
      @PathParam("schedule-name") String scheduleName) throws Exception {
    ApplicationReference appReference = new ApplicationReference(namespaceId, appId);
    JsonObject json = new JsonObject();
    // ScheduleId is versionless (always "-SNAPSHOT")
    ScheduleId scheduleId = appReference.app(ApplicationId.DEFAULT_VERSION).schedule(scheduleName);
    ApplicationSpecification appSpec = ApplicationId.DEFAULT_VERSION.equals(versionId)
        ? Optional.ofNullable(store.getLatest(appReference)).map(ApplicationMeta::getSpec)
        .orElse(null)
        : store.getApplication(appReference.app(versionId));
    if (appSpec == null) {
      throw new NotFoundException(appReference);
    }
    json.addProperty("status", programScheduleService.getStatus(scheduleId).toString());
    responder.sendJson(HttpResponseStatus.OK, json.toString());
  }

  /**
   * Perform an action on the latest version of a schedule.
   */
  @POST
  @Path("/apps/{app-id}/schedules/{schedule-name}/{action}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void performAction(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("schedule-name") String scheduleName,
      @PathParam("action") String action) throws Exception {
    doPerformAction(responder, namespaceId, appId, ApplicationId.DEFAULT_VERSION, scheduleName, action);
  }

  /**
   * Perform an action on the schedule of a specific {@code appVersion}.
   */
  @POST
  @Path("/apps/{app-id}/versions/{app-version}/schedules/{schedule-name}/{action}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void performActionVersioned(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId,
      @PathParam("app-version") String appVersion,
      @PathParam("schedule-name") String scheduleName,
      @PathParam("action") String action) throws Exception {
    doPerformAction(responder, namespaceId, appId, appVersion, scheduleName, action);
  }

  /**
   * Perform an action on the schedule of a specific {@code appVersion}.
   *
   * @param namespaceId namespace of the program
   * @param appId appId of the program
   * @param appVersion app version of the program
   * @param programId programId of the program
   * @param action action to be performed. The value can be one of enable/disable/suspend/resume
   *     for schedule.
   *
   * @throws NotFoundException if action is an unknown action
   * @throws BadRequestException if type is schedule and action is not any of
   *     enable/disable/suspend/resume
   * @throws BadRequestException if action is not supported.
   */
  private void doPerformAction(HttpResponder responder, String namespaceId,
      String appId, String appVersion, String programId, String action) throws Exception {
    ApplicationId applicationId = new ApplicationId(namespaceId, appId, appVersion);
    ScheduleId scheduleId = applicationId.schedule(programId);
    if (action.equals("disable") || action.equals("suspend")) {
      programScheduleService.suspend(scheduleId);
    } else if (action.equals("enable") || action.equals("resume")) {
      programScheduleService.resume(scheduleId);
    } else {
      throw new BadRequestException(
          "Action for schedules may only be 'enable', 'disable', 'suspend', or 'resume' but is'"
              + action + "'");
    }
    responder.sendJson(HttpResponseStatus.OK, "OK");
  }

  /**
   * Update schedules which were suspended between startTimeMillis and endTimeMillis.
   *
   * @param startTimeMillis lower bound in millis of the update time for schedules (inclusive)
   * @param endTimeMillis upper bound in millis of the update time for schedules (exclusive)
   */
  @PUT
  @Path("schedules/re-enable")
  public void reEnableSuspendedSchedules(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @QueryParam("start-time-millis") long startTimeMillis,
      @QueryParam("end-time-millis") long endTimeMillis) throws Exception {
    programScheduleService.reEnableSchedules(new NamespaceId(namespaceId), startTimeMillis,
        endTimeMillis);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Get schedules containing {@link ProgramStatusTrigger} filtered by triggering program, and
   * optionally by triggering program statuses or schedule status.
   *
   * @param triggerNamespaceId namespace of the triggering program in {@link
   *     ProgramStatusTrigger}
   * @param triggerAppName application name of the triggering program in {@link
   *     ProgramStatusTrigger}
   * @param triggerAppVersion application version of the triggering program in {@link
   *     ProgramStatusTrigger}
   * @param triggerProgramType program type of the triggering program in {@link
   *     ProgramStatusTrigger}
   * @param triggerProgramName program name of the triggering program in {@link
   *     ProgramStatusTrigger}
   * @param triggerProgramStatuses comma separated {@link ProgramStatus} in {@link
   *     ProgramStatusTrigger}. Schedules with {@link ProgramStatusTrigger} triggered by none of the
   *     {@link ProgramStatus} in triggerProgramStatuses will be filtered out. If not specified,
   *     schedules will be returned regardless of triggering program status.
   * @param scheduleStatus status of the schedule. Can only be one of "SCHEDULED" or
   *     "SUSPENDED". If specified, only schedules with matching status will be returned.
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

    ProgramType programType = ProgramType.valueOfCategoryName(triggerProgramType, BadRequestException::new);
    ProgramScheduleStatus programScheduleStatus;
    try {
      programScheduleStatus =
          scheduleStatus == null ? null : ProgramScheduleStatus.valueOf(scheduleStatus);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(
          String.format("Invalid schedule status '%s'. Must be one of %s.",
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
        throw new BadRequestException(
            String.format("Unable to parse program statuses '%s'. Must be comma separated "
                    + "valid ProgramStatus names such as COMPLETED, FAILED, KILLED.",
                triggerProgramStatuses), e);
      }
    } else {
      // Query for schedules with all the statuses if no query status is specified
      Collections.addAll(queryProgramStatuses, io.cdap.cdap.api.ProgramStatus.values());
    }

    List<ScheduleDetail> details = programScheduleService.findTriggeredBy(triggerProgramId,
            queryProgramStatuses)
        .stream()
        .filter(record -> programScheduleStatus == null || record.getMeta().getStatus()
            .equals(programScheduleStatus))
        .map(ProgramScheduleRecord::toScheduleDetail)
        .collect(Collectors.toList());
    responder.sendJson(HttpResponseStatus.OK,
        ProgramHandlerUtil.toJson(details, Schedulers.SCHEDULE_DETAILS_TYPE));
  }

  /**
   * Gets the schedule for the given {@code appName}.
   */
  @GET
  @Path("apps/{app-name}/schedules/{schedule-name}")
  public void getSchedule(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("schedule-name") String scheduleName) throws Exception {
    doGetSchedule(responder, namespaceId, appName, scheduleName);
  }

  /*
   * Deprecated: Schedules are versionless.
   * */
  @Deprecated
  @GET
  @Path("apps/{app-name}/versions/{app-version}/schedules/{schedule-name}")
  public void getScheduleVersioned(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("app-version") String appVersion,
      @PathParam("schedule-name") String scheduleName) throws Exception {
    doGetSchedule(responder, namespaceId, appName, scheduleName);
  }

  private void doGetSchedule(HttpResponder responder, String namespace, String app,
      String scheduleName)
      throws Exception {
    ScheduleId scheduleId = new ApplicationId(namespace, app).schedule(scheduleName);
    ProgramScheduleRecord record = programScheduleService.getRecord(scheduleId);
    ScheduleDetail detail = record.toScheduleDetail();
    responder.sendJson(HttpResponseStatus.OK, ProgramHandlerUtil.toJson(detail, ScheduleDetail.class));
  }

  /**
   * See {@link #getAllSchedulesVersioned(HttpRequest, HttpResponder, String, String, String,
   * String, String)}.
   */
  @GET
  @Path("apps/{app-name}/schedules")
  public void getAllSchedules(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @QueryParam("trigger-type") String triggerType,
      @QueryParam("schedule-status") String scheduleStatus) throws Exception {
    doGetSchedules(responder, new NamespaceId(namespaceId).app(appName), null, triggerType,
        scheduleStatus);
  }

  /**
   * Get schedules in a given application, optionally filtered by the given {@link
   *  io.cdap.cdap.proto.ProtoTrigger.Type}.
   * Deprecated : Schedules are versionless.
   * @param namespaceId namespace of the application to get schedules from
   * @param appName name of the application to get schedules from
   * @param appVersion version of the application to get schedules from
   * @param triggerType trigger type of returned schedules. If not specified, all schedules are
   *     returned regardless of trigger type
   * @param scheduleStatus the status of the schedule, must be values in {@link
   *     ProgramScheduleStatus}.
   *
   */
  @Deprecated
  @GET
  @Path("apps/{app-name}/versions/{app-version}/schedules")
  public void getAllSchedulesVersioned(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("app-version") String appVersion,
      @QueryParam("trigger-type") String triggerType,
      @QueryParam("schedule-status") String scheduleStatus) throws Exception {
    doGetSchedules(responder, new NamespaceId(namespaceId).app(appName), null, triggerType,
        scheduleStatus);
  }

  /**
   * Get program schedules.
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
    getProgramSchedulesVersioned(request, responder, namespace, application,
        ApplicationId.DEFAULT_VERSION,
        type, program, triggerType, scheduleStatus);
  }

  /**
   * Get Workflow schedules.
   * Deprecated : Schedules are versionless.
   */
  @Deprecated
  @GET
  @Path("/apps/{app-name}/versions/{app-version}/{program-type}/{program-name}/schedules")
  public void getProgramSchedulesVersioned(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace,
      @PathParam("app-name") String application,
      @PathParam("app-version") String appVersion,
      @PathParam("program-type") String type,
      @PathParam("program-name") String program,
      @QueryParam("trigger-type") String triggerType,
      @QueryParam("schedule-status") String scheduleStatus) throws Exception {
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    if (programType.getSchedulableType() == null) {
      throw new BadRequestException("Program type " + programType + " cannot have schedule");
    }

    ProgramId programId = new ApplicationId(namespace, application).program(programType, program);
    doGetSchedules(responder, new NamespaceId(namespace).app(application), programId,
        triggerType, scheduleStatus);
  }

  private void doGetSchedules(HttpResponder responder, ApplicationId applicationId,
      @Nullable ProgramId programId, @Nullable String triggerTypeStr,
      @Nullable String statusStr) throws Exception {
    ApplicationSpecification appSpec = Optional.ofNullable(
            store.getLatest(applicationId.getAppReference()))
        .map(ApplicationMeta::getSpec)
        .orElse(null);
    if (appSpec == null) {
      throw new NotFoundException(applicationId);
    }
    ProgramScheduleStatus status;
    try {
      status = statusStr == null ? null : ProgramScheduleStatus.valueOf(statusStr);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(
          String.format("Invalid schedule status '%s'. Must be one of %s.",
              statusStr, Joiner.on(',').join(ProgramScheduleStatus.values())), e);
    }

    ProtoTrigger.Type triggerType;
    try {
      triggerType =
          triggerTypeStr == null ? null : ProtoTrigger.Type.valueOfCategoryName(triggerTypeStr);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
    }

    Predicate<ProgramScheduleRecord> predicate = record -> true;
    if (status != null) {
      predicate = predicate.and(record -> record.getMeta().getStatus().equals(status));
    }
    if (triggerType != null) {
      predicate = predicate.and(
          record -> record.getSchedule().getTrigger().getType().equals(triggerType));
    }

    Collection<ProgramScheduleRecord> schedules;
    if (programId != null) {
      if (!appSpec.getProgramsByType(programId.getType().getApiProgramType())
          .contains(programId.getProgram())) {
        throw new NotFoundException(programId);
      }
      schedules = programScheduleService.list(programId, predicate);
    } else {
      schedules = programScheduleService.list(applicationId, predicate);
    }

    List<ScheduleDetail> details = schedules.stream()
        .map(ProgramScheduleRecord::toScheduleDetail)
        .collect(Collectors.toList());
    responder.sendJson(HttpResponseStatus.OK,
        ProgramHandlerUtil.toJson(details, Schedulers.SCHEDULE_DETAILS_TYPE));
  }

  /**
   * Returns the previous runtime when the scheduled program ran.
   */
  @GET
  @Path("/apps/{app-name}/{program-type}/{program-name}/previousruntime")
  public void getPreviousScheduledRunTime(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("program-type") String type,
      @PathParam("program-name") String program) throws Exception {
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    ApplicationId appId = store.getLatestApp(new ApplicationReference(namespaceId, appName));
    handleScheduleRunTime(responder, appId.program(programType, program), true);
  }

  /**
   * Returns next scheduled runtime of a workflow.
   */
  @GET
  @Path("/apps/{app-name}/{program-type}/{program-name}/nextruntime")
  public void getNextScheduledRunTime(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("program-type") String type,
      @PathParam("program-name") String program) throws Exception {
    ProgramType programType = ProgramType.valueOfCategoryName(type, BadRequestException::new);
    ApplicationId appId = store.getLatestApp(new ApplicationReference(namespaceId, appName));
    handleScheduleRunTime(responder, appId.program(programType, program), false);
  }

  private void handleScheduleRunTime(HttpResponder responder, ProgramId programId,
      boolean previousRuntimeRequested) throws Exception {
    try {
      lifecycleService.ensureProgramExists(programId);
      responder.sendJson(HttpResponseStatus.OK,
          ProgramHandlerUtil.toJson(getScheduledRunTimes(programId, previousRuntimeRequested)));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    }
  }

  /**
   * Adds a schedule for a given {@code appName}.
   */
  @PUT
  @Path("apps/{app-name}/schedules/{schedule-name}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addSchedule(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("schedule-name") String scheduleName)
      throws Exception {
    doAddSchedule(request, responder, namespaceId, appName, scheduleName);
  }

  /*
   * Deprecated : Schedules are versionless.
   */
  @Deprecated
  @PUT
  @Path("apps/{app-name}/versions/{app-version}/schedules/{schedule-name}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addScheduleVersioned(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("app-version") String appVersion,
      @PathParam("schedule-name") String scheduleName)
      throws Exception {
    // Schedules are versionless (all are "-SNAPSHOT" version)
    doAddSchedule(request, responder, namespaceId, appName, scheduleName);
  }

  private void doAddSchedule(FullHttpRequest request, HttpResponder responder, String namespace,
      String appName,
      String scheduleName) throws Exception {

    final ApplicationId applicationId = new ApplicationId(namespace, appName);
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
    ProgramType programType = ProgramType.valueOfSchedulableType(
        scheduleFromRequest.getProgram().getProgramType());
    String programName = scheduleFromRequest.getProgram().getProgramName();
    ProgramId programId = applicationId.program(programType, programName);

    // Schedules are versionless
    lifecycleService.ensureLatestProgramExists(programId.getProgramReference());

    String description = Objects.firstNonNull(scheduleFromRequest.getDescription(), "");
    Map<String, String> properties = Objects.firstNonNull(scheduleFromRequest.getProperties(),
        Collections.emptyMap());
    List<? extends Constraint> constraints = Objects.firstNonNull(
        scheduleFromRequest.getConstraints(), NO_CONSTRAINTS);
    long timeoutMillis =
        Objects.firstNonNull(scheduleFromRequest.getTimeoutMillis(),
            Schedulers.JOB_QUEUE_TIMEOUT_MILLIS);
    ProgramSchedule schedule = new ProgramSchedule(scheduleName, description, programId, properties,
        scheduleFromRequest.getTrigger(), constraints, timeoutMillis);
    programScheduleService.add(schedule);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Updates the schedule for a given {@code appName}.
   */
  @POST
  @Path("apps/{app-name}/schedules/{schedule-name}/update")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void updateSchedule(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("schedule-name") String scheduleName) throws Exception {
    doUpdateSchedule(request, responder, namespaceId, appName, scheduleName);
  }

  /*
   * Deprecated : Schedules are versionless.
   * */
  @Deprecated
  @POST
  @Path("apps/{app-name}/versions/{app-version}/schedules/{schedule-name}/update")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void updateScheduleVersioned(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("app-version") String appVersion,
      @PathParam("schedule-name") String scheduleName) throws Exception {
    doUpdateSchedule(request, responder, namespaceId, appName, scheduleName);
  }

  private void doUpdateSchedule(FullHttpRequest request, HttpResponder responder,
      String namespaceId, String appId,
      String scheduleName) throws Exception {

    ScheduleId scheduleId = new ApplicationId(namespaceId, appId).schedule(scheduleName);
    ScheduleDetail scheduleDetail = readScheduleDetailBody(request, scheduleName);

    programScheduleService.update(scheduleId, scheduleDetail);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private ScheduleDetail readScheduleDetailBody(FullHttpRequest request, String scheduleName)
      throws BadRequestException, IOException {

    JsonElement json;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()),
        Charsets.UTF_8)) {
      // The schedule spec in the request body does not contain the program information
      json = ProgramHandlerUtil.fromJson(reader, JsonElement.class);
    } catch (IOException e) {
      throw new IOException("Error reading request body", e);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Request body is invalid json: " + e.getMessage());
    }
    if (!json.isJsonObject()) {
      throw new BadRequestException(
          "Expected a json object in the request body but received " + ProgramHandlerUtil.toJson(json));
    }
    ScheduleDetail scheduleDetail;
    try {
      scheduleDetail = ProgramHandlerUtil.fromJson(json, ScheduleDetail.class);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException(
          "Error parsing request body as a schedule specification: " + e.getMessage());
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
    doDeleteSchedule(responder, namespaceId, appName, scheduleName);
  }

  /*
   * Deprecated : Schedules are versionless.
   * */
  @Deprecated
  @DELETE
  @Path("apps/{app-name}/versions/{app-version}/schedules/{schedule-name}")
  public void deleteScheduleVersioned(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-name") String appName,
      @PathParam("app-version") String appVersion,
      @PathParam("schedule-name") String scheduleName) throws Exception {
    doDeleteSchedule(responder, namespaceId, appName, scheduleName);
  }

  private void doDeleteSchedule(HttpResponder responder, String namespaceId, String appName,
      String scheduleName) throws Exception {
    ScheduleId scheduleId = new ApplicationId(namespaceId, appName).schedule(scheduleName);
    programScheduleService.delete(scheduleId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Returns the previous scheduled run time for all programs that are passed into the data. The
   * data is an array of JSON objects where each object must contain the following three elements:
   * appId, programType, and programId (flow name, service name, etc.).
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Workflow", "programId": "WF1"},
   * {"appId": "App1", "programType": "Workflow", "programId": "WF2"}]
   * </code></pre>
   * </p>
   * <p>The response will be an array of JsonObjects each of which will contain the three input
   * parameters as well as a "schedules" field, which is a list of {@link ScheduledRuntime} object.
   * </p>
   * <p>If an error occurs in the input (for the example above, App1 does not exist), then all
   * JsonObjects for which the parameters have a valid status will have the status field but all
   * JsonObjects for which the parameters do not have a valid status will have an error message and
   * statusCode.
   */
  @POST
  @Path("/previousruntime")
  public void batchPreviousRunTimes(FullHttpRequest request,
      HttpResponder responder,
      @PathParam("namespace-id") String namespaceId) throws Exception {
    List<BatchProgram> batchPrograms = ProgramHandlerUtil.validateAndGetBatchInput(request, BATCH_PROGRAMS_TYPE);
    responder.sendJson(HttpResponseStatus.OK,
        ProgramHandlerUtil.toJson(batchRunTimes(namespaceId, batchPrograms, true)));
  }

  /**
   * Returns the next scheduled run time for all programs that are passed into the data. The data is
   * an array of JSON objects where each object must contain the following three elements: appId,
   * programType, and programId (flow name, service name, etc.).
   * <p>
   * Example input:
   * <pre><code>
   * [{"appId": "App1", "programType": "Workflow", "programId": "WF1"},
   * {"appId": "App1", "programType": "Workflow", "programId": "WF2"}]
   * </code></pre>
   * </p>
   * <p>The response will be an array of JsonObjects each of which will contain the three input
   * parameters as well as a "schedules" field, which is a list of {@link ScheduledRuntime} object.
   * </p>
   * <p>If an error occurs in the input (for the example above, App1 does not exist), then all
   * JsonObjects for which the parameters have a valid status will have the status field but all
   * JsonObjects for which the parameters do not have a valid status will have an error message and
   * statusCode.
   */
  @POST
  @Path("/nextruntime")
  public void batchNextRunTimes(FullHttpRequest request,
      HttpResponder responder,
      @PathParam("namespace-id") String namespaceId) throws Exception {
    List<BatchProgram> batchPrograms = ProgramHandlerUtil.validateAndGetBatchInput(request, BATCH_PROGRAMS_TYPE);
    responder.sendJson(HttpResponseStatus.OK,
        ProgramHandlerUtil.toJson(batchRunTimes(namespaceId, batchPrograms, false)));
  }

  /**
   * Fetches scheduled run times for a set of programs.
   *
   * @param namespace namespace of the programs
   * @param programs the list of programs to fetch scheduled run times
   * @param previous {@code true} to get the previous scheduled times; {@code false} to get the
   *     next scheduled times
   * @return a list of {@link BatchProgramSchedule} containing the result
   * @throws SchedulerException if failed to fetch schedules
   */
  private List<BatchProgramSchedule> batchRunTimes(String namespace,
      Collection<? extends BatchProgram> programs,
      boolean previous) throws Exception {
    List<ProgramReference> programReferences = programs.stream()
        .map(p -> new ProgramReference(namespace, p.getAppId(), p.getProgramType(),
            p.getProgramId()))
        .collect(Collectors.toList());
    Map<ProgramReference, ProgramId> programMap = store.getPrograms(programReferences);

    List<BatchProgramSchedule> result = new ArrayList<>();
    for (ProgramReference programReference : programReferences) {
      if (programMap.containsKey(programReference)) {
        ProgramId programId = programMap.get(programReference);
        result.add(new BatchProgramSchedule(programId, HttpResponseStatus.OK.code(), null,
            getScheduledRunTimes(programId, previous)));
      } else {
        result.add(new BatchProgramSchedule(programReference, HttpResponseStatus.NOT_FOUND.code(),
            new NotFoundException(programReference).getMessage(), null));
      }
    }
    return result;
  }

  /**
   * Returns a list of {@link ScheduledRuntime} for the given program.
   *
   * @param programId the program to fetch schedules for
   * @param previous {@code true} to get the previous scheduled times; {@code false} to get the
   *     next scheduled times
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
}
