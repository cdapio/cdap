/*
 * Copyright © 2018-2020 Cask Data, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.TriggeringScheduleInfoAdapter;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.ops.DashboardProgramRunRecord;
import io.cdap.cdap.reporting.ProgramHeartbeatService;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;


/**
 * {@link io.cdap.http.HttpHandler} to handle program run operation dashboard and reports for v3 REST APIs
 *
 * TODO: [CDAP-13355] Move this handler into report generation app
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class OperationsDashboardHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(OperationsDashboardHttpHandler.class);
  private static final Gson GSON =
    TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final String MANUAL = "MANUAL";
  private static final String SCHEDULED = "SCHEDULED";
  private static final String TRIGGERED = "TRIGGERED";
  private final ProgramHeartbeatService programHeartbeatService;
  private final Scheduler scheduler;
  private final TimeSchedulerService timeSchedulerService;


  @Inject
  public OperationsDashboardHttpHandler(ProgramHeartbeatService programHeartbeatService,
                                        Scheduler scheduler, TimeSchedulerService timeSchedulerService) {
    this.programHeartbeatService = programHeartbeatService;
    this.scheduler = scheduler;
    this.timeSchedulerService = timeSchedulerService;
  }

  @GET
  @Path("/dashboard")
  public void readDashboardDetail(FullHttpRequest request, HttpResponder responder,
                                  @QueryParam("start") long startTimeSecs,
                                  @QueryParam("duration") int durationTimeSecs,
                                  @QueryParam("namespace") Set<String> namespaces) throws Exception {
    if (startTimeSecs < 0) {
      throw new BadRequestException("'start' time cannot be smaller than 0.");
    }
    if (durationTimeSecs < 0) {
      throw new BadRequestException("'duration' cannot be smaller than 0.");
    }
    if (namespaces.isEmpty()) {
      throw new BadRequestException("'namespace' cannot be empty, please provide at least one namespace.");
    }
    long endTimeSecs = startTimeSecs + durationTimeSecs;
    // end time is exclusive
    Collection<RunRecordDetail> runRecordMetas =
      programHeartbeatService.scan(startTimeSecs, endTimeSecs + 1, namespaces);

    List<DashboardProgramRunRecord> result = new ArrayList<>();
    for (RunRecordDetail runRecordMeta : runRecordMetas) {
      result.add(OperationsDashboardHttpHandler.runRecordToDashboardRecord(runRecordMeta));
    }

    Set<NamespaceId> namespaceIds = namespaces.stream().map(NamespaceId::new).collect(Collectors.toSet());
    // if the end time is in the future, also add scheduled program runs to the result
    long currentTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    // if start time in query is earlier than current time, use currentTime as start when querying future schedules
    long scheduleStartTimeSeconds = startTimeSecs > currentTimeInSeconds ? startTimeSecs : currentTimeInSeconds;
    if (endTimeSecs > currentTimeInSeconds) {
      // end time is exclusive
      result.addAll(getAllScheduledRuns(namespaceIds, scheduleStartTimeSeconds, endTimeSecs + 1));
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(result));
  }

  /**
   * Gets all the scheduled program run in the given time range from the given namespaces.
   *
   * @param namespaceIds the namespaces to get the program runs from
   * @param startTimeSecs the start of the time range in seconds (inclusive, i.e. scheduled time larger or
   *                      equal to the start will be returned)
   * @param endTimeSecs the end of the time range in seconds (exclusive, i.e. scheduled time smaller than the end
   *                    will be returned)
   * @return a list of dashboard program run records with scheduled time as start time
   */
  private List<DashboardProgramRunRecord> getAllScheduledRuns(Set<NamespaceId> namespaceIds,
                                                              long startTimeSecs, long endTimeSecs) throws Exception {
    List<DashboardProgramRunRecord> result = new ArrayList<>();
    // get enabled time schedules from all given namespaces
    for (ProgramSchedule programSchedule : getTimeSchedules(namespaceIds)) {
      try {
        result.addAll(getScheduledDashboardRecords(programSchedule, startTimeSecs, endTimeSecs));
      } catch (Exception e) {
        LOG.error("Failed to get scheduled program runs for schedule {}", programSchedule, e);
        throw e;
      }
    }
    return result;
  }

  /**
   * Gets all the enabled time schedules in the given namespaces
   */
  private List<ProgramSchedule> getTimeSchedules(Set<NamespaceId> namespaceIds) {
    return namespaceIds.stream()
      // get schedules from each namespace
      .flatMap(ns -> scheduler.listSchedules(ns, schedule ->
        // create a filter to get only time schedules
        Trigger.Type.TIME.equals(schedule.getTrigger().getType()))
        .stream()).collect(Collectors.toList());
  }

  /**
   * For a given time schedule, gets all the scheduled program run in the given time range
   *
   * @param schedule the schedule to get scheduled program run with
   * @param startTimeSecs the start of the time range in seconds (inclusive, i.e. scheduled time larger or
   *                      equal to the start will be returned)
   * @param endTimeSecs the end of the time range in seconds (exclusive, i.e. scheduled time smaller than the end
   *                    will be returned)
   * @return a list of dashboard program run records with scheduled time as start time
   * @throws Exception
   */
  private List<DashboardProgramRunRecord> getScheduledDashboardRecords(ProgramSchedule schedule,
                                                                       long startTimeSecs, long endTimeSecs)
    throws Exception {
    ProgramId programId = schedule.getProgramId();
    // get all the scheduled run times within the given time range of the given program
    List<ScheduledRuntime> scheduledRuntimes =
      timeSchedulerService.getAllScheduledRunTimes(programId, programId.getType().getSchedulableType(), startTimeSecs,
                                                   endTimeSecs);
    String userId = schedule.getProperties().get(ProgramOptionConstants.USER_ID);
    String artifactId = schedule.getProperties().get(ProgramOptionConstants.ARTIFACT_ID);
    ArtifactSummary artifactSummary =
      artifactId == null ? null : ArtifactSummary.from(GSON.fromJson(artifactId, ArtifactId.class));
    // for each scheduled runtime, construct a dashboard record for it with the scheduled time as start time
    return scheduledRuntimes.stream()
      .map(scheduledRuntime ->
             new DashboardProgramRunRecord(programId.getNamespace(), artifactSummary,
                                           new DashboardProgramRunRecord.ApplicationNameVersion(
                                             programId.getApplication(), programId.getVersion()),
                                           programId.getType().name(), programId.getProgram(), null, userId, SCHEDULED,
                                           // convert the scheduled time from millis to seconds as start time
                                           TimeUnit.MILLISECONDS.toSeconds(scheduledRuntime.getTime()),
                                           null, null, null, null, null))
      .collect(Collectors.toList());
  }

  /**
   * Converts a {@link RunRecordDetail} to a {@link DashboardProgramRunRecord}
   */
  @VisibleForTesting
  static DashboardProgramRunRecord runRecordToDashboardRecord(RunRecordDetail meta) throws IOException {
    ProgramRunId runId = meta.getProgramRunId();
    String startMethod = MANUAL;
    String scheduleInfoJson = meta.getSystemArgs().get(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO);
    if (scheduleInfoJson != null) {
      TriggeringScheduleInfo scheduleInfo = GSON.fromJson(scheduleInfoJson, TriggeringScheduleInfo.class);
      // assume there's no composite trigger, since composite is not supported in the UI yet
      startMethod = scheduleInfo.getTriggerInfos().stream().findFirst()
        .map(trigger -> TriggerInfo.Type.TIME.equals(trigger.getType()) ? SCHEDULED : TRIGGERED
        )
        // return "manual" if there's no trigger in the schedule info, but this should never happen
        .orElse(MANUAL);
    }
    String user = meta.getPrincipal();
    if (user != null) {
      user = new KerberosName(user).getShortName();
    }
    return new DashboardProgramRunRecord(runId, meta, meta.getArtifactId(),
                                        user, startMethod);
  }
}
