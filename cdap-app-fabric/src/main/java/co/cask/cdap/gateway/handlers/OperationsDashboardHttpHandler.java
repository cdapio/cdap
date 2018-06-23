/*
 * Copyright © 2018 Cask Data, Inc.
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

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.api.schedule.TriggeringScheduleInfo;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import co.cask.cdap.internal.app.runtime.schedule.TriggeringScheduleInfoAdapter;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.ops.DashboardProgramRunRecord;
import co.cask.cdap.scheduler.Scheduler;
import co.cask.cdap.security.impersonation.SecurityUtil;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;


/**
 * {@link co.cask.http.HttpHandler} to handle program run operation dashboard and reports for v3 REST APIs
 *
 * TODO: [CDAP-13355] Move this handler into report generation app
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class OperationsDashboardHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(OperationsDashboardHttpHandler.class);
  private static final Gson GSON = TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final String MANUAL = "MANUAL";
  private static final String SCHEDULED = "SCHEDULED";
  private static final String TRIGGERED = "TRIGGERED";
  private final Store store;
  private final Scheduler scheduler;
  private final TimeSchedulerService timeSchedulerService;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final CConfiguration cConf;

  @Inject
  public OperationsDashboardHttpHandler(Store store, Scheduler scheduler, TimeSchedulerService timeSchedulerService,
                                        NamespaceQueryAdmin namespaceQueryAdmin,
                                        CConfiguration cConf) {
    this.store = store;
    this.scheduler = scheduler;
    this.timeSchedulerService = timeSchedulerService;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.cConf = cConf;
  }

  // TODO: [CDAP-13351] Need to support getting scheduled program runs if start + duration is ahead of the current time
  @GET
  @Path("/dashboard")
  public void readDashboardDetail(FullHttpRequest request, HttpResponder responder,
                                  @QueryParam("start") long startTimeSecs,
                                  @QueryParam("duration") int durationTimeSecs,
                                  @QueryParam("namespace") Set<String> namespaces)
    throws BadRequestException {
    if (startTimeSecs < 0) {
      throw new BadRequestException("'start' time cannot be smaller than 0.");
    }
    if (durationTimeSecs < 0) {
      throw new BadRequestException("'duration' cannot be smaller than 0.");
    }

    Set<NamespaceId> namespaceIds = namespaces.stream().map(NamespaceId::new).collect(Collectors.toSet());
    long endTimeSecs = startTimeSecs + durationTimeSecs;
    Map<ProgramRunId, RunRecordMeta> historicalRuns =
      // TODO: [CDAP-13352] Currently, to get active program runs within a time range,
      // a full table scan is required in AppMetaStore. Performance improvement will be done.
      store.getHistoricalRuns(namespaceIds, startTimeSecs, endTimeSecs, Integer.MAX_VALUE);
    // get historical runs within the query time range
    Stream<DashboardProgramRunRecord> historicalRecords =
      historicalRuns.values().stream().map(OperationsDashboardHttpHandler::runRecordToDashboardRecord);
    // get active runs with start time earlier than the end of query time range
    Stream<DashboardProgramRunRecord> activeRecords =
      store.getActiveRuns(namespaceIds, run -> run.getStartTs() < endTimeSecs).values().stream()
        .map(OperationsDashboardHttpHandler::runRecordToDashboardRecord);
    List<DashboardProgramRunRecord> result = new ArrayList<>();
    // add combined historical runs and active runs to the result
    result.addAll(Stream.concat(historicalRecords, activeRecords).collect(Collectors.toList()));
    // if the end time is in the future, also add scheduled program runs to the result
      if (endTimeSecs > TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())) {
      // get enabled time schedules from all given namespaces
      getEnabledTimeSchedules(namespaceIds)
        // for each schedule, add all the scheduled runs within given the time range to the result
        .forEach(schedule -> {
          try {
            result.addAll(getScheduledDashboardRecords(schedule, startTimeSecs, endTimeSecs));
          } catch (Exception e) {
            LOG.error("Failed to get scheduled program runs for schedule {}", schedule, e);
          }
        });
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(result));
  }

  /**
   * Gets all the enabled time schedules in the given namespaces
   */
  private Stream<ProgramScheduleRecord> getEnabledTimeSchedules(Set<NamespaceId> namespaceIds) {
    return namespaceIds.stream()
      // get schedules from each namespace
      .flatMap(ns -> scheduler.listScheduleRecords(ns).stream())
      // filter schedules to get enabled time schedules
      .filter(scheduleRecord ->
      ProgramScheduleStatus.SCHEDULED.equals(scheduleRecord.getMeta().getStatus()) &&
        Trigger.Type.TIME.equals(scheduleRecord.getSchedule().getTrigger().getType()));
  }

  /**
   * For a given time schedule, gets all the scheduled program run in the given time range
   *
   * @param scheduleRecord the schedule to get scheduled program run with
   * @param startTimeSecs the start of the time range in seconds (inclusive, i.e. scheduled time larger or
   *                      equal to the start will be returned)
   * @param endTimeSecs the end of the time range in seconds (exclusive, i.e. scheduled time smaller than the end
   *                    will be returned)
   * @return a list of dashboard program run records with scheduled time as start time
   * @throws Exception
   */
  private List<DashboardProgramRunRecord> getScheduledDashboardRecords(ProgramScheduleRecord scheduleRecord,
                                                                       long startTimeSecs, long endTimeSecs)
    throws Exception {
    ProgramSchedule schedule = scheduleRecord.getSchedule();
    ProgramId programId = schedule.getProgramId();
    ProgramDescriptor programDescriptor = store.loadProgram(programId);

    ArtifactSummary artifactSummary = ArtifactSummary.from(programDescriptor.getArtifactId().toApiArtifactId());
    // if the program has a namespace user configured then set that user in the security request context.
    // See: CDAP-7396
    String nsPrincipal = namespaceQueryAdmin.get(programId.getNamespaceId()).getConfig().getPrincipal();
    String userId = nsPrincipal != null && SecurityUtil.isKerberosEnabled(cConf) ?
      new KerberosName(nsPrincipal).getServiceName() : null;
    // get all the scheduled runtimes within the given time range of the given program
    List<ScheduledRuntime> scheduledRuntimes =
      timeSchedulerService.nextScheduledRuntime(programId, programId.getType().getSchedulableType(), startTimeSecs,
        endTimeSecs);
    // for each scheduled runtime, construct a dashboard record for it with the scheduled time as start time
    return scheduledRuntimes.stream()
      .map(scheduledRuntime -> new DashboardProgramRunRecord(programId.getNamespace(), artifactSummary,
        new DashboardProgramRunRecord.ApplicationNameVersion(programId.getApplication(), programId.getVersion()),
        programId.getType().name(), programId.getProgram(), null, userId, SCHEDULED,
        // convert the scheduled time from millis to seconds as start time
        TimeUnit.MILLISECONDS.toSeconds(scheduledRuntime.getTime()), null, null, null))
      .collect(Collectors.toList());
  }


  /**
   * Converts a {@link RunRecordMeta} to a {@link DashboardProgramRunRecord}
   */
  private static DashboardProgramRunRecord runRecordToDashboardRecord(RunRecordMeta meta) {
    ProgramRunId runId = meta.getProgramRunId();
    String startMethod = MANUAL;
    String scheduleInfoJson = meta.getSystemArgs().get(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO);
    if (scheduleInfoJson != null) {
      TriggeringScheduleInfo scheduleInfo = GSON.fromJson(scheduleInfoJson, TriggeringScheduleInfo.class);
      // assume there's no composite trigger, since composite is not supported in the UI yet
      startMethod = scheduleInfo.getTriggerInfos().stream().findFirst()
        .map(trigger -> Trigger.Type.TIME.name().equals(trigger.getType().name()) ? SCHEDULED : TRIGGERED
        )
        // return "manual" if there's no trigger in the schedule info, but this should never happen
        .orElse(MANUAL);
    }
    return new DashboardProgramRunRecord(runId, meta, meta.getArtifactId(), meta.getPrincipal(), startMethod);
  }
}
