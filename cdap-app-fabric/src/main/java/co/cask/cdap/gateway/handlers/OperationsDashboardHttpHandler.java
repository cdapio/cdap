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

import co.cask.cdap.api.schedule.TriggeringScheduleInfo;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.ops.DashboardProgramRunRecord;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
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
  private static final Gson GSON = new Gson();
  private final Store store;

  @Inject
  public OperationsDashboardHttpHandler(Store store) {
    this.store = store;
  }

  // TODO: [CDAP-13351] Need to support getting scheduled program runs if start + duration is ahead of the current time
  @GET
  @Path("/dashboard")
  public void readDashboardDetail(FullHttpRequest request, HttpResponder responder,
                                  @QueryParam("start") long startTimeSecs,
                                  @QueryParam("duration") int durationTimeSecs,
                                  @QueryParam("namespace") Set<String> namespaces)
    throws IOException, BadRequestException {
    if (startTimeSecs < 0) {
      throw new BadRequestException("'start' time cannot be smaller than 0.");
    }
    if (durationTimeSecs < 0) {
      throw new BadRequestException("'duration' cannot be smaller than 0.");
    }

    Set<NamespaceId> namespaceIds = namespaces.stream().map(NamespaceId::new).collect(Collectors.toSet());
    Map<ProgramRunId, RunRecordMeta> historicalRuns =
      // TODO: [CDAP-13352] Currently, to get active program runs within a time range,
      // a full table scan is required in AppMetaStore. Performance improvement will be done.
      store.getHistoricalRuns(namespaceIds, startTimeSecs, startTimeSecs + durationTimeSecs, Integer.MAX_VALUE);
    // get historical runs within the query time range
    Stream<DashboardProgramRunRecord> historicalRecords =
      historicalRuns.values().stream().map(OperationsDashboardHttpHandler::runRecordToDashboardRecord);
    // get active runs with start time earlier than the end of query time range
    Stream<DashboardProgramRunRecord> activeRecords =
      store.getActiveRuns(namespaceIds, run -> run.getStartTs() < startTimeSecs + durationTimeSecs).values().stream()
      .map(OperationsDashboardHttpHandler::runRecordToDashboardRecord);
    responder.sendJson(HttpResponseStatus.OK,
                       // combine historical runs and active runs
                       GSON.toJson(Stream.concat(historicalRecords, activeRecords).collect(Collectors.toSet())));
  }

  /**
   * Converts a {@link RunRecordMeta} to a {@link DashboardProgramRunRecord}
   */
  private static DashboardProgramRunRecord runRecordToDashboardRecord(RunRecordMeta meta) {
    ProgramRunId runId = meta.getProgramRunId();
    String startMethod = "manual";
    String scheduleInfoJson = meta.getSystemArgs().get(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO);
    if (scheduleInfoJson != null) {
      TriggeringScheduleInfo scheduleInfo = GSON.fromJson(scheduleInfoJson, TriggeringScheduleInfo.class);
      // assume there's no composite trigger, since composite is not supported in the UI yet
      startMethod = scheduleInfo.getTriggerInfos().stream().findFirst()
        .map(trigger -> trigger.getType().name())
        // return "manual" if there's no trigger in the schedule info, but this should never happen
        .orElseGet(() -> "manual");
    }
    return new DashboardProgramRunRecord(runId, meta, meta.getArtifactId(), meta.getPrincipal(), startMethod);
  }
}
