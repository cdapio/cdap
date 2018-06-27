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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.schedule.TriggeringScheduleInfo;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.TriggeringScheduleInfoAdapter;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.ops.DashboardProgramRunRecord;
import co.cask.cdap.reporting.ProgramHeartbeatService;
import co.cask.http.HttpResponder;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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
  private static final Gson GSON =
    TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder()).create();
  private final ProgramHeartbeatService programHeartbeatService;

  @Inject
  public OperationsDashboardHttpHandler(ProgramHeartbeatService programHeartbeatService) {
    this.programHeartbeatService = programHeartbeatService;
  }

  // TODO: [CDAP-13351] Need to support getting scheduled program runs if start + duration is ahead of the current time
  @GET
  @Path("/dashboard")
  public void readDashboardDetail(FullHttpRequest request, HttpResponder responder,
                                  @QueryParam("start") long startTimeSecs,
                                  @QueryParam("duration") int durationTimeSecs,
                                  @QueryParam("namespace") Set<String> namespaces) throws BadRequestException {
    if (startTimeSecs < 0) {
      throw new BadRequestException("'start' time cannot be smaller than 0.");
    }
    if (durationTimeSecs < 0) {
      throw new BadRequestException("'duration' cannot be smaller than 0.");
    }
    if (namespaces.isEmpty()) {
      throw new BadRequestException("'namespace' cannot be empty, please provide at least one namespace.");
    }

    Collection<RunRecordMeta> runRecordMetas =
      programHeartbeatService.scan(startTimeSecs, startTimeSecs + durationTimeSecs + 1, namespaces);
    List<DashboardProgramRunRecord> result =
      runRecordMetas.stream()
        .map(OperationsDashboardHttpHandler::runRecordToDashboardRecord).collect(Collectors.toList());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(result));
  }

  /**
   * Converts a {@link RunRecordMeta} to a {@link DashboardProgramRunRecord}
   */
  @VisibleForTesting
  static DashboardProgramRunRecord runRecordToDashboardRecord(RunRecordMeta meta) {
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
