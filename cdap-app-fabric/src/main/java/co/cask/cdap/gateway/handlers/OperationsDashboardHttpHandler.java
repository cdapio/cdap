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

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.schedule.TriggeringScheduleInfo;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.TriggeringScheduleInfoAdapter;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.ops.DashboardProgramRunRecord;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * {@link co.cask.http.HttpHandler} to handle program run operation dashboard and reports for v3 REST APIs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class OperationsDashboardHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Gson GSON = TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder()).create();
  private final Store store;

  @Inject
  public OperationsDashboardHttpHandler(Store store) {
    this.store = store;
  }

  @GET
  @Path("/dashboard")
  public void readDashboardDetail(FullHttpRequest request, HttpResponder responder,
                                  @QueryParam("start") long start,
                                  @QueryParam("duration") int duration,
                                  @QueryParam("namespace") Set<String> namespaces)
    throws IOException, BadRequestException {
    List<DashboardProgramRunRecord> dashboardDetails = new ArrayList<>();
    Map<ProgramRunId, RunRecordMeta> historicalRuns =
      store.getHistoricalRuns(namespaces, start, start + duration, Integer.MAX_VALUE);
    // get historical runs within the query time range
    historicalRuns.values().stream().map(OperationsDashboardHttpHandler::runRecordToDashboardRecord)
      .forEach(dashboardDetails::add);
    // get active runs with start time earlier than the end of query time range
    namespaces.stream().map(NamespaceId::new)
      .map(store::getActiveRuns)
      .flatMap(runs -> runs.values().stream())
      .filter(run -> run.getStartTs() < start + duration)
      .map(OperationsDashboardHttpHandler::runRecordToDashboardRecord)
      .forEach(dashboardDetails::add);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(dashboardDetails));
  }

  /**
   * Converts a {@link RunRecordMeta} to a {@link DashboardProgramRunRecord}
   */
  private static DashboardProgramRunRecord runRecordToDashboardRecord(RunRecordMeta meta) {
    ProgramRunId runId = meta.getProgramRunId();
    ArtifactId artifactId = meta.getArtifactId();
    String startMethod = "manual";
    String scheduleInfoJson = meta.getSystemArgs().get(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO);
    if (scheduleInfoJson != null) {
      TriggeringScheduleInfo scheduleInfo = GSON.fromJson(scheduleInfoJson, TriggeringScheduleInfo.class);
      // assume there's no composite trigger, since composite is not supported in the UI yet
      startMethod = scheduleInfo.getTriggerInfos().stream().findFirst()
        .map(trigger -> trigger.getType().name())
        .orElseGet(() -> "manual");
    }
    return new DashboardProgramRunRecord(runId.getNamespace(), ArtifactSummary.from(artifactId),
                                         runId.getApplication(), runId.getType().name(), runId.getProgram(),
                                         runId.getRun(), meta.getPrincipal(), startMethod,
                                         meta.getStartTs(), meta.getRunTs(), meta.getStopTs(), meta.getStatus());
  }
}
