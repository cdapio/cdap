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
import co.cask.cdap.api.schedule.TriggeringScheduleInfo;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.ops.ArtifactMetaInfo;
import co.cask.cdap.proto.ops.DashboardProgramRunRecord;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
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
  private static final Gson GSON = new Gson();
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
    historicalRuns.values().stream().map(OperationsDashboardHttpHandler::getDashboardRecord)
      .forEach(dashboardDetails::add);
    namespaces.stream().map(ns -> new NamespaceId(ns))
      .map(store::getActiveRuns)
      .flatMap(runs -> runs.values().stream())
      .map(OperationsDashboardHttpHandler::getDashboardRecord)
      .forEach(dashboardDetails::add);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(dashboardDetails));
  }

  public static DashboardProgramRunRecord getDashboardRecord(RunRecordMeta meta) {
    ProgramRunId runId = meta.getProgramRunId();
    ArtifactId artifactId = meta.getArtifactId();
    String startMethod = "manual";
    String scheduleInfoJson = meta.getSystemArgs().get(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO);
    if (scheduleInfoJson != null) {
      TriggeringScheduleInfo scheduleInfo = GSON.fromJson(scheduleInfoJson, TriggeringScheduleInfo.class);
      // assume there's no composite trigger, since composite
      startMethod = scheduleInfo.getTriggerInfos().stream().findFirst()
        .map(trigger -> trigger.getType().name())
        .orElseGet(() -> "manual");
    }
    return new DashboardProgramRunRecord(runId.getNamespace(),
                                         new ArtifactMetaInfo(artifactId.getScope().name(), artifactId.getName(),
                                                              artifactId.getVersion().getVersion()),
                                         runId.getApplication(), runId.getType().name(), runId.getProgram(),
                                         runId.getRun(), meta.getPrincipal(), startMethod,
                                         meta.getStartTs(), meta.getRunTs(), meta.getStopTs(), meta.getStatus());
  }
}
