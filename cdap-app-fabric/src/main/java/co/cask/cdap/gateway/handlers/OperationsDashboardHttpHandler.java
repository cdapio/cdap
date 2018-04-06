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

import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
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
import java.util.Set;
import java.util.UUID;
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
                                  @QueryParam("start") String start,
                                  @QueryParam("duration") String duration,
                                  @QueryParam("namespace") Set<String> namespaces)
    throws IOException, BadRequestException {
    List<DashboardProgramRunRecord> dashboardDetails = new ArrayList<>();
    String[] namespaceArray = new String[namespaces.size()];
    namespaces.toArray(namespaceArray);
    String[] types = {"RealtimePipeline", "BatchPipeline", "MapReduce"};
    String[] users = {"Ajai", "Lea", "Mao"};
    String[] startMethods = {"Manual", "Scheduled", "Triggered"};
    long startTs = Long.valueOf(start);
    int durationTs = Integer.valueOf(duration);

    ProgramRunStatus[] statuses = {ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.RUNNING};
    int numRuns = 60;
    for (int i = 0; i < numRuns; i++) {
      long currentStart = startTs + durationTs / numRuns * i;
      int m = i % 3;
      dashboardDetails.add(
        new DashboardProgramRunRecord(namespaceArray[i % namespaceArray.length],
                                      new ArtifactMetaInfo("USER", "CustomApp", "v1"),
                                      types[m] + Integer.toString(i / 3), ProgramType.WORKFLOW.name(), types[m],
                                      UUID.randomUUID().toString(),
                                      users[m], startMethods[m], currentStart, currentStart + 100, currentStart + 200,
                                      statuses[m]));
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(dashboardDetails));
  }
}
