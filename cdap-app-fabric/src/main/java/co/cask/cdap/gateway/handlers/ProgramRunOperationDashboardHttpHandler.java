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

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ops.ArtifactMetaInfo;
import co.cask.cdap.proto.ops.DashboardProgramRunRecord;
import co.cask.cdap.proto.ops.DashboardSummaryRecord;
import co.cask.cdap.proto.ops.DashboardSummaryRequest;
import co.cask.cdap.proto.ops.ExistingDashboardSummaryRecord;
import co.cask.cdap.proto.ops.FilterDeserializer;
import co.cask.cdap.proto.ops.ProgramRunReport;
import co.cask.cdap.proto.ops.ProgramRunReportRecord;
import co.cask.cdap.proto.ops.ReportGenerationInfo;
import co.cask.cdap.proto.ops.ReportGenerationRequest;
import co.cask.cdap.proto.ops.ReportGenerationStatus;
import co.cask.cdap.proto.ops.ReportList;
import co.cask.cdap.proto.ops.ReportStatus;
import co.cask.cdap.proto.ops.ReportStatusInfo;
import co.cask.cdap.proto.ops.ReportSummary;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Singleton;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * {@link co.cask.http.HttpHandler} to handle program run operation dashboard and reports for v3 REST APIs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class ProgramRunOperationDashboardHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ReportGenerationRequest.Filter.class, new FilterDeserializer())
    .create();
  private static final Type REPORT_GENERATION_REQUEST_TYPE = new TypeToken<ReportGenerationRequest>() { }.getType();
  private static final Type DASHBOARD_SUMMARY_REQUEST_TYPE = new TypeToken<DashboardSummaryRequest>() { }.getType();

  public static final List<ReportStatusInfo> MOCK_REPORT_STATUS_INFO = ImmutableList.of(
    new ReportStatusInfo("report0", 1516805200L, ReportStatus.RUNNING),
    new ReportStatusInfo("report1", 1516805201L, ReportStatus.COMPLETED),
    new ReportStatusInfo("report2", 1516805202L, ReportStatus.FAILED));

  public static final ReportGenerationRequest MOCK_REPORT_GENERATION_REQUEST =
    new ReportGenerationRequest(0, 1, ImmutableList.of("namespace", "duration", "user"),
                                ImmutableList.of(
                                  new ReportGenerationRequest.Sort("duration",
                                                                   ReportGenerationRequest.Order.DESCENDING)),
                                ImmutableList.of(
                                  new ReportGenerationRequest.RangeFilter<>("duration",
                                                                            new ReportGenerationRequest.Range<>(null,
                                                                                                                30L))));

  @GET
  @Path("/reports")
  public void getReports(FullHttpRequest request, HttpResponder responder,
                         @QueryParam("offset") String offsetString,
                         @QueryParam("limit") String limitString)
    throws IOException, BadRequestException {
    int offset = Integer.valueOf(offsetString);
    int limit = Integer.valueOf(limitString);
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(new ReportList(offset, limit, MOCK_REPORT_STATUS_INFO.size(),
                                                  MOCK_REPORT_STATUS_INFO)));
  }

  @POST
  @Path("/reports")
  public void executeReportGeneration(FullHttpRequest request, HttpResponder responder)
    throws IOException, BadRequestException {
    ReportGenerationRequest reportGenerationRequest = decodeRequestBody(request, REPORT_GENERATION_REQUEST_TYPE);
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(ImmutableMap.of("id", UUID.randomUUID().toString())));
  }

  @GET
  @Path("/reports/{report-id}")
  public void getReportStatus(HttpRequest request, HttpResponder responder,
                              @PathParam("report-id") String reportId,
                              @QueryParam("share-id") String shareId) {
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(new ReportGenerationInfo(1517410100L, ReportGenerationStatus.COMPLETED,
                                                            MOCK_REPORT_GENERATION_REQUEST)));
  }

  @GET
  @Path("reports/{report-id}/runs")
  public void getReportRuns(FullHttpRequest request, HttpResponder responder,
                            @PathParam("report-id") String reportId,
                            @QueryParam("offset") String offsetString,
                            @QueryParam("limit") String limitString,
                            @QueryParam("share-id") String shareId) throws IOException, BadRequestException {
    long offset = (offsetString == null || offsetString.isEmpty()) ? 0 : Long.parseLong(offsetString);
    long limit = (limitString == null || limitString.isEmpty()) ? Long.MAX_VALUE : Long.parseLong(limitString);
    List<ProgramRunReportRecord> runs = new ArrayList<>();
    String[] namespaces = {"default", "ns1", "ns2"};
    String[] types = {"RealtimePipeline", "BatchPipeline", "MapReduce"};
    String[] users = {"Ajai", "Lea", "Mao"};
    String[] startMethods = {"Manual", "Scheduled", "Triggered"};

    ProgramRunStatus[] statuses = {ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED};
    int[] durations = {400, 500, 600};
    for (int i = 0; i < 60; i++) {
      int m = i % 3;
      long startTs = 1516810000 + i * 100;
      long runningTs = startTs + 100;
      long endTs = startTs + durations[m];
      runs.add(new ProgramRunReportRecord(namespaces[m], new ArtifactMetaInfo("USER", "CustomApp", "v1"),
                                          new ProgramRunReportRecord.ApplicationMetaInfo("CustomApp", "v1"),
                                          ProgramType.WORKFLOW.name(), types[m],
                                          statuses[m], startTs, runningTs, endTs, durations[m], users[m],
                                          startMethods[m], Collections.EMPTY_MAP, 2, 4, 6));
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(new ProgramRunReport(offset, limit, 10000, runs)));
  }

  @GET
  @Path("/reports/{report-id}/summary")
  public void getReportSummary(HttpRequest request, HttpResponder responder,
                               @PathParam("report-id") String reportId,
                               @QueryParam("share-id") String shareId) {
    ReportSummary reportSummary =
      new ReportSummary(ImmutableList.of("default", "ns1", "ns2"), 1516805200, 1517410000,
                        ImmutableList.of(new ReportSummary.ProgramRunArtifact("SYSTEM", "RealtimePipeline", "v1", 20),
                                         new ReportSummary.ProgramRunArtifact("SYSTEM", "BatchPipeline", "v1", 20),
                                         new ReportSummary.ProgramRunArtifact("USER", "CustomApp", "v1", 20)),
                        new ReportSummary.DurationStats(400, 600, 500),
                        new ReportSummary.StartStats(1516815900, 1516810000),
                        ImmutableList.of(new ReportSummary.ProgramRunOwner("Ajai", 20),
                                         new ReportSummary.ProgramRunOwner("Lea", 20),
                                         new ReportSummary.ProgramRunOwner("Mao", 20)),
                        ImmutableList.of(new ReportSummary.ProgramRunStartMethod("MANUAL", 20),
                                         new ReportSummary.ProgramRunStartMethod("TIME", 20),
                                         new ReportSummary.ProgramRunStartMethod("PROGRAM_STATUS", 20)));
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(reportSummary));
  }

  @DELETE
  @Path("/reports/{report-id}")
  public void deleteReport(HttpRequest request, HttpResponder responder,
                           @PathParam("report-id") String reportId) {
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/reports/{report-id}/shareid")
  public void shareReport(HttpRequest request, HttpResponder responder,
                          @PathParam("report-id") String reportId) {
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(ImmutableMap.of("shareid", UUID.randomUUID().toString())));
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

//  @POST
//  @Path("/dashboard/summary")
  private void readDashboardSummary(FullHttpRequest request, HttpResponder responder)
    throws IOException, BadRequestException {

    DashboardSummaryRequest dashboardRequest = decodeRequestBody(request, DASHBOARD_SUMMARY_REQUEST_TYPE);
    int resolution = dashboardRequest.getResolution();
    if (resolution != 300 && resolution != 3600) {
      throw new BadRequestException(String.format("Resolution is %d, but it can only be 500 or 3600.", resolution));
    }
    long currentTs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    List<DashboardSummaryRecord> dashboardSummary = new ArrayList<>();
    long startTs = dashboardRequest.getStartTs();
    int i = 0;
    for (long time = startTs; time < dashboardRequest.getEndTs(); time += resolution) {
      int m = i++ % 5;
      if (time < currentTs) {
        DashboardSummaryRecord.NamespaceSummary namespaceSummary1 =
          new ExistingDashboardSummaryRecord.ExistingNamespaceSummary("ns1", 10, 10, 10, 10, 10, 10,
                                                                      20000 - 2000 * m, 8 - m);
        DashboardSummaryRecord.NamespaceSummary namespaceSummary2 =
          new ExistingDashboardSummaryRecord.ExistingNamespaceSummary("ns2", 10, 10, 10, 10, 10, 10,
                                                                      20000 - 2000 * m, 8 - m);
        dashboardSummary.add(new ExistingDashboardSummaryRecord(time, 50000, 40000 - 4000 * m, 20, 16 - 2 * m,
                                                                ImmutableList.of(namespaceSummary1,
                                                                                 namespaceSummary2)));

      } else {
        DashboardSummaryRecord.NamespaceSummary namespaceSummary1 =
          new DashboardSummaryRecord.NamespaceSummary("ns1", 20);
        DashboardSummaryRecord.NamespaceSummary namespaceSummary2 =
          new DashboardSummaryRecord.NamespaceSummary("ns2", 10);
        dashboardSummary.add(new DashboardSummaryRecord(time, ImmutableList.of(namespaceSummary1, namespaceSummary2)));
      }
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(dashboardSummary));
  }

  private static <T> T decodeRequestBody(FullHttpRequest request, Type type) throws IOException, BadRequestException {
    T decodedRequestBody;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
      try {
        decodedRequestBody = GSON.fromJson(reader, type);
        if (decodedRequestBody == null) {
          throw new BadRequestException("Request body cannot be empty.");
        }
      } catch (JsonSyntaxException e) {
        throw new BadRequestException("Request body is invalid json: " + e.getMessage());
      }
    }
    return decodedRequestBody;
  }
}
