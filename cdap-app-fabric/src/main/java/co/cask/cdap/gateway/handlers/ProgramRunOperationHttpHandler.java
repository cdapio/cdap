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
import co.cask.cdap.proto.ops.DashboardPorgramRunRecord;
import co.cask.cdap.proto.ops.DashboardSummaryRecord;
import co.cask.cdap.proto.ops.DashboardSummaryRequest;
import co.cask.cdap.proto.ops.ExistingDashboardSummaryRecord;
import co.cask.cdap.proto.ops.ProgramRunOperationRequest;
import co.cask.cdap.proto.ops.ReportGenerationStatus;
import co.cask.cdap.proto.ops.ReportProgramRunRecord;
import co.cask.cdap.proto.ops.ReportReadRequest;
import co.cask.cdap.proto.ops.ReportStatus;
import co.cask.cdap.proto.ops.ReportSummary;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * {@link co.cask.http.HttpHandler} to handle program run operation dashboard and reports for v3 REST APIs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/ops")
public class ProgramRunOperationHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Gson GSON = new Gson();
  private static final Type OPERATION_REQUEST_TYPE = new TypeToken<ProgramRunOperationRequest>() { }.getType();
  private static final Type REPORT_READ_REQUEST_TYPE = new TypeToken<ProgramRunOperationRequest>() { }.getType();
  private static final Type DASHBOARD_SUMMARY_REQUEST_TYPE = new TypeToken<DashboardSummaryRequest>() { }.getType();

  @POST
  @Path("report/execute")
  public void executeReportGeneration(FullHttpRequest request, HttpResponder responder)
    throws IOException, BadRequestException {

    ProgramRunOperationRequest operationRequest = decodeRequestBody(request, OPERATION_REQUEST_TYPE);
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(new ReportStatus(UUID.randomUUID().toString(), 1517410100L,
                                                    ReportGenerationStatus.COMPLETED)));
  }

  @GET
  @Path("report/{report-id}/status")
  public void getReportStatus(HttpRequest request, HttpResponder responder,
                              @PathParam("report-id") String reportId) {
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(new ReportStatus(reportId, 1517410100L, ReportGenerationStatus.COMPLETED)));
  }

  @GET
  @Path("report/{report-id}/summary")
  public void getReportSummary(HttpRequest request, HttpResponder responder,
                               @PathParam("report-id") String reportId) {
    ReportSummary reportSummary = new ReportSummary(ImmutableList.of("default", "ns1", "ns2"), 1516805200, 1517410000,
                                                    ImmutableMap.of("RealtimePipeline", 20,
                                                                    "BatchPipeline", 20,
                                                                    "MapReduce", 20),
                                                    400, 600, 500, 1516815900, 1516810000,
                                                    ImmutableList.of(new ReportSummary.ProgramRunOwner("Ajai", 20),
                                                                     new ReportSummary.ProgramRunOwner("Lea", 20),
                                                                     new ReportSummary.ProgramRunOwner("Mao", 20)),
                                                    20, 20, 20);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(reportSummary));
  }

  @POST
  @Path("report/{report-id}/read")
  public void readReport(FullHttpRequest request, HttpResponder responder,
                         @PathParam("report-id") String reportId,
                         @QueryParam("offset") String offsetString,
                         @QueryParam("limit") String limitString) throws IOException, BadRequestException {
    long start = (offsetString == null || offsetString.isEmpty()) ? 0 : Long.parseLong(offsetString);
    long limit = (limitString == null || limitString.isEmpty()) ? Long.MAX_VALUE : Long.parseLong(limitString);
    ReportReadRequest reportReadRequest = decodeRequestBody(request, REPORT_READ_REQUEST_TYPE);
    List<ReportProgramRunRecord> report = new ArrayList<>();
    String[] namespaces = {"default", "ns1", "ns2"};
    String[] types = {"RealtimePipeline", "BatchPipeline", "MapReduce"};
    String[] users = {"Ajai", "Lea", "Mao"};
    String[] startMethods = {"Manual", "Scheduled", "Triggered"};

    ProgramRunStatus[] statuses = {ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED};
    int[] durations = {400, 500, 600};
    for (int i = 0; i < 60; i++) {
      int m = i % 3;
      long startTs = 1516810000 + i * 100;
      long endTs = startTs + durations[m];
      report.add(new ReportProgramRunRecord(namespaces[m], types[m] + Integer.toString(i / 3), types[m], statuses[m],
                                            startTs, endTs, durations[m], users[m], startMethods[m],
                                            Collections.EMPTY_MAP, 50, 200, 100, 2, 10, 5, 1, 5, 3, 2, 4, 6));
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(report));
  }

  @POST
  @Path("dashboard/summary")
  public void readDashboardSummary(FullHttpRequest request, HttpResponder responder)
    throws IOException, BadRequestException {

    DashboardSummaryRequest dashboardRequest = decodeRequestBody(request, DASHBOARD_SUMMARY_REQUEST_TYPE);
    int resolution = dashboardRequest.getResolution();
    if (resolution != 300 || resolution != 3600) {
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

  @POST
  @Path("dashboard/detail")
  public void readDashboardDetail(FullHttpRequest request, HttpResponder responder)
    throws IOException, BadRequestException {
    ProgramRunOperationRequest dashboardRequest = decodeRequestBody(request, OPERATION_REQUEST_TYPE);
    List<DashboardPorgramRunRecord> dashboardDetails = new ArrayList<>();
    String[] namespaces = {"ns1", "ns2"};
    String[] types = {"RealtimePipeline", "BatchPipeline", "MapReduce"};
    String[] users = {"Ajai", "Lea", "Mao"};
    String[] startMethods = {"Manual", "Scheduled", "Triggered"};

    ProgramRunStatus[] statuses = {ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.RUNNING};
    int[] durations = {400, 500, -1};
    for (int i = 0; i < 60; i++) {
      int m = i % 3;
      dashboardDetails.add(new DashboardPorgramRunRecord(namespaces[i % 2], types[m] + Integer.toString(i / 3),
                                                         types[m], durations[m], users[m], startMethods[m],
                                                         statuses[m]));
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(dashboardDetails));
  }

  private <T> T decodeRequestBody(FullHttpRequest request, Type type) throws IOException, BadRequestException {
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
