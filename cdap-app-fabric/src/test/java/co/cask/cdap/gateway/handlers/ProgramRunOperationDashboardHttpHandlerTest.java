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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.ops.DashboardPorgramRunRecord;
import co.cask.cdap.proto.ops.DashboardSummaryRecord;
import co.cask.cdap.proto.ops.DashboardSummaryRequest;
import co.cask.cdap.proto.ops.ProgramRunOperationRequest;
import co.cask.cdap.proto.ops.ReportGenerationStatus;
import co.cask.cdap.proto.ops.ReportProgramRunRecord;
import co.cask.cdap.proto.ops.ReportReadRequest;
import co.cask.cdap.proto.ops.ReportStatus;
import co.cask.cdap.proto.ops.ReportSummary;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Tests for {@link ProgramRunOperationDashboardHttpHandler}
 */
public class ProgramRunOperationDashboardHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final String OPS_BASE_PATH = Constants.Gateway.API_VERSION_3 + "/ops";
  private static final Type REPORT_STATUS_TYPE = new TypeToken<ReportStatus>() { }.getType();
  private static final Type REPORT_SUMMARY_TYPE = new TypeToken<ReportSummary>() { }.getType();
  private static final Type REPORT_TYPE = new TypeToken<List<ReportProgramRunRecord>>() { }.getType();
  private static final Type DASHBOARD_SUMMARY_TYPE = new TypeToken<List<DashboardSummaryRecord>>() { }.getType();
  private static final Type DASHBOARD_DETAIL_TYPE = new TypeToken<List<DashboardPorgramRunRecord>>() { }.getType();



  @Test
  public void testGenerateReport() throws Exception {
    HttpResponse response = generateReport(new ProgramRunOperationRequest(0L, 1L, "mao", ImmutableList.of("default")));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    ReportStatus reportStatus = GSON.fromJson(content, REPORT_STATUS_TYPE);
    Assert.assertEquals(ReportGenerationStatus.COMPLETED, reportStatus.getStatus());
  }

  @Test
  public void testGetReportStatusAndSummary() throws Exception {
    String reportId = "randomReportId";
    HttpResponse response = doGet(OPS_BASE_PATH + "/report/" + reportId + "/status");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    ReportStatus reportStatus = GSON.fromJson(content, REPORT_STATUS_TYPE);
    Assert.assertEquals(reportId, reportStatus.getReportId());
    Assert.assertEquals(ReportGenerationStatus.COMPLETED, reportStatus.getStatus());

    response = doGet(OPS_BASE_PATH + "/report/" + reportId + "/summary");
    content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    ReportSummary summary = GSON.fromJson(content, REPORT_SUMMARY_TYPE);
    Assert.assertEquals(3, summary.getNamespaces().size());
  }

  @Test
  public void testReadReport() throws Exception {
    ReportReadRequest reportReadRequest =
      new ReportReadRequest(new ReportReadRequest.Filter(ImmutableList.of("default, mao")), null, null, null,
                            new ReportReadRequest.Sortable(new ReportReadRequest.Range(100, 1000),
                                                           ReportReadRequest.SortBy.DESCENDING),
                            null, null, null, null, null, null, null, null, null, null, null);
    HttpResponse response = readReport("randomReportId", 0, 20, reportReadRequest);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    List<ReportProgramRunRecord> report = GSON.fromJson(content, REPORT_TYPE);
    Assert.assertEquals(60, report.size());
  }

  @Test
  public void testDashboardSummary() throws Exception {
    DashboardSummaryRequest dashboardRequest = new DashboardSummaryRequest(100, 3700, "mao",
                                                                           ImmutableList.of("ns1", "ns2"), 300);
    HttpResponse response = readDashboardSummary(dashboardRequest);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    List<DashboardSummaryRecord> dashboardSummary = GSON.fromJson(content, DASHBOARD_SUMMARY_TYPE);
    Assert.assertEquals(12, dashboardSummary.size());
  }

  @Test
  public void testDashboardDetail() throws Exception {
    ProgramRunOperationRequest dashboardRequest = new ProgramRunOperationRequest(100, 400, "mao",
                                                                                 ImmutableList.of("ns1", "ns2"));
    HttpResponse response = readDashboardDetail(dashboardRequest);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    List<DashboardPorgramRunRecord> dashboardDetail = GSON.fromJson(content, DASHBOARD_DETAIL_TYPE);
    Assert.assertEquals(60, dashboardDetail.size());
  }

  private HttpResponse generateReport(ProgramRunOperationRequest request) throws Exception {
    return doPost(OPS_BASE_PATH + "/report/execute", GSON.toJson(request));
  }

  private HttpResponse readReport(String reportId, int offset, int limit, ReportReadRequest request) throws Exception {
    return doPost(String.format("%s/report/%s/read?offset=%d&limit=%d", OPS_BASE_PATH, reportId, offset, limit),
                  GSON.toJson(request));
  }

  private HttpResponse readDashboardSummary(DashboardSummaryRequest request) throws Exception {
    return doPost(OPS_BASE_PATH + "/dashboard/summary", GSON.toJson(request));
  }

  private HttpResponse readDashboardDetail(ProgramRunOperationRequest request) throws Exception {
    return doPost(OPS_BASE_PATH + "/dashboard/detail", GSON.toJson(request));
  }
}
