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
import co.cask.cdap.proto.ops.DashboardProgramRunRecord;
import co.cask.cdap.proto.ops.DashboardSummaryRecord;
import co.cask.cdap.proto.ops.DashboardSummaryRequest;
import co.cask.cdap.proto.ops.ProgramRunReport;
import co.cask.cdap.proto.ops.ReportGenerationInfo;
import co.cask.cdap.proto.ops.ReportGenerationRequest;
import co.cask.cdap.proto.ops.ReportGenerationStatus;
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
import java.util.Map;

/**
 * Tests for {@link ProgramRunOperationDashboardHttpHandler}
 */
public class ProgramRunOperationDashboardHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final String BASE_PATH = Constants.Gateway.API_VERSION_3;
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type REPORT_SUMMARY_TYPE = new TypeToken<ReportSummary>() { }.getType();
  private static final Type REPORT_GENERATION_INFO_TYPE = new TypeToken<ReportGenerationInfo>() { }.getType();
  private static final Type REPORT_TYPE = new TypeToken<ProgramRunReport>() { }.getType();
  private static final Type DASHBOARD_SUMMARY_TYPE = new TypeToken<List<DashboardSummaryRecord>>() { }.getType();
  private static final Type DASHBOARD_DETAIL_TYPE = new TypeToken<List<DashboardProgramRunRecord>>() { }.getType();

  @Test
  public void testGenerateReport() throws Exception {
    HttpResponse response = generateReport(ProgramRunOperationDashboardHttpHandler.MOCK_REPORT_GENERATION_REQUEST);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    Map<String, String> id = GSON.fromJson(content, MAP_STRING_STRING_TYPE);
    Assert.assertNotNull(id.get("id"));
  }

  @Test
  public void testGetReportStatusAndSummary() throws Exception {
    String reportId = "randomReportId";
    HttpResponse response = doGet(BASE_PATH + "/reports/" + reportId);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    ReportGenerationInfo reportGenerationInfo = GSON.fromJson(content, REPORT_GENERATION_INFO_TYPE);
    Assert.assertEquals(ProgramRunOperationDashboardHttpHandler.MOCK_REPORT_GENERATION_REQUEST.getStart(),
                        reportGenerationInfo.getRequest().getStart());
    Assert.assertEquals(ProgramRunOperationDashboardHttpHandler.MOCK_REPORT_GENERATION_REQUEST.getFilters().size(),
                        reportGenerationInfo.getRequest().getFilters().size());
    Assert.assertEquals(ReportGenerationStatus.COMPLETED, reportGenerationInfo.getStatus());

    response = doGet(BASE_PATH + "/reports/" + reportId + "/summary?shareid=randomShareId");
    content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    ReportSummary summary = GSON.fromJson(content, REPORT_SUMMARY_TYPE);
    Assert.assertEquals(3, summary.getNamespaces().size());
  }

  @Test
  public void testReadReport() throws Exception {
    HttpResponse response = readReport("randomReportId", 0, 20);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    ProgramRunReport report = GSON.fromJson(content, REPORT_TYPE);
    Assert.assertEquals(60, report.getRuns().size());
  }

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
    HttpResponse response = doGet(BASE_PATH + "/dashboard?start=1000&duration=1440&namespace=mao&namespace=default");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    List<DashboardProgramRunRecord> dashboardDetail = GSON.fromJson(content, DASHBOARD_DETAIL_TYPE);
    Assert.assertEquals(60, dashboardDetail.size());
  }

  private HttpResponse generateReport(ReportGenerationRequest request) throws Exception {
    return doPost(BASE_PATH + "/reports", GSON.toJson(request));
  }

  private HttpResponse readReport(String reportId, int offset, int limit)
    throws Exception {
    return doGet(String.format("%s/reports/%s/runs?offset=%d&limit=%d&shareid=randomShareId", BASE_PATH, reportId,
                                offset, limit));
  }

  private HttpResponse readDashboardSummary(DashboardSummaryRequest request) throws Exception {
    return doPost(BASE_PATH + "/dashboard/summary", GSON.toJson(request));
  }
}
