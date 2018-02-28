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

package co.cask.cdap.internal.app.report;

import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractExtendedSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.api.spark.service.AbstractSparkHttpServiceHandler;
import co.cask.cdap.api.spark.service.SparkHttpServiceContext;
import co.cask.cdap.api.spark.service.SparkHttpServiceHandler;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ops.ArtifactMetaInfo;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * A spark program for generating report.
 */
public class ReportGenerationSpark extends AbstractExtendedSpark implements JavaSparkMain {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ReportGenerationRequest.Filter.class, new FilterDeserializer())
    .create();
  private static final Type REPORT_GENERATION_REQUEST_TYPE = new TypeToken<ReportGenerationRequest>() { }.getType();
  public static final List<ReportStatusInfo> MOCK_REPORT_STATUS_INFO = ImmutableList.of(
    new ReportStatusInfo("report0", 1516805200L, ReportStatus.RUNNING),
    new ReportStatusInfo("report1", 1516805201L, ReportStatus.COMPLETED),
    new ReportStatusInfo("report2", 1516805202L, ReportStatus.FAILED));

  @Override
  protected void configure() {
    setMainClass(ReportGenerationSpark.class);
    addHandlers(new ReportSparkHandler());
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();
  }

  /**
   *
   */
  public static class ProgramStartingInfo implements Serializable {
    private final String user;

    public ProgramStartingInfo(String user) {
      this.user = user;
    }

    public String getUser() {
      return user;
    }
  }

  /**
   *
   */
  public static class StatusTime implements Serializable {
    private final String status;
    private final long time;

    public StatusTime(String status, long time) {
      this.status = status;
      this.time = time;
    }

    public String getStatus() {
      return status;
    }

    public long getTime() {
      return time;
    }
  }

  /**
   *
   */
  public static class ReportRecordBuilder implements Serializable {
    private String program;
    private String run;
    private List<StatusTime> statusTimes;
    @Nullable
    private ProgramStartingInfo programStartingInfo;

    public ReportRecordBuilder() {
      this.statusTimes = new ArrayList<>();
    }

    public void setProgramRunStatus(String program, String run, String status, long time, @Nullable String user) {
      this.program = program;
      this.run = run;
      this.statusTimes.add(new StatusTime(status, time));
      if (user != null) {
        this.programStartingInfo = new ProgramStartingInfo(user);
      }
    }

    public ReportRecordBuilder merge(ReportRecordBuilder other) {
      this.statusTimes.addAll(other.statusTimes);
      this.programStartingInfo = this.programStartingInfo != null ?
        this.programStartingInfo : other.programStartingInfo;
      return this;
    }
  }

  /**
   *
   */
  public static class ProgramRunMetaAggregator extends Aggregator<Row, ReportRecordBuilder, ReportRecordBuilder> {

    @Override
    public ReportRecordBuilder zero() {
      return new ReportRecordBuilder();
    }

    @Override
    public ReportRecordBuilder reduce(ReportRecordBuilder recordBuilder, Row row) {
      recordBuilder.setProgramRunStatus(row.getAs("program"), row.getAs("run"), row.getAs("status"),
                                        row.getAs("time"), "user");
      return recordBuilder;
    }

    @Override
    public ReportRecordBuilder merge(ReportRecordBuilder b1, ReportRecordBuilder b2) {
      return b1.merge(b2);
    }

    @Override
    public ReportRecordBuilder finish(ReportRecordBuilder reduction) {
      return reduction;
    }

    @Override
    public Encoder<ReportRecordBuilder> bufferEncoder() {
      return (Encoder<ReportRecordBuilder>) Encoders.bean(ReportRecordBuilder.class);
    }

    @Override
    public Encoder<ReportRecordBuilder> outputEncoder() {
      return (Encoder<ReportRecordBuilder>) Encoders.bean(ReportRecordBuilder.class);
    }
  }

  /**
   * A {@link SparkHttpServiceHandler} for read and generate report.
   */
  public static final class ReportSparkHandler extends AbstractSparkHttpServiceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReportSparkHandler.class);

    @Override
    public void initialize(SparkHttpServiceContext context) throws Exception {
      super.initialize(context);
    }

    @GET
    @Path("/reports")
    public void getReports(HttpServiceRequest request, HttpServiceResponder responder,
                           @QueryParam("offset") String offsetString,
                           @QueryParam("limit") String limitString)
      throws IOException, BadRequestException {
      int offset = Integer.valueOf(offsetString);
      int limit = Integer.valueOf(limitString);
      responder.sendJson(HttpResponseStatus.OK.code(),
                         GSON.toJson(new ReportList(offset, limit, MOCK_REPORT_STATUS_INFO.size(),
                                                    MOCK_REPORT_STATUS_INFO)));
    }

    @POST
    @Path("/reports")
    public void executeReportGeneration(HttpServiceRequest request, HttpServiceResponder responder)
      throws IOException, BadRequestException {
      String reportId = UUID.randomUUID().toString();
//      Executors.newSingleThreadExecutor().submit(() -> {
//          ReportGenerationRequest reportGenerationRequest =
//            decodeRequestBody(request, REPORT_GENERATION_REQUEST_TYPE);
        String input = getContext().getRuntimeArguments().get("input");
        String output = getContext().getRuntimeArguments().get("output");
        SQLContext sqlContext = new SQLContext(getContext().getSparkContext());
        Dataset<Row> dataset = sqlContext.read().format("com.databricks.spark.avro").load(input);
        dataset.groupBy("program", "run")
          .agg(new ProgramRunMetaAggregator()
                 .toColumn().name("reportRecord"))
          .select("reportRecord").write().json(output);
        LOG.info("Write report to " + output);
//      });
      responder.sendJson(HttpResponseStatus.OK.code(),
                         GSON.toJson(ImmutableMap.of("id", reportId)));
    }

    @GET
    @Path("/reports/{report-id}")
    public void getReportStatus(HttpServiceRequest request, HttpServiceResponder responder,
                                @PathParam("report-id") String reportId,
                                @QueryParam("share-id") String shareId) {
      String output = getContext().getRuntimeArguments().get("output");
      File outputFile = new File(output);
      ReportGenerationStatus status = outputFile.exists() ?
        ReportGenerationStatus.COMPLETED : ReportGenerationStatus.RUNNING;
      responder.sendJson(HttpResponseStatus.OK.code(),
                         GSON.toJson(new ReportGenerationInfo(1517410100L, status, null)));
    }

    @GET
    @Path("reports/{report-id}/runs")
    public void getReportRuns(HttpServiceRequest request, HttpServiceResponder responder,
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
                                            new ProgramRunReportRecord.ApplicationMetaInfo("CustomApp", "v1"), types[m],
                                            statuses[m], startTs, runningTs, endTs, durations[m], users[m],
                                            startMethods[m], Collections.EMPTY_MAP, 2, 4, 6));
      }
      responder.sendJson(HttpResponseStatus.OK.code(), GSON.toJson(new ProgramRunReport(offset, limit, 10000, runs)));
    }

    @GET
    @Path("/reports/{report-id}/summary")
    public void getReportSummary(HttpServiceRequest request, HttpServiceResponder responder,
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
      responder.sendJson(HttpResponseStatus.OK.code(), GSON.toJson(reportSummary));
    }

    @DELETE
    @Path("/reports/{report-id}")
    public void deleteReport(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("report-id") String reportId) {
      responder.sendStatus(HttpResponseStatus.OK.code());
    }

    @POST
    @Path("/reports/{report-id}/shareid")
    public void shareReport(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("report-id") String reportId) {
      responder.sendJson(HttpResponseStatus.OK.code(),
                         GSON.toJson(ImmutableMap.of("shareid", UUID.randomUUID().toString())));
    }
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
