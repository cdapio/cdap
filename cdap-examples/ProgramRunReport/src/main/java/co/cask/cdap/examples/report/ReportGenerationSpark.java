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

package co.cask.cdap.examples.report;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractExtendedSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.api.spark.service.AbstractSparkHttpServiceHandler;
import co.cask.cdap.api.spark.service.SparkHttpContentConsumer;
import co.cask.cdap.api.spark.service.SparkHttpServiceContext;
import co.cask.cdap.api.spark.service.SparkHttpServiceHandler;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * A spark program for generating report.
 */
public class ReportGenerationSpark extends AbstractExtendedSpark implements JavaSparkMain {
  private static final Logger LOG = LoggerFactory.getLogger(ReportGenerationSpark.class);
  private static final Gson GSON = new Gson();
  private static final String output = "/Users/Chengfeng/tmp/report";

  @Override
  protected void configure() {
    setMainClass(ReportGenerationSpark.class);
    addHandlers(new ReportSparkHandler());
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();
    SQLContext sqlContext = new SQLContext(jsc);
    LOG.info("Created sqlContext");
    DataFrame dataset = sqlContext.read().format("com.databricks.spark.avro")
      .load("/Users/Chengfeng/tmp/run_meta.avro");
    LOG.info("Loaded input");
    GroupedData groupedData = dataset.groupBy("program", "run");
    LOG.info("Grouped data");
    DataFrame agg = groupedData.agg(new ProgramRunMetaAggregator()
                                      .toColumn(Encoders.bean(ReportRecordBuilder.class),
                                                Encoders.bean(ReportRecordBuilder.class))
                                      .alias("reportRecord"));
    LOG.info("Aggregated data");
    agg.select("reportRecord").write().json(output + "/reportId.json");
    new File(output + "/_SUCCESS").createNewFile();
    LOG.info("Wrote files to " + output);
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
    private static final Logger LOG = LoggerFactory.getLogger(ProgramRunMetaAggregator.class);

    @Override
    public ReportRecordBuilder zero() {
      return new ReportRecordBuilder();
    }

    @Override
    public ReportRecordBuilder reduce(ReportRecordBuilder recordBuilder, Row row) {
      LOG.info("reduce");
      recordBuilder.setProgramRunStatus(row.getAs("program"), row.getAs("run"), row.getAs("status"),
                                        row.getAs("time"), "user");
      return recordBuilder;
    }

    @Override
    public ReportRecordBuilder merge(ReportRecordBuilder b1, ReportRecordBuilder b2) {
      LOG.info("merge");
      return b1.merge(b2);
    }

    @Override
    public ReportRecordBuilder finish(ReportRecordBuilder reduction) {
      LOG.info("finish");
      return reduction;
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

    @POST
    @Path("/reports")
    public SparkHttpContentConsumer executeReportGeneration(HttpServiceRequest request, HttpServiceResponder responder)
      throws IOException {
      String reportId = UUID.randomUUID().toString();
      String input = getContext().getRuntimeArguments().get("input");
      String output = getContext().getRuntimeArguments().get("output");
      File startedFile = new File(output + "/_START");
      startedFile.createNewFile();
      FileChannel outputChannel = new FileOutputStream(startedFile, false).getChannel();
      return new SparkHttpContentConsumer() {

        @Override
        public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
          LOG.info("Received chunk ");
          outputChannel.write(chunk);
        }

        @Override
        public void onFinish(HttpServiceResponder responder) throws Exception {
          outputChannel.close();
          LOG.info("Finish writing file {} ", startedFile.getAbsolutePath());
          Executors.newSingleThreadExecutor().submit(() -> {
//          ReportGenerationRequest reportGenerationRequest =
//            decodeRequestBody(request, REPORT_GENERATION_REQUEST_TYPE);
            SQLContext sqlContext = new SQLContext(getContext().getJavaSparkContext());
            LOG.info("Created sqlContext");
            DataFrame dataset = sqlContext.read().format("com.databricks.spark.avro").load(input);
            LOG.info("Loaded input");
            GroupedData groupedData = dataset.groupBy("program", "run");
            LOG.info("Grouped data");
            DataFrame agg = groupedData.agg(new ProgramRunMetaAggregator()
                                              .toColumn(Encoders.bean(ReportRecordBuilder.class),
                                                        Encoders.bean(ReportRecordBuilder.class))
                                              .alias("reportRecord"));
            LOG.info("Aggregated data");
            agg.select("reportRecord").write().json(output + "/reportId.json");
            LOG.info("Write report to " + output);
          });
          responder.sendJson(200,
                             GSON.toJson(ImmutableMap.of("id", reportId)));
        }

        @Override
        public void onError(HttpServiceResponder responder, Throwable failureCause) {
          try {
            outputChannel.close();
          } catch (IOException e) {
            LOG.warn("Failed to close file channel for file {}", startedFile.getAbsolutePath(), e);
          }
        }
      };
    }
  }
}
