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

package co.cask.cdap.report;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractExtendedSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.api.spark.service.AbstractSparkHttpServiceHandler;
import co.cask.cdap.api.spark.service.SparkHttpContentConsumer;
import co.cask.cdap.api.spark.service.SparkHttpServiceContext;
import co.cask.cdap.api.spark.service.SparkHttpServiceHandler;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * A spark program for generating report.
 */
public class ReportGenerationSpark extends AbstractExtendedSpark implements JavaSparkMain {
  private static final Logger LOG = LoggerFactory.getLogger(ReportGenerationSpark.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ReportGenerationRequest.Filter.class, new FilterDeserializer())
    .create();

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
   * A {@link SparkHttpServiceHandler} for read and generate report.
   */
  public static final class ReportSparkHandler extends AbstractSparkHttpServiceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReportSparkHandler.class);
    private static final Type REPORT_GENERATION_REQUEST_TYPE = new TypeToken<ReportGenerationRequest>() { }.getType();

    private ReportGen reportGen;

    @Override
    public void initialize(SparkHttpServiceContext context) throws Exception {
      super.initialize(context);
      reportGen = new ReportGen(getContext().getSparkContext());
    }

    @POST
    @Path("/reportsExecutor")
    public void executeReportGeneration(HttpServiceRequest request, HttpServiceResponder responder)
      throws IOException {
      String requestJson = Charsets.UTF_8.decode(request.getContent()).toString();

      ReportGenerationRequest reportGenerationRequest = decodeRequestBody(requestJson, REPORT_GENERATION_REQUEST_TYPE);
      String reportId = UUID.randomUUID().toString();
//      Executors.newSingleThreadExecutor().submit(() -> reportGen.getAggregatedDataset(reportGenerationRequest));
      responder.sendJson(200, GSON.toJson(ImmutableMap.of("id", reportId)));
    }

    @POST
    @Path("/reports")
    public void executeReportGenerationDirect(HttpServiceRequest request, HttpServiceResponder responder)
      throws IOException {
      ReportGenerationRequest reportRequest;
      try {
        String requestJson = Charsets.UTF_8.decode(request.getContent()).toString();
        LOG.info("Received report generation request {}", requestJson);
        reportRequest = decodeRequestBody(requestJson, REPORT_GENERATION_REQUEST_TYPE);
        reportRequest.validate();
      } catch (IllegalArgumentException e) {
        responder.sendError(400, "Invalid report generation request: " + e.getMessage());
        return;
      }
      Location metaLocation = Transactionals.execute(getContext(), context -> {
        return context.<FileSet>getDataset(ProgramOperationReportApp.RUN_META_FILESET).getBaseLocation();
      });
      List<Location> nsLocations = metaLocation.list();

      ReportGenerationRequest.ValueFilter<String> namespaceFilter = null;
      if (reportRequest.getFilters() != null) {
        for (ReportGenerationRequest.Filter filter : reportRequest.getFilters()) {
          if (ReportFieldType.NAMESPACE.getFieldName().equals(filter.getFieldName())) {
            // ReportGenerationRequest is validated to contain only one filter for namespace field
            namespaceFilter = (ReportGenerationRequest.ValueFilter<String>) filter;
            break;
          }
        }
      }
      Stream<Location> filteredNsLocations = nsLocations.stream();
      final ReportGenerationRequest.ValueFilter<String> nsFilter = namespaceFilter;
      if (nsFilter != null) {
        filteredNsLocations = nsLocations.stream().filter(nsLocation -> nsFilter.apply(nsLocation.getName()));
      }
      List<String> metaFiles = filteredNsLocations.flatMap(nsLocation -> {
        try {
          return nsLocation.list().stream();
        } catch (IOException e) {
          LOG.error("Failed to list program run meta files for namespace {}", nsLocation.getName(), e);
          return Stream.empty();
        }
      }).filter(metaFile -> Long.parseLong(FilenameUtils.removeExtension(metaFile.getName())) < reportRequest.getEnd())
        .map(location -> location.toURI().toString()).collect(Collectors.toList());
      String reportId = UUID.randomUUID().toString();
      String reportOutputDirectory = Transactionals.execute(getContext(), context -> {
        return context.<FileSet>getDataset(ProgramOperationReportApp.REPORT_FILESET)
          .getLocation(reportId).toURI().toString();
      });
      reportGen.generateReport(reportRequest, metaFiles, reportOutputDirectory);
//      Column startCol = ds.col("record").getField("start");
//      Column endCol = ds.col("record").getField("end");
//      Column startCondition = startCol.isNotNull().and(startCol.lt(reportGenerationRequest.getEnd()));
//      Column endCondition = endCol.isNull().or(
//        (endCol.isNotNull().and(endCol.gt(reportGenerationRequest.getStart()))));
//      Dataset<Row> filteredDs = ds.filter(startCondition.and(endCondition)).persist();
      responder.sendJson(200, GSON.toJson(ImmutableMap.of("id", reportId)));
    }


    @POST
    @Path("/reportsGen")
    public SparkHttpContentConsumer generateReport(HttpServiceRequest request, HttpServiceResponder responder)
      throws IOException {

      String reportId = UUID.randomUUID().toString();

//      Location tmpLocation = Transactionals.execute(getContext(), context -> {
//        return context.<FileSet>getDataset("request").getLocation(UUID.randomUUID().toString());
//      });
//      WritableByteChannel outputChannel = Channels.newChannel(tmpLocation.getOutputStream());

//      String input = getContext().getRuntimeArguments().get("input");
//      String output = getContext().getRuntimeArguments().get("output");
//      File startedFile = new File(output + "/_START");
//      startedFile.createNewFile();
//      FileChannel outputChannel = new FileOutputStream(startedFile, false).getChannel();
      return new SparkHttpContentConsumer() {
        String requestJson;

        @Override
        public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
          LOG.info("Received chunk: {}", Charsets.UTF_8.decode(chunk).toString());
          requestJson = Charsets.UTF_8.decode(chunk).toString();
        }

        @Override
        public void onFinish(HttpServiceResponder responder) throws Exception {
//          outputChannel.close();
//          LOG.info("Finish writing file {} ", startedFile.getAbsolutePath());
          ReportGenerationRequest reportGenerationRequest = decodeRequestBody(requestJson,
                                                                              REPORT_GENERATION_REQUEST_TYPE);
//          Executors.newSingleThreadExecutor().submit(() -> reportGen.getAggregatedDataset(reportGenerationRequest));
          responder.sendJson(200, GSON.toJson(ImmutableMap.of("id", reportId)));
        }

        @Override
        public void onError(HttpServiceResponder responder, Throwable failureCause) {
//          try {
//            outputChannel.close();
//          } catch (IOException e) {
//            LOG.warn("Failed to close file channel for file {}", startedFile.getAbsolutePath(), e);
//          }
        }
      };
    }
  }

  static <T> T decodeRequestBody(String request, Type type) throws IOException {
    T decodedRequestBody;
    try {
      decodedRequestBody = GSON.fromJson(request, type);
      if (decodedRequestBody == null) {
        throw new IllegalArgumentException("Request body cannot be empty.");
      }
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException("Request body is invalid json: " + e.getMessage());
    }
    return decodedRequestBody;
  }
}
