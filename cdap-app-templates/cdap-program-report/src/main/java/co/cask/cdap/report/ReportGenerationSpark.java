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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

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
    private static final String START_FILE = "_START";
    private static final String SUCCESS_FILE = "_SUCCESS";
    private static final String FAILURE_FILE = "_FAILURE";


    private ReportGen reportGen;
    private ConcurrentMap<String, Long> runningReportGeneration;
    private ListeningExecutorService executorService;


    @Override
    public void initialize(SparkHttpServiceContext context) throws Exception {
      super.initialize(context);
      reportGen = new ReportGen(getContext().getSparkContext());
      executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
      runningReportGeneration = new ConcurrentHashMap<>();
    }

    @GET
    @Path("/reports")
    public void getReports(HttpServiceRequest request, HttpServiceResponder responder,
                           @QueryParam("offset") String offsetString,
                           @QueryParam("limit") String limitString)
      throws IOException {
      int offset = Integer.valueOf(offsetString);
      int limit = Integer.valueOf(limitString);
      Location reportBaseLocation = Transactionals.execute(getContext(), context -> {
        return context.<FileSet>getDataset(ProgramOperationReportApp.REPORT_FILESET).getBaseLocation();
      });
      List<ReportStatusInfo> reportStatuses = new ArrayList<>();
      int idx = offset;
      List<Location> reportLocations = reportBaseLocation.list();
      while (idx < reportLocations.size() && reportStatuses.size() < limit) {
        Location reportLocation = reportLocations.get(idx++);
        String reportId = reportLocation.getName();
        BasicFileAttributes attr = Files.readAttributes(Paths.get(reportBaseLocation.toURI()),
                                                        BasicFileAttributes.class);
        Long creationTime = attr.creationTime().toMillis();
        if (runningReportGeneration.containsKey(reportId)) {
          reportStatuses.add(new ReportStatusInfo(reportId, creationTime, ReportStatus.RUNNING));
          continue;
        }
        File successFile = new File(new File(reportLocation.toURI()), SUCCESS_FILE);
        ReportStatus status = successFile.exists() ? ReportStatus.COMPLETED : ReportStatus.FAILED;
        reportStatuses.add(new ReportStatusInfo(reportId, creationTime, status));
      }
      responder.sendJson(200, GSON.toJson(new ReportList(offset, limit, reportLocations.size(), reportStatuses)));
    }


    @GET
    @Path("/reports/{report-id}")
    public void getReportStatus(HttpServiceRequest request, HttpServiceResponder responder,
                                @PathParam("report-id") String reportId,
                                @QueryParam("share-id") String shareId)
      throws IOException {
      if (runningReportGeneration.containsKey(reportId)) {

      }
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
      String requestJson = Charsets.UTF_8.decode(request.getContent()).toString();
      ReportGenerationRequest reportRequest;
      try {
        LOG.info("Received report generation request {}", requestJson);
        reportRequest = decodeRequestBody(requestJson, REPORT_GENERATION_REQUEST_TYPE);
        reportRequest.validate();
      } catch (IllegalArgumentException e) {
        responder.sendError(400, "Invalid report generation request: " + e.getMessage());
        return;
      }

      String reportId = UUID.randomUUID().toString();
      URI reportDirURI = Transactionals.execute(getContext(), context -> {
        return context.<FileSet>getDataset(ProgramOperationReportApp.REPORT_FILESET)
          .getLocation(reportId).toURI();
      });
      File reportDir = new File(reportDirURI);
      reportDir.mkdir();
      File startFile = new File(reportDir, START_FILE);
      startFile.createNewFile();
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(startFile))) {
        bw.write(requestJson);
      }
      runningReportGeneration.put(reportId, System.currentTimeMillis());
      generateReport(reportRequest, reportDirURI.toString());
//      ListenableFuture<String> futureTask = executorService.submit(() -> {
//        generateReport(reportRequest, reportDirURI.toString());
//        return reportId;
//      });
//      Futures.addCallback(futureTask, new FutureCallback<String>() {
//        @Override
//        public void onSuccess(String reportId) {
//          runningReportGeneration.remove(reportId);
//        }
//
//        @Override
//        public void onFailure(Throwable t) {
//          runningReportGeneration.remove(reportId);
//          File failureFile = new File(reportDir, FAILURE_FILE);
//          try {
//            failureFile.createNewFile();
//            try (BufferedWriter bw = new BufferedWriter(new FileWriter(failureFile))) {
//              bw.write(t.toString());
//            }
//          } catch (IOException e) {
//            throw new RuntimeException(e);
//          }
//        }
//      });

//      Column startCol = ds.col("record").getField("start");
//      Column endCol = ds.col("record").getField("end");
//      Column startCondition = startCol.isNotNull().and(startCol.lt(reportGenerationRequest.getEnd()));
//      Column endCondition = endCol.isNull().or(
//        (endCol.isNotNull().and(endCol.gt(reportGenerationRequest.getStart()))));
//      Dataset<Row> filteredDs = ds.filter(startCondition.and(endCondition)).persist();
      responder.sendJson(200, GSON.toJson(ImmutableMap.of("id", reportId)));
    }

    private void generateReport(ReportGenerationRequest reportRequest, String reportOutputPath) {
      List<Location> nsLocations;
      try {
        nsLocations = Transactionals.execute(getContext(), context -> {
          return context.<FileSet>getDataset(ProgramOperationReportApp.RUN_META_FILESET).getBaseLocation();
        }).list();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      Stream<Location> filteredNsLocations = nsLocations.stream();
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
      final ReportGenerationRequest.ValueFilter<String> nsFilter = namespaceFilter;
      if (nsFilter != null) {
        filteredNsLocations = nsLocations.stream().filter(nsLocation -> nsFilter.apply(nsLocation.getName()));
      }
      List<String> metaFiles = filteredNsLocations.flatMap(nsLocation -> {
        try {
          return nsLocation.list().stream();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).filter(metaFile -> Long.parseLong(FilenameUtils.removeExtension(metaFile.getName())) < reportRequest.getEnd())
        .map(location -> location.toURI().toString()).collect(Collectors.toList());
      reportGen.generateReport(reportRequest, metaFiles, reportOutputPath);
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
