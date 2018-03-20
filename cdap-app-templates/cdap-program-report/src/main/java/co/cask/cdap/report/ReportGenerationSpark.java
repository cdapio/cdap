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

import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractExtendedSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.api.spark.service.AbstractSparkHttpServiceHandler;
import co.cask.cdap.api.spark.service.SparkHttpServiceContext;
import co.cask.cdap.api.spark.service.SparkHttpServiceHandler;
import co.cask.cdap.report.proto.FilterDeserializer;
import co.cask.cdap.report.proto.ReportContent;
import co.cask.cdap.report.proto.ReportGenerationInfo;
import co.cask.cdap.report.proto.ReportGenerationRequest;
import co.cask.cdap.report.proto.ReportList;
import co.cask.cdap.report.proto.ReportStatus;
import co.cask.cdap.report.proto.ReportStatusInfo;
import co.cask.cdap.report.util.ProgramRunMetaFileUtil;
import co.cask.cdap.report.util.ReportField;
import co.cask.cdap.report.util.ReportIds;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
  private static final String START_FILE = "_START";
  private static final String REPORT_DIR = "report";
  private static final String SUCCESS_FILE = "_SUCCESS";
  private static final String FAILURE_FILE = "_FAILURE";

  private final Map<String, Long> runningReports = new ConcurrentHashMap<>();

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
    private static final Type REPORT_GENERATION_REQUEST_TYPE = new TypeToken<ReportGenerationRequest>() {
    }.getType();

    private ReportGenerationHelper reportGenerationHelper;

    @Override
    public void initialize(SparkHttpServiceContext context) throws Exception {
      super.initialize(context);
      reportGenerationHelper = new ReportGenerationHelper(getContext().getSparkContext());
      try {
        // TODO: temporarily create the run meta fileset and generate mock program run meta files here.
        // Will remove once the TMS subscriber writing to the run meta fileset is implemented.
        context.getAdmin().createDataset(ReportGenerationApp.RUN_META_FILESET, FileSet.class.getName(),
                                         FileSetProperties.builder().build());
        ProgramRunMetaFileUtil.populateMetaFiles(getDatasetBaseLocation(ReportGenerationApp.RUN_META_FILESET));
      } catch (InstanceConflictException e) {
        // It's ok if the dataset already exists
      }
    }

    @GET
    @Path("/reports")
    public void getReports(HttpServiceRequest request, HttpServiceResponder responder,
                           @QueryParam("offset") String offsetString,
                           @QueryParam("limit") String limitString)
      throws IOException {
      int offset = (offsetString == null || offsetString.isEmpty()) ? 0 : Integer.parseInt(offsetString);
      int limit = (limitString == null || limitString.isEmpty()) ? Integer.MAX_VALUE : Integer.parseInt(limitString);
      Location reportFilesetLocation = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET);
      List<ReportStatusInfo> reportStatuses = new ArrayList<>();
      int idx = offset;
      List<Location> reportBaseDirs = reportFilesetLocation.list();
      while (idx < reportBaseDirs.size() && reportStatuses.size() < limit) {
        Location reportBaseDir = reportBaseDirs.get(idx++);
        String reportId = reportBaseDir.getName();
        long creationTime = ReportIds.getTime(reportId, TimeUnit.SECONDS);
        reportStatuses.add(new ReportStatusInfo(reportId, creationTime, getReportStatus(reportBaseDir)));
      }
      responder.sendJson(200, new ReportList(offset, limit, reportBaseDirs.size(), reportStatuses));
    }

    @GET
    @Path("/reports/{report-id}")
    public void getReportStatus(HttpServiceRequest request, HttpServiceResponder responder,
                                @PathParam("report-id") String reportId,
                                @QueryParam("share-id") String shareId)
      throws IOException {
      Location reportBaseDir = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(reportId);
      if (!reportBaseDir.exists()) {
        responder.sendError(404, String.format("Report with id %s does not exist.", reportId));
        return;
      }
      long creationTime = ReportIds.getTime(reportId, TimeUnit.SECONDS);
      String reportRequest =
        new String(ByteStreams.toByteArray(reportBaseDir.append(START_FILE).getInputStream()), Charsets.UTF_8);
      responder.sendJson(new ReportGenerationInfo(creationTime, getReportStatus(reportBaseDir), reportRequest));
    }

    @GET
    @Path("reports/{report-id}/runs")
    public void getReportRuns(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("report-id") String reportId,
                              @QueryParam("offset") String offsetString,
                              @QueryParam("limit") String limitString,
                              @QueryParam("share-id") String shareId) throws IOException {
      long offset = (offsetString == null || offsetString.isEmpty()) ? 0 : Long.parseLong(offsetString);
      int limit = (limitString == null || limitString.isEmpty()) ? Integer.MAX_VALUE : Integer.parseInt(limitString);
      if (offset < 0) {
        responder.sendError(400, "offset cannot be negative");
        return;
      }
      if (limit <= 0) {
        responder.sendError(400, "limit must be a positive integer");
        return;
      }

      Location reportBaseDir = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(reportId);
      if (!reportBaseDir.exists()) {
        responder.sendError(404, String.format("Report with id %s does not exist.", reportId));
        return;
      }
      ReportStatus status = getReportStatus(reportBaseDir);
      if (!ReportStatus.COMPLETED.equals(status)) {
        responder.sendError(400, String.format("Report with id %s with status %s cannot be read.", reportId, status));
        return;
      }
      List<String> reportRecords = new ArrayList<>();
      long lineCount = 0;
      Location reportDir = reportBaseDir.append(REPORT_DIR);
      Location reportFile = null;
      // TODO: assume only one report file for now
      for (Location file : reportDir.list()) {
        if (file.getName().endsWith(".json")) {
          reportFile = file;
          break;
        }
      }
      if (reportFile == null) {
        responder.sendError(500, "No files found for report " + reportId);
        return;
      }

      try (BufferedReader br = new BufferedReader(new InputStreamReader(reportFile.getInputStream()))) {
        String line;
        while ((line = br.readLine()) != null) {
          // skip lines before the offset
          if (lineCount++ < offset) {
            continue;
          }
          if (reportRecords.size() == limit) {
            break;
          }
          reportRecords.add(line);
        }
      }
      String total =
        new String(ByteStreams.toByteArray(reportDir.append(SUCCESS_FILE).getInputStream()), Charsets.UTF_8);
      responder.sendJson(200, new ReportContent(offset, limit, Integer.parseInt(total), reportRecords));
    }

    @POST
    @Path("/reports")
    public void executeReportGeneration(HttpServiceRequest request, HttpServiceResponder responder)
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

      String reportId = ReportIds.generate().toString();
      Location reportBaseDir = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(reportId);
      reportBaseDir.mkdirs();
      LOG.info("reportBaseDir {} exists='{}', isDir='{}'", reportBaseDir, reportBaseDir.exists(),
               reportBaseDir.isDirectory());
      Location startFile = reportBaseDir.append(START_FILE);
      try {
        startFile.createNew();

        LOG.info("startFile {} exists='{}', isDir='{}'", startFile, startFile.exists(), startFile.isDirectory());
      } catch (IOException e) {
        LOG.error("Failed to create startFile {}", startFile.toURI(), e);
        throw e;
      }
      try (PrintWriter writer = new PrintWriter(startFile.getOutputStream())) {
        writer.write(requestJson);
      } catch (IOException e) {
        LOG.error("Failed to write to startFile {}", startFile.toURI(), e);
        throw e;
      }
      Location reportDir = reportBaseDir.append(REPORT_DIR);
      LOG.info("Wrote to startFile {}", startFile.toURI());
      Executors.newSingleThreadExecutor().submit(() -> {
        try {
          generateReport(reportRequest, reportDir);
        } catch (Throwable t) {
          LOG.error("Failed to generate report {}", reportId, t);
          try {
            Location failureFile = reportBaseDir.append(FAILURE_FILE);
            failureFile.createNew();
            try (PrintWriter writer = new PrintWriter(failureFile.getOutputStream())) {
              writer.println(t.toString());
              t.printStackTrace(writer);
            }
          } catch (Throwable t2) {
            LOG.error("Failed to write cause of failure to file for report {}", reportId, t2);
            throw new RuntimeException("Failed to write cause of failure to file for report " + reportId, t2);
          }
        }
      });
      responder.sendJson(200, GSON.toJson(ImmutableMap.of("id", reportId)));
    }

    private void generateReport(ReportGenerationRequest reportRequest, Location reportDir) {
      Location baseLocation = Transactionals.execute(getContext(), context -> {
        return context.<FileSet>getDataset(ReportGenerationApp.RUN_META_FILESET).getBaseLocation();
      });
      List<Location> nsLocations;
      try {
        nsLocations = baseLocation.list();
      } catch (IOException e) {
        LOG.error("Failed to get namespace locations from {}", baseLocation.toURI().toString());
        throw new RuntimeException(e);
      }
      LOG.info("Existing namespaces: {}",
               nsLocations.stream().map(location -> location.getName()).collect(Collectors.toList()));
      Stream<Location> filteredNsLocations = nsLocations.stream();
      ReportGenerationRequest.ValueFilter<String> namespaceFilter = null;
      if (reportRequest.getFilters() != null) {
        for (ReportGenerationRequest.Filter filter : reportRequest.getFilters()) {
          if (ReportField.NAMESPACE.getFieldName().equals(filter.getFieldName())) {
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
          LOG.info("Files under ns={}: {}", nsLocation.getName(), nsLocation.list());
          return nsLocation.list().stream();
        } catch (IOException e) {
          LOG.error("Failed to list files under namespace {}", nsLocation.toURI().toString(), e);
          throw new RuntimeException(e);
        }
      }).filter(metaFile -> {
        String fileName = metaFile.getName();
        return fileName.endsWith(".avro")
          && Long.parseLong(fileName.substring(0, fileName.indexOf(".avro"))) < reportRequest.getEnd();
      }).map(location -> location.toURI().toString()).collect(Collectors.toList());
      LOG.info("Filtered meta files {}", metaFiles);
      long total = reportGenerationHelper.generateReport(reportRequest, metaFiles, reportDir.toURI().toString());
      try (PrintWriter writer = new PrintWriter(reportDir.append(SUCCESS_FILE).getOutputStream())) {
        writer.write(Long.toString(total));
      } catch (IOException e) {
        LOG.error("Failed to write to {} in {}", SUCCESS_FILE, reportDir.toURI().toString(), e);
        throw new RuntimeException(e);
      }
    }

    private Location getDatasetBaseLocation(String datasetName) {
      return Transactionals.execute(getContext(), context -> {
        return context.<FileSet>getDataset(datasetName).getBaseLocation();
      });
    }

    private ReportStatus getReportStatus(Location reportBaseDir) throws IOException {
      if (reportBaseDir.append(REPORT_DIR).append(SUCCESS_FILE).exists()) {
        return ReportStatus.COMPLETED;
      }
      if (reportBaseDir.append(FAILURE_FILE).exists()) {
        return ReportStatus.FAILED;
      }
      return ReportStatus.RUNNING;
    }

    private  <T> T decodeRequestBody(String request, Type type) {
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
}
