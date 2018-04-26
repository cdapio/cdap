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
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractExtendedSpark;
import co.cask.cdap.api.spark.service.AbstractSparkHttpServiceHandler;
import co.cask.cdap.api.spark.service.SparkHttpServiceContext;
import co.cask.cdap.api.spark.service.SparkHttpServiceHandler;
import co.cask.cdap.report.main.SparkPersistRunRecordMain;
import co.cask.cdap.report.proto.Filter;
import co.cask.cdap.report.proto.FilterDeserializer;
import co.cask.cdap.report.proto.ReportContent;
import co.cask.cdap.report.proto.ReportGenerationInfo;
import co.cask.cdap.report.proto.ReportGenerationRequest;
import co.cask.cdap.report.proto.ReportList;
import co.cask.cdap.report.proto.ReportSaveRequest;
import co.cask.cdap.report.proto.ReportStatus;
import co.cask.cdap.report.proto.ReportStatusInfo;
import co.cask.cdap.report.proto.ReportSummary;
import co.cask.cdap.report.proto.ValueFilter;
import co.cask.cdap.report.util.Constants;
import co.cask.cdap.report.util.ReportField;
import co.cask.cdap.report.util.ReportIds;
import co.cask.cdap.report.util.TriggeringScheduleInfoAdapter;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * A Spark program for generating reports, querying for report statuses, and reading reports.
 */
public class ReportGenerationSpark extends AbstractExtendedSpark {
  private static final Logger LOG = LoggerFactory.getLogger(ReportGenerationSpark.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Filter.class, new FilterDeserializer())
    .create();

  @Override
  protected void configure() {
    setMainClass(SparkPersistRunRecordMain.class);
    addHandlers(new ReportSparkHandler());
  }

  /**
   * A {@link SparkHttpServiceHandler} generating reports, querying for report statuses, and reading reports.
   */
  public static final class ReportSparkHandler extends AbstractSparkHttpServiceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReportSparkHandler.class);
    private static final Type REPORT_GENERATION_REQUEST_TYPE = new TypeToken<ReportGenerationRequest>() { }.getType();
    private static final Type REPORT_SAVED_REQUEST_TYPE = new TypeToken<ReportSaveRequest>() { }.getType();
    private static final String DEFAULT_LIMIT = "10000";
    private SparkSession sparkSession;

    public static final String READ_LIMIT = "readLimit";
    public static final String THREAD_POOL_SIZE = "poolSize";
    public static final String START_FILE = "_START";
    public static final String REPORT_DIR = "reports";
    public static final String COUNT_FILE = "COUNT";
    public static final String SUCCESS_FILE = "_SUCCESS";
    public static final String FAILURE_FILE = "_FAILURE";
    public static final String SAVED_FILE = "_SAVED";
    private static final String TO_BE_DELETED_FILE = "_TO_BE_DELETED";
    // report files will expire after 48 hours after they are generated
    public static final long VALID_DURATION = TimeUnit.DAYS.toSeconds(2);

    private int readLimit;
    private ExecutorService reportGenerationExecutor;

    @Override
    public void initialize(SparkHttpServiceContext context) throws Exception {
      super.initialize(context);
      sparkSession = new SQLContext(getContext().getSparkContext()).sparkSession();
      String readLimitString = context.getRuntimeArguments().get(READ_LIMIT);
      readLimit = readLimitString == null ? 10000 : Integer.valueOf(readLimitString);
      String threadPoolSizeString = context.getRuntimeArguments().get(THREAD_POOL_SIZE);
      reportGenerationExecutor =
        Executors.newFixedThreadPool(threadPoolSizeString == null ? 3 : Integer.valueOf(threadPoolSizeString));
//      try {
//        context.getAdmin().createDataset(ReportGenerationApp.RUN_META_FILESET, FileSet.class.getName(),
//                                         FileSetProperties.builder().build());
//        populateMetaFiles(getDatasetBaseLocation(ReportGenerationApp.RUN_META_FILESET));
//      } catch (InstanceConflictException e) {
//        // It's ok if the dataset already exists
//      }
    }

    @GET
    @Path("/reports")
    public void getReports(HttpServiceRequest request, HttpServiceResponder responder,
                           @QueryParam("offset") @DefaultValue("0") int offset,
                           @QueryParam("limit")  @DefaultValue(DEFAULT_LIMIT) int limit)
      throws IOException {
      Location reportFilesetLocation = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET);
      List<ReportStatusInfo> reportStatuses = new ArrayList<>();
      // The index of the report directory to start reading from, initialized to the given offset
      int idx = offset;
      // TODO: [CDAP-13292] use cache store reportIdDirs
      List<Location> reportIdDirs = new ArrayList<>(reportFilesetLocation.list());
      // sort reportIdDirs directories by the creation time of report ID
      reportIdDirs.sort((loc1, loc2) -> Long.compare(ReportIds.getTime(loc1.getName(), TimeUnit.SECONDS),
                                                     ReportIds.getTime(loc2.getName(), TimeUnit.SECONDS)));

      // Keep add report status information to the list until the index is no longer smaller than
      // the number of report directories or the list is reaching the given limit
      while (idx < reportIdDirs.size() && reportStatuses.size() < limit) {
        Location reportIdDir = reportIdDirs.get(idx++);
        ReportStatus status = getReportStatus(reportIdDir);
        // skip adding reports that are to be deleted
        if (ReportStatus.DELETED.equals(status)) {
          continue;
        }
        String reportId = reportIdDir.getName();
        // Report ID is time based UUID. Get the creation time from the report ID.
        long creationTime = ReportIds.getTime(reportId, TimeUnit.SECONDS);
        // Read the report request from _START file, which was written at the beginning of report generation
        String reportRequestString =
          new String(ByteStreams.toByteArray(reportIdDir.append(START_FILE).getInputStream()), StandardCharsets.UTF_8);
        ReportGenerationRequest reportRequest = GSON.fromJson(reportRequestString, REPORT_GENERATION_REQUEST_TYPE);
        ReportSaveRequest reportSavedInfo = getReportSavedInfo(reportIdDir);
        if (reportSavedInfo != null) {
          reportStatuses.add(new ReportStatusInfo(reportId, reportSavedInfo.getName(), reportSavedInfo.getDescription(),
                                                  creationTime, null, status));
        } else {
          reportStatuses.add(new ReportStatusInfo(reportId, reportRequest.getName(), null,
                                                  creationTime, getExpiryTime(reportIdDir), status));
        }
      }
      responder.sendJson(200, new ReportList(offset, limit, reportIdDirs.size(), reportStatuses));
    }

    @GET
    @Path("/reports/{report-id}")
    public void getReportStatus(HttpServiceRequest request, HttpServiceResponder responder,
                                @PathParam("report-id") String reportId,
                                @QueryParam("share-id") String shareId)
      throws IOException {
      Location reportIdDir = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(reportId);
      if (!reportIdDir.exists()) {
        responder.sendError(404, String.format("Report with id %s does not exist.", reportId));
        return;
      }
      long creationTime = ReportIds.getTime(reportId, TimeUnit.SECONDS);
      ReportStatus status = getReportStatus(reportIdDir);
      if (ReportStatus.DELETED.equals(status)) {
        responder.sendError(404, String.format("Report with id %s no longer exists.", reportId));
        return;
      }
      String error = null;
      ReportSummary summary = null;
      if (ReportStatus.FAILED.equals(status)) {
        error = new String(ByteStreams.toByteArray(reportIdDir.append(FAILURE_FILE).getInputStream()),
                           StandardCharsets.UTF_8);
      } else if (status.equals(ReportStatus.COMPLETED)) {
        String summaryJson =
          new String(ByteStreams.toByteArray(reportIdDir.append(Constants.SUMMARY).getInputStream()),
                     StandardCharsets.UTF_8);
        summary = GSON.fromJson(summaryJson, ReportSummary.class);
      }
      // Read the report request from _START file, which was written at the beginning of report generation
      String reportRequestString =
        new String(ByteStreams.toByteArray(reportIdDir.append(START_FILE).getInputStream()), StandardCharsets.UTF_8);
      ReportGenerationRequest reportRequest = GSON.fromJson(reportRequestString, REPORT_GENERATION_REQUEST_TYPE);
      ReportSaveRequest reportSavedInfo = getReportSavedInfo(reportIdDir);
      ReportGenerationInfo reportGenerationInfo;
      if (reportSavedInfo != null) {
        reportGenerationInfo = new ReportGenerationInfo(reportSavedInfo.getName(), reportSavedInfo.getDescription(),
                                                        creationTime, null, status, error, reportRequest, summary);
      } else {
        reportGenerationInfo = new ReportGenerationInfo(reportRequest.getName(), null, creationTime,
                                                        getExpiryTime(reportIdDir), status, error,
                                                        reportRequest, summary);
      }
      responder.sendJson(reportGenerationInfo);
    }

    @DELETE
    @Path("/reports/{report-id}")
    public void deleteReport(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("report-id") String reportId) throws IOException {
      Location reportIdDir = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(reportId);
      Location deletedFile = reportIdDir.append(TO_BE_DELETED_FILE);
      if (!deletedFile.createNew()) {
        reportIdDir.delete();
        responder.sendError(500, "Failed to delete " + reportId);
        return;
      }
      // Save the time of when this report is marked as to-be-deleted in the _TO_BE_DELETED_FILE file
      try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(deletedFile.getOutputStream(),
                                                                       StandardCharsets.UTF_8), true)) {
        writer.write(Long.toString(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())));
      }
      responder.sendStatus(200);
    }

    @POST
    @Path("/reports/{report-id}/save")
    public void saveReport(HttpServiceRequest request, HttpServiceResponder responder,
                                @PathParam("report-id") String reportId) throws IOException {
      Location reportIdDir = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(reportId);
      if (!reportIdDir.exists()) {
        responder.sendError(404, String.format("Report with id %s does not exist.", reportId));
        return;
      }
      ReportStatus status = getReportStatus(reportIdDir);
      switch (status) {
        case COMPLETED:
          break;
        default:
          responder.sendError(403, "Cannot save the report with status " + status);
          return;
      }
      Location savedFile = reportIdDir.append(SAVED_FILE);
      if (savedFile.exists()) {
        responder.sendError(403, String.format("Report %s is already saved and cannot be saved again.", reportId));
        return;
      }
      if (!savedFile.createNew()) {
        reportIdDir.delete();
        responder.sendError(500, "Failed to create a file for saving the report " + reportId);
        return;
      }
      String requestJson = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      ReportSaveRequest saveRequest = null;
      try {
        saveRequest = GSON.fromJson(requestJson, REPORT_SAVED_REQUEST_TYPE);
      } catch (JsonSyntaxException e) {
        responder.sendError(400, "Failed to parse the report saving request: " + e);
        return;
      }

      // Save the report generation request in the _SAVED file
      try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(savedFile.getOutputStream(),
                                                                       StandardCharsets.UTF_8), true)) {
        writer.write(requestJson);
      }
      responder.sendString(200,
                           String.format("Report is saved successfully with the name: '%s' and description: '%s' ",
                                         saveRequest.getName(), saveRequest.getDescription()),
                           StandardCharsets.UTF_8);
    }

    @GET
    @Path("reports/{report-id}/details")
    public void getReportDetails(HttpServiceRequest request, HttpServiceResponder responder,
                                 @PathParam("report-id") String reportId,
                                 @QueryParam("offset") @DefaultValue("0") long offset,
                                 @QueryParam("limit") @DefaultValue(DEFAULT_LIMIT) int limit,
                                 @QueryParam("share-id") String shareId) throws IOException {
      if (offset < 0) {
        responder.sendError(400, "offset cannot be negative");
        return;
      }
      if (limit <= 0) {
        responder.sendError(400, "limit must be a positive integer");
        return;
      }
      if (limit > readLimit) {
        responder.sendError(400, "limit must cannot be larger than " + readLimit);
        return;
      }
      Location reportIdDir = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(reportId);
      if (!reportIdDir.exists()) {
        responder.sendError(404, String.format("Report with id %s does not exist.", reportId));
        return;
      }
      // Get the status of the report and only COMPLETED report can be read
      ReportStatus status = getReportStatus(reportIdDir);
      switch (status) {
        case RUNNING:
          responder.sendError(202, String.format("Report %s is still being generated, please retry later.", reportId));
          return;
        case FAILED:
          responder.sendError(400, String.format("Reading details of the report %s with failed status is not allowed.",
                                                 reportId));
          return;
        case DELETED:
          responder.sendError(404, String.format("Report with id %s no longer exists.", reportId));
          return;
        case COMPLETED:
          break;
        default: // this should never happen
          responder.sendError(500, String.format("Unable to read report %s with unknown status %s.", reportId, status));
          return;
      }
      List<String> reportRecords = new ArrayList<>();
      long lineCount = 0;
      Location reportDir = reportIdDir.append(REPORT_DIR);
      // TODO: [CDAP-13290] reports should be in avro format instead of json text;
      // TODO: [CDAP-13291] need to support reading multiple report files
      Optional<Location> reportFile = reportDir.list().stream().filter(l -> l.getName().endsWith(".json")).findFirst();
      // TODO: [CDAP-13292] use cache to store content of the reports
      // Read the report file and add lines starting from the position of offset to the result until the result reaches
      // the limit
      if (reportFile.isPresent()) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(reportFile.get().getInputStream(),
                                                                          StandardCharsets.UTF_8))) {
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
      }
      // Get the total number of records from the COUNT file
      String total =
        new String(ByteStreams.toByteArray(reportIdDir.append(COUNT_FILE).getInputStream()), StandardCharsets.UTF_8);
      responder.sendString(200, new ReportContent(offset, limit, Integer.parseInt(total), reportRecords).toJson(),
                           StandardCharsets.UTF_8);
    }

    @POST
    @Path("/reports")
    public void executeReportGeneration(HttpServiceRequest request, HttpServiceResponder responder)
      throws IOException {
      String requestJson = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      LOG.debug("Received report generation request {}", requestJson);
      ReportGenerationRequest reportRequest;
      try {
        reportRequest = decodeRequestBody(requestJson, REPORT_GENERATION_REQUEST_TYPE);
        reportRequest.validate();
      } catch (IllegalArgumentException e) {
        responder.sendError(400, e.getMessage());
        return;
      }

      String reportId = ReportIds.generate().toString();
      Location reportIdDir = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(reportId);
      if (!reportIdDir.mkdirs()) {
        responder.sendError(500, "Failed to create a new directory for report " + reportId);
        return;
      }
      LOG.debug("Created report base directory {} for report {}", reportIdDir, reportId);
      // Create a _START file to indicate the start of report generation
      Location startFile = reportIdDir.append(START_FILE);
      if (!startFile.createNew()) {
        reportIdDir.delete();
        responder.sendError(500, "Failed to create a _START file for report " + reportId);
        return;
      }
      // Save the report generation request in the _START file
      try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(startFile.getOutputStream(),
                                                                       StandardCharsets.UTF_8), true)) {
        writer.write(requestJson);
      }
      LOG.debug("Wrote to startFile {}", startFile.toURI());
      // Generate the report asynchronously
      if (reportGenerationExecutor == null) {
        throw new RuntimeException("reportGenerationExecutor is null");
      }
      reportGenerationExecutor.execute(() -> {
        tryGenerateReport(reportRequest, reportIdDir, reportId);
      });
      responder.sendJson(200, ImmutableMap.of("id", reportId));
    }

    /**
     * Delegates report generation to {@link #generateReport(ReportGenerationRequest, Location)} and catch any
     * errors in report generation and write them to a _FAILURE file.
     *
     * @param reportRequest the request to generate report
     * @param reportIdDir the location of the directory which will be the parent directory of _FAILURE file
     *                    and will be passed to {@link #generateReport(ReportGenerationRequest, Location)}
     * @param reportId the ID of the report being generated
     */
    private void tryGenerateReport(ReportGenerationRequest reportRequest, Location reportIdDir, String reportId) {
      try {
        generateReport(reportRequest, reportIdDir);
      } catch (Throwable t) {
        LOG.error("Failed to generate report {}", reportId, t);
        try {
          // Write to the failure file in case of any exception occurs during report generation
          Location failureFile = reportIdDir.append(FAILURE_FILE);
          if (!failureFile.createNew()) {
            // delete the reportIdDir in case of failing to create failure file, otherwise the report generation job
            // will be seen as still running
            reportIdDir.delete();
            LOG.error("Failed to create a _FAILURE file for report {}", reportId);
            return;
          }
          try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(failureFile.getOutputStream(),
                                                                           StandardCharsets.UTF_8), true)) {
            writer.println(t.toString());
            t.printStackTrace(writer);
          }
        } catch (Throwable t2) {
          LOG.error("Failed to write cause of failure to file for report {}", reportId, t2);
        }
      }
    }

    /**
     * Generates report files according to the given request and write them to the given location.
     * Program run meta files are first filtered to exclude unnecessary files for report generation,
     * and send the paths of qualified run meta files to {@link ReportGenerationHelper#generateReport}
     * that actually launches a Spark job to generate reports.
     *
     * @param reportRequest the request to generate report
     * @param reportIdDir the location of the directory where the report files directory, COUNT file,
     *                    and _SUCCESS file will be created.
     */
    private void generateReport(ReportGenerationRequest reportRequest, Location reportIdDir) throws IOException {
      Location baseLocation = Transactionals.execute(getContext(), context -> {
        return context.<FileSet>getDataset(ReportGenerationApp.RUN_META_FILESET).getBaseLocation();
      });
      // Get a list of directories of all namespaces under RunMetaFileset base location
      List<Location> nsLocations;
      nsLocations = baseLocation.list();
      // Get the namespace filter from the request if it exists
      final ValueFilter<String> nsFilter = getNamespaceFilterIfExists(reportRequest);
      Stream<Location> filteredNsLocations = nsLocations.stream();
      // If the namespace filter exists, apply the filter to get filtered namespace directories
      if (nsFilter != null) {
        filteredNsLocations = nsLocations.stream().filter(nsLocation -> nsFilter.apply(nsLocation.getName()));
      }
      // Iterate through all qualified namespaces directories to get program run meta files
      Stream<Location> metaFiles = filteredNsLocations.flatMap(nsLocation -> {
        try {
          List<Location> metaFileLocations = nsLocation.list();
          LOG.debug("Files under namespace {}: {}", nsLocation.getName(), metaFileLocations);
          return metaFileLocations.stream();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      // Program run meta files are in avro format. Each file is named by the earliest program run meta record
      // in the file, so exclude the files with no record earlier than the end of query time range.
      List<String> metaFilePaths = metaFiles.filter(metaFile -> {
        String fileName = metaFile.getName();
        return fileName.endsWith(".avro")
          && Long.parseLong(fileName.substring(0, fileName.indexOf("-")))
          < TimeUnit.SECONDS.toMillis(reportRequest.getEnd());
      }).map(location -> location.toURI().toString()).collect(Collectors.toList());
      LOG.debug("Filtered meta files {}", metaFiles);
      // Generate the report with the request and program run meta files
      ReportGenerationHelper.generateReport(sparkSession, reportRequest, metaFilePaths, reportIdDir);
    }

    /**
     * Get the value filter on namespace from the report generation request
     *
     * @param request the request to get the namespace filter from
     * @return the namespace filter found or {@code null}
     */
    @Nullable
    private static ValueFilter<String> getNamespaceFilterIfExists(ReportGenerationRequest request) {
      if (request.getFilters() != null) {
        for (Filter filter : request.getFilters()) {
          if (ReportField.NAMESPACE.getFieldName().equals(filter.getFieldName())) {
            // ReportGenerationRequest is validated to contain only one filter for namespace field
            LOG.debug("Found namespace filter {}", filter);
            return (ValueFilter<String>) filter;
          }
        }
      }
      return null;
    }

    @Nullable
    private static Long getExpiryTime(Location reportIdDir) throws IOException {
      Location successFile = reportIdDir.append(SUCCESS_FILE);
      return successFile.exists() ? TimeUnit.MILLISECONDS.toSeconds(successFile.lastModified()) + VALID_DURATION : null;
    }

    @Nullable
    private static ReportSaveRequest getReportSavedInfo(Location reportIdDir) throws IOException {
      Location savedFile = reportIdDir.append(SAVED_FILE);
      if (!savedFile.exists()) {
       return null;
      }
      String reportSavedRequestString =
        new String(ByteStreams.toByteArray(savedFile.getInputStream()), StandardCharsets.UTF_8);
      if (reportSavedRequestString.isEmpty()) {
        return null;
      }
      return GSON.fromJson(reportSavedRequestString, REPORT_SAVED_REQUEST_TYPE);
    }

    /**
     * Returns the status of the report generation by checking the presence of the success file or the failure file.
     * If neither of these files exists, the report generation is still running.
     *
     * TODO: [CDAP-13215] failure file may not be written if the Spark program is killed. Status of killed
     * report generation job might be returned as RUNNING
     *
     * @param reportIdDir the base directory with report ID as directory name
     * @return status of the report generation
     */
    private static ReportStatus getReportStatus(Location reportIdDir) throws IOException {
      if (reportIdDir.append(TO_BE_DELETED_FILE).exists()) {
        return ReportStatus.DELETED;
      }
      if (reportIdDir.append(FAILURE_FILE).exists()) {
        return ReportStatus.FAILED;
      }
      if (reportIdDir.append(SUCCESS_FILE).exists()) {
        return ReportStatus.COMPLETED;
      }
      return ReportStatus.RUNNING;
    }

    private Location getDatasetBaseLocation(String datasetName) {
      return Transactionals.execute(getContext(), context -> {
        return context.<FileSet>getDataset(datasetName).getBaseLocation();
      });
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
