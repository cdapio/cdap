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

package io.cdap.cdap.report;

import static io.cdap.cdap.report.util.Constants.LocationName;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.Transactionals;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.spark.AbstractExtendedSpark;
import io.cdap.cdap.api.spark.service.AbstractSparkHttpServiceHandler;
import io.cdap.cdap.api.spark.service.SparkHttpServiceContext;
import io.cdap.cdap.api.spark.service.SparkHttpServiceHandler;
import io.cdap.cdap.report.main.SparkPersistRunRecordMain;
import io.cdap.cdap.report.proto.Filter;
import io.cdap.cdap.report.proto.FilterCodec;
import io.cdap.cdap.report.proto.ReportContent;
import io.cdap.cdap.report.proto.ReportGenerationInfo;
import io.cdap.cdap.report.proto.ReportGenerationRequest;
import io.cdap.cdap.report.proto.ReportIdentifier;
import io.cdap.cdap.report.proto.ReportList;
import io.cdap.cdap.report.proto.ReportMetaInfo;
import io.cdap.cdap.report.proto.ReportSaveRequest;
import io.cdap.cdap.report.proto.ReportStatus;
import io.cdap.cdap.report.proto.ReportStatusInfo;
import io.cdap.cdap.report.proto.ShareId;
import io.cdap.cdap.report.proto.ValueFilter;
import io.cdap.cdap.report.proto.summary.ReportSummary;
import io.cdap.cdap.report.util.Constants;
import io.cdap.cdap.report.util.ReportField;
import io.cdap.cdap.report.util.ReportIds;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.SQLContext;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Spark program for generating reports, querying for report statuses, and reading reports.
 */
public class ReportGenerationSpark extends AbstractExtendedSpark {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(Filter.class, new FilterCodec())
      .disableHtmlEscaping()
      .create();
  private static final Logger LOG = LoggerFactory.getLogger(ReportGenerationSpark.class);
  private static final ExecutorService REPORT_EXECUTOR =
      new ThreadPoolExecutor(0, 3, 60L, TimeUnit.SECONDS,
          new SynchronousQueue<>(), Threads.createDaemonThreadFactory("report-generation-%d"));
  // User name authenticated and passed down by CDAP-Router using this key in header
  private static final String USER_ID = "CDAP-UserId";
  // Default user id will be used on non-authenticated cluster
  // add security enabled checks after CDAP-13672 is resolved
  private static final String DEFAULT_USER_ID = "system";

  @Override
  protected void configure() {
    setMainClass(SparkPersistRunRecordMain.class);
    addHandlers(new ReportSparkHandler());
    String version = getSparkVersion();
    if (!isSparkVersionSupported(version)) {
      throw new RuntimeException(
          String.format(
              "Spark Version %s is not supported, Please upgrade to Spark version 2.1 or later",
              version));
    }
  }

  /**
   * Spark version returned using SparkContext#version() is set by org.apache.spark.SparkBuildInfo
   * in a similar manner by loading the spark-version-info.properties file and getting the property
   * version.
   *
   * @return version string or null if unable to load the file due to error
   */
  @Nullable
  private String getSparkVersion() {
    ClassLoader classLoader = org.apache.spark.SparkContext.class.getClassLoader();
    try (InputStream inputStream = openResource(classLoader, "spark-version-info.properties")) {
      if (inputStream == null) {
        return null;
      }
      Properties properties = new Properties();
      properties.load(inputStream);
      String version = properties.getProperty("version");
      return version;
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * ReportGeneration is supported only from Spark version 2.1 currrently, we return false for
   * versions lower than 2.1
   */
  private boolean isSparkVersionSupported(@Nullable String version) {
    LOG.debug("Spark version is {}", version);
    // if version cannot be loaded assume its supported
    if (version == null || version.isEmpty()) {
      return true;
    }
    Iterator<String> versionSplits = Splitter.on('.').split(version).iterator();
    if (!versionSplits.hasNext()) {
      return false;
    }
    int majorVersion = Integer.parseInt(versionSplits.next());
    if (majorVersion < 2) {
      return false;
    }
    if (!versionSplits.hasNext()) {
      return false;
    }
    int minorVersion = Integer.parseInt(versionSplits.next());
    if (minorVersion < 1) {
      return false;
    }
    return true;
  }

  /**
   * Returns an {@link InputStream} to the given resource by looking it up from the given {@link
   * ClassLoader} or {@code null} if the given resource is not found.
   */
  @Nullable
  private InputStream openResource(ClassLoader classLoader, String resourceName)
      throws IOException {
    URL resource = classLoader.getResource(resourceName);
    if (resource == null) {
      return null;
    }

    // (CDAP-14062) Need to disable connection cache to workaround a Java bug.
    // When multithreads are opening JarURLConnections pointing to the same jar file,
    // closing one might affect the other.
    URLConnection urlConn = resource.openConnection();
    urlConn.setUseCaches(false);
    return urlConn.getInputStream();
  }

  /**
   * A {@link SparkHttpServiceHandler} generating reports, querying for report statuses, and reading
   * reports.
   */
  public static final class ReportSparkHandler extends AbstractSparkHttpServiceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReportSparkHandler.class);
    private static final Type REPORT_GENERATION_REQUEST_TYPE = new TypeToken<ReportGenerationRequest>() {
    }.getType();
    private static final Type REPORT_SAVE_REQUEST_TYPE = new TypeToken<ReportSaveRequest>() {
    }.getType();
    private static final String DEFAULT_LIMIT = "10000";
    private static final String READ_LIMIT = "readLimit";
    private static final String START_FILE = "_START";
    private static final String FAILURE_FILE = "_FAILURE";
    private static final String SAVED_FILE = "_SAVED";

    private int readLimit;
    private SQLContext sqlContext;
    private long reportsExpiryTimeMillis;

    @Override
    public void initialize(SparkHttpServiceContext context) throws Exception {
      super.initialize(context);
      sqlContext = new SQLContext(getContext().getSparkContext());
      Map<String, String> runtimeArguments = context.getRuntimeArguments();
      readLimit = Integer.parseInt(runtimeArguments.getOrDefault(READ_LIMIT, "10000"));
      String expiryTimeInSecondsString =
          runtimeArguments.getOrDefault(Constants.Report.REPORT_EXPIRY_TIME_SECONDS,
              Constants.Report.DEFAULT_REPORT_EXPIRY_TIME_SECONDS);
      reportsExpiryTimeMillis = TimeUnit.SECONDS.toMillis(
          Long.parseLong(expiryTimeInSecondsString));
    }

    /**
     * Read the secure key file to read the security key bytes and initialize Cipher using AES and
     * the mode encryption/decryption is based on the parameter cipherMode.
     *
     * @param cipherMode either encrypt or decrypt mode
     * @return Initialized Cipher
     * @throws IllegalStateException If the key file doesn't exist
     * @throws IOException If there are any exception while reading the file
     * @throws NoSuchAlgorithmException if encryption algorithm is not found
     * @throws NoSuchPaddingException could be thrown while getting cipher instance for
     *     encryption algorithm
     * @throws InvalidKeyException could be thrown while getting cipher instance for encryption
     *     algorithm
     */
    private Cipher initializeCipher(int cipherMode)
        throws IllegalStateException, IOException, NoSuchPaddingException,
        NoSuchAlgorithmException, InvalidKeyException {
      Location keyLocation =
          getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(
              Constants.Security.KEY_FILE_NAME);
      if (!keyLocation.exists()) {
        throw new IllegalStateException("Security Key file doesn't exist, cannot share reports");
      }
      try (InputStream inputStream = keyLocation.getInputStream()) {
        byte[] keyBytes = ByteStreams.toByteArray(inputStream);
        Cipher cipher = Cipher.getInstance(Constants.Security.ENCRYPTION_ALGORITHM);
        SecretKeySpec secretKeySpec = new SecretKeySpec(keyBytes,
            Constants.Security.ENCRYPTION_ALGORITHM);
        cipher.init(cipherMode, secretKeySpec);
        return cipher;
      }
    }

    @GET
    @Path("/health")
    public void healthCheck(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    }

    @GET
    @Path("/reports")
    public void getReports(HttpServiceRequest request, HttpServiceResponder responder,
        @QueryParam("offset") @DefaultValue("0") int offset,
        @QueryParam("limit") @DefaultValue(DEFAULT_LIMIT) int limit) {
      ReportList reportList;
      try {
        reportList = getReportList(offset, limit, getUserName(request.getAllHeaders()));
      } catch (Exception e) {
        LOG.error("Failed to list reports.", e);
        responder.sendError(500,
            String.format("Failed to list reports because of error: %s", e.getMessage()));
        return;
      }
      responder.sendJson(200, reportList);
    }

    private String getUserName(Map<String, List<String>> headers) {
      return headers.getOrDefault(USER_ID, Collections.singletonList(DEFAULT_USER_ID)).stream()
          .findFirst().get();
    }

    /**
     * Gets a list of report statuses from all the unexpired reports starting from the given offset
     * position, with at most the limit number of report statuses in the list returned.
     *
     * @param offset the starting index position in the unexpired report directories
     * @param limit the maximum number of report statuses returned in the result
     * @param user name of the user requesting reports
     * @return a list of report statuses of unexpired reports
     * @throws Exception if getting status of a report fails
     */
    private ReportList getReportList(int offset, int limit, String user) throws Exception {
      Location reportFilesetLocation = getDatasetBaseLocation(
          ReportGenerationApp.REPORT_FILESET).append(user);
      // we create directory for the user when the user generates a report first,
      // return empty list if location doesn't exist.
      if (!reportFilesetLocation.exists()) {
        return new ReportList(offset, limit, 0, Collections.emptyList());
      }
      List<ReportStatusInfo> reportStatuses = new ArrayList<>();
      // TODO: [CDAP-13292] use cache to store reportIdDirs
      List<Location> reportIdDirs = new ArrayList<>(reportFilesetLocation.list());
      // sort reportIdDirs directories by the creation time of report ID in DESCENDING order
      reportIdDirs.sort(
          (loc1, loc2) -> Long.compare(ReportIds.getTime(loc2.getName(), TimeUnit.SECONDS),
              ReportIds.getTime(loc1.getName(), TimeUnit.SECONDS)));
      // the index of current report directory among the unexpired report directories
      int existingReportDirIdx = 0;
      Iterator<Location> reportIdDirIter = reportIdDirs.iterator();
      // Keep adding report status information to the list until the index is no longer smaller than
      // the number of report directories or the list is reaching the given limit
      while (reportIdDirIter.hasNext() && reportStatuses.size() < limit) {
        Location reportIdDir = reportIdDirIter.next();
        String reportId = reportIdDir.getName();
        ReportMetaInfo metaInfo = getReportMetaInfo(reportId, reportIdDir,
            getReportRequest(reportIdDir));
        // skip adding reports that are expired, or the index of the current directory among existing directories
        // is smaller than the given offset
        if (ReportStatus.EXPIRED.equals(metaInfo.getStatus()) || existingReportDirIdx++ < offset) {
          continue;
        }
        reportStatuses.add(new ReportStatusInfo(reportId, metaInfo));
      }
      return new ReportList(offset, limit, reportStatuses.size(), reportStatuses);
    }

    @POST
    @Path("/reports/{report-id}/share")
    public void shareReport(HttpServiceRequest request, HttpServiceResponder responder,
        @PathParam("report-id") String reportId) {
      Location reportIdDir;
      try {
        String userName = getUserName(request.getAllHeaders());
        reportIdDir = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(userName)
            .append(reportId);
        if (!reportIdDir.exists()) {
          responder.sendError(404,
              String.format("Invalid report-id %s, report does not exist", reportId));
          return;
        }
        String shareId = encodeShareId(new ReportIdentifier(userName, reportId));
        responder.sendJson(200, new ShareId(shareId), ShareId.class, GSON);
      } catch (IOException | GeneralSecurityException e) {
        LOG.error("Failed to read report with id {}", reportId, e);
        responder.sendError(500,
            String.format("Failed to read report with id %s because of error: %s",
                reportId, e.getMessage()));
        return;
      }
    }

    private String encodeShareId(ReportIdentifier reportIdentifier)
        throws GeneralSecurityException, IOException {
      byte[] response = GSON.toJson(reportIdentifier).getBytes(StandardCharsets.UTF_8);
      Cipher encryptionCipher = initializeCipher(Cipher.ENCRYPT_MODE);
      byte[] cipherReponse = encryptionCipher.doFinal(response);
      return Base64.getUrlEncoder().encodeToString(cipherReponse);
    }

    private ReportIdentifier decodeShareId(String shareId)
        throws GeneralSecurityException, IOException {
      byte[] decodedCipherBytes = Base64.getUrlDecoder().decode(shareId.getBytes());
      Cipher decryptionCipher = initializeCipher(Cipher.DECRYPT_MODE);
      byte[] decodedBytes = decryptionCipher.doFinal(decodedCipherBytes);
      String decodedString = new String(decodedBytes, 0, decodedBytes.length,
          StandardCharsets.UTF_8);
      return GSON.fromJson(decodedString, ReportIdentifier.class);
    }

    @GET
    @Path("/reports/info")
    public void getReportStatus(HttpServiceRequest request, HttpServiceResponder responder,
        @QueryParam("report-id") String reportId,
        @QueryParam("share-id") String shareId) {

      Location reportIdDir;
      ReportIdentifier reportIdentifier;
      String idMessage;
      try {
        reportIdentifier = validateAndGetReportIdentifier(reportId, request.getAllHeaders(),
            shareId);
        idMessage = shareId == null ? String.format("ReportId : %s", reportId) :
            String.format("ShareId : %s", shareId);
      } catch (IllegalArgumentException iae) {
        responder.sendError(400, iae.getMessage());
        return;
      } catch (GeneralSecurityException | IOException e) {
        responder.sendError(500, String.format(
            "Error while decoding shareId %s, due to exception : ", shareId, e.getMessage()));
        return;
      }

      try {
        reportIdDir = getLocationFromReportIdentifier(reportIdentifier);
      } catch (IOException e) {
        LOG.error("Failed to get location for report with {}", idMessage, e);
        responder.sendError(500,
            String.format("Failed to get location for report with %s because of error: %s",
                idMessage, e.getMessage()));
        return;
      }
      try {
        if (!reportIdDir.exists()) {
          responder.sendError(404, String.format("Report with %s does not exist.", idMessage));
          return;
        }
      } catch (IOException e) {
        LOG.error("Failed to check whether the location of report with {} exists", idMessage, e);
        responder.sendError(500,
            String.format("Failed to check whether the location of report with %s exists"
                + " because of error: %s", idMessage, e.getMessage()));
        return;
      }
      ReportGenerationInfo reportGenerationInfo;
      try {
        if (reportId == null) {
          reportId = decodeShareId(shareId).getReportId();
        }
        reportGenerationInfo = getReportGenerationInfo(reportId, reportIdDir);
      } catch (Exception e) {
        LOG.error("Failed to get the status for report with {}.", idMessage, e);
        responder.sendError(500, String.format("Failed to get the status for report with %s"
            + " because of error: %s", idMessage, e.getMessage()));
        return;
      }
      // expired report is considered as deleted
      if (ReportStatus.EXPIRED.equals(reportGenerationInfo.getStatus())) {
        responder.sendError(404, String.format("Report with %s does not exist.", idMessage));
        return;
      }
      responder.sendJson(200, reportGenerationInfo);
    }

    /**
     * Gets the report generation information of the given report id with the information stored in
     * files under the given directory
     *
     * @param reportId the id of the report
     * @param reportIdDir the location of the directory containing files with the report
     *     generation information
     * @return the report generation information of the given report id
     */
    private ReportGenerationInfo getReportGenerationInfo(String reportId, Location reportIdDir)
        throws Exception {
      ReportGenerationRequest reportRequest = getReportRequest(reportIdDir);
      ReportMetaInfo metaInfo = getReportMetaInfo(reportId, reportIdDir, reportRequest);
      // if the report generation completed, read the summary from _SUMMARY file and include the summary
      // in the response
      if (ReportStatus.COMPLETED.equals(metaInfo.getStatus())) {
        ReportSummary summary = GSON.fromJson(
            readStringFromFile(reportIdDir.append(LocationName.SUMMARY)),
            ReportSummary.class);
        return new ReportGenerationInfo(metaInfo, null, reportRequest, summary);
      }
      // if the report generation failed, read the error from _FAILURE file and include it in the response
      if (ReportStatus.FAILED.equals(metaInfo.getStatus())) {
        return new ReportGenerationInfo(metaInfo,
            readStringFromFile(reportIdDir.append(FAILURE_FILE)),
            reportRequest, null);
      }
      // if the report is neither COMPLETED nor FAILED, return response with error and summary as null
      return new ReportGenerationInfo(metaInfo, null, reportRequest, null);
    }

    /**
     * Read report summary for the report if it exists, return null otherwise
     *
     * @return {@link ReportSummary}
     * @throws IOException if failed to read the summary content
     */
    @Nullable
    private ReportSummary getReportSummaryIfExists(Location reportIdDir) throws IOException {
      Location summaryFileLocation = reportIdDir.append(LocationName.SUMMARY);
      try {
        return GSON.fromJson(readStringFromFile(summaryFileLocation), ReportSummary.class);
      } catch (FileNotFoundException e) {
        return null;
      }
    }

    /**
     * Reads the String content from the given file under the given directory
     *
     * @param fileLocation file location
     * @return the String content of the file
     * @throws IOException if the file does not exist or fails to read the content of the file
     */
    private String readStringFromFile(Location fileLocation) throws IOException {
      return new String(ByteStreams.toByteArray(fileLocation.getInputStream()),
          StandardCharsets.UTF_8);
    }

    /**
     * Gets the report request from _START file, which was written at the beginning of report
     * generation
     */
    private ReportGenerationRequest getReportRequest(Location reportIdDir) throws Exception {
      // read the report request from _START file, which was written at the beginning of report generation
      String reportRequestString = readStringFromFile(reportIdDir.append(START_FILE));
      return GSON.fromJson(reportRequestString, REPORT_GENERATION_REQUEST_TYPE);
    }

    /**
     * Gets the meta information of a report including the creation time parsed from the report ID,
     * the report name and the report description from the report save request if the report is
     * saved, or the report name from report generation request if the report is not saved, and also
     * the status of the report.
     *
     * @param reportId the id of the report
     * @param reportIdDir the location of the directory containing files with the report meta
     *     information
     * @param generationRequest the report generation request
     * @return the meta information of the report
     * @throws Exception if fails to get the status of the report or fails to access the file
     *     with report save request
     */
    private ReportMetaInfo getReportMetaInfo(String reportId, Location reportIdDir,
        ReportGenerationRequest generationRequest) throws Exception {
      // Get the creation time from the report ID, which is time based UUID
      long creationTime = ReportIds.getTime(reportId, TimeUnit.SECONDS);
      ExtendedReportStatus extendedReportStatus = getReportStatus(reportIdDir);
      ReportStatus status = extendedReportStatus.getReportStatus();
      Location savedFile = reportIdDir.append(SAVED_FILE);
      // if the report is not saved, get the name of the report from the report generation request and include the
      // expiry time in the response if the report is COMPLETED
      if (!savedFile.exists()) {
        Long expiryTimeSecs = null;
        if (extendedReportStatus.getExpirationTime() != null) {
          expiryTimeSecs = TimeUnit.MILLISECONDS.toSeconds(
              extendedReportStatus.getExpirationTime());
        }
        return new ReportMetaInfo(generationRequest.getName(), null, creationTime, expiryTimeSecs,
            status);
      }
      // read the report save request from the file
      String reportSaveRequestString =
          new String(ByteStreams.toByteArray(savedFile.getInputStream()), StandardCharsets.UTF_8);
      ReportSaveRequest saveRequest = GSON.fromJson(reportSaveRequestString,
          REPORT_SAVE_REQUEST_TYPE);
      // the report is saved, get the name and the description from the report save request
      // and return null as expiry time
      return new ReportMetaInfo(saveRequest.getName(), saveRequest.getDescription(), creationTime,
          null, status);
    }

    @DELETE
    @Path("/reports/{report-id}")
    public void deleteReport(HttpServiceRequest request, HttpServiceResponder responder,
        @PathParam("report-id") String reportId) {
      Location reportIdDir;
      try {
        String userName = getUserName(request.getAllHeaders());
        reportIdDir = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(userName)
            .append(reportId);
        if (!reportIdDir.exists()) {
          responder.sendError(404, String.format("Report with id %s does not exist.", reportId));
          return;
        }
      } catch (IOException e) {
        LOG.error("Failed to access the directory of the report with id {}", reportId, e);
        responder.sendError(500,
            String.format("Failed to access the directory of the report with id %s "
                + "because of error: %s", reportId, e.getMessage()));
        return;
      }
      ReportStatus status;
      try {
        status = getReportStatus(reportIdDir).getReportStatus();
      } catch (Exception e) {
        LOG.error("Failed to get the status of the report {}", reportId, e);
        responder.sendError(500,
            String.format("Failed to get the status of the report %s becasue of error: %s",
                reportId, e.getMessage()));
        return;
      }
      // if the report is already expired, deleting the report is not allowed to avoid conflict
      if (ReportStatus.EXPIRED.equals(status)) {
        responder.sendError(404,
            String.format("Report with id %s has already been deleted", reportId));
        return;
      }
      try {
        if (!reportIdDir.delete(true)) {
          // this should never happen since the directory is asserted to exist with valid path before reaching here
          responder.sendError(500,
              String.format("Failed to delete report with id %s because the directory %s "
                      + "does not exist or the path is invalid", reportId,
                  reportIdDir.toURI().toString()));
          return;
        }
      } catch (IOException e) {
        LOG.error("Failed to delete report with id {}", reportId, e);
        responder.sendError(500,
            String.format("Failed to delete report with id %s because of error: %s",
                reportId, e.getMessage()));
        return;
      }
      responder.sendStatus(200);
    }

    @POST
    @Path("/reports/{report-id}/save")
    public void saveReport(HttpServiceRequest request, HttpServiceResponder responder,
        @PathParam("report-id") String reportId) {
      Location reportIdDir;
      try {
        String userName = getUserName(request.getAllHeaders());
        reportIdDir = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(userName)
            .append(reportId);
        if (!reportIdDir.exists()) {
          responder.sendError(404, String.format("Report with id %s does not exist.", reportId));
          return;
        }
      } catch (IOException e) {
        LOG.error("Failed to access the directory of the report with id {}", reportId, e);
        responder.sendError(500,
            String.format("Failed to access the directory of the report with id %s "
                + "because of error: %s", reportId, e.getMessage()));
        return;
      }
      ReportStatus status;
      try {
        status = getReportStatus(reportIdDir).getReportStatus();
      } catch (Exception e) {
        LOG.error("Failed to get the status of the report {}", reportId, e);
        responder.sendError(500,
            String.format("Failed to get the status of the report %s because of error: %s",
                reportId, e.getMessage()));
        return;
      }
      // only allow saving the report when its status is COMPLETED
      switch (status) {
        case COMPLETED:
          break;
        // expired report is considered as deleted
        case EXPIRED:
          responder.sendError(404, String.format("Report with id %s does not exist.", reportId));
          return;
        default:
          responder.sendError(403, "Cannot save the report with status " + status);
          return;
      }
      Location savedFile;
      try {
        // if the report is already saved, saving the report again is not allowed
        savedFile = reportIdDir.append(SAVED_FILE);
        if (savedFile.exists()) {
          try {
            ReportSaveRequest saveRequest =
                GSON.fromJson(readStringFromFile(reportIdDir.append(SAVED_FILE)),
                    REPORT_SAVE_REQUEST_TYPE);
            responder.sendError(403,
                String.format("Report with id %s is already saved with name '%s' "
                        + "and description '%s'. Updating the saved report is not allowed.",
                    reportId, saveRequest.getName(), saveRequest.getDescription()));
            return;
          } catch (Exception e) {
            LOG.warn("Failed to parse the content of the existing report saving request in {}. "
                + "Will overwrite with the new saving request.", savedFile.toURI().toString(), e);
          }
        }
      } catch (Exception e) {
        LOG.error("Failed to save the report {}", reportId, e);
        responder.sendError(500, String.format("Failed to save the report %s because of error: %s",
            reportId, e.getMessage()));
        return;
      }
      String requestJson = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      ReportSaveRequest saveRequest;
      try {
        saveRequest = GSON.fromJson(requestJson, REPORT_SAVE_REQUEST_TYPE);
      } catch (JsonSyntaxException e) {
        responder.sendError(400, "Failed to parse the report saving request: " + e);
        return;
      }
      try {
        if (!savedFile.createNew()) {
          responder.sendError(500,
              String.format("Failed to create a file %s for saving the report with id %s "
                  + "since it already exists", savedFile.toURI().toString(), reportId));
          return;
        }
      } catch (IOException e) {
        LOG.error("Failed to save the report {} when creating the file {}", reportId,
            savedFile.toURI().toString(), e);
        responder.sendError(500, String.format("Failed to save the report %s because of error in "
                + "creating the file %s: %s",
            reportId, savedFile.toURI().toString(), e.getMessage()));
        return;
      }
      // save the report save request in the _SAVED file
      try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(savedFile.getOutputStream(),
          StandardCharsets.UTF_8), true)) {
        writer.write(requestJson);
      } catch (IOException e) {
        LOG.error("Failed to save the report {} when writing to the file", reportId,
            savedFile.toURI().toString(), e);
        responder.sendError(500, String.format("Failed to save the report %s because of error "
                + "in writing to the file %s: %s",
            reportId, savedFile.toURI().toString(), e.getMessage()));
        return;
      }
      responder.sendString(200,
          String.format("Report with id %s is saved successfully with the name: '%s' "
                  + "and description: '%s' ", reportId, saveRequest.getName(),
              saveRequest.getDescription()),
          StandardCharsets.UTF_8);
    }

    /**
     * Use reportId and userName to construct the {@link ReportIdentifier} If instead shareId is
     * provided, we decode the userName and reportId from shareId and construct the identifier and
     * return that.
     *
     * @param reportId - id of the report, can be null if shareId is provided
     * @param headers - request headers, used to retrieve the name of the owner who generated
     *     the report, if reportId is not null
     * @param shareId - shareId if the report is a shared report, can be null if reportId and
     *     userName is provided
     * @return {@link ReportIdentifier}
     * @throws IllegalArgumentException if none of the reportId and shareId is provided or if
     *     both are provided
     * @throws UnsupportedEncodingException if there are any error while decoding the shareId
     */
    private ReportIdentifier validateAndGetReportIdentifier(
        @Nullable String reportId, Map<String, List<String>> headers, @Nullable String shareId)
        throws IllegalArgumentException, IOException, GeneralSecurityException {
      if (reportId == null && shareId == null) {
        throw new IllegalArgumentException(
            "Either reportId or sharedId must be provided, missing both");
      }

      if (reportId != null && shareId != null) {
        throw new IllegalArgumentException(
            "Only one of reportId or sharedId must be provided, provided both");
      }
      if (reportId != null) {
        return new ReportIdentifier(getUserName(headers), reportId);
      }
      // share-id is provided
      return decodeShareId(shareId);
    }

    private Location getLocationFromReportIdentifier(ReportIdentifier reportIdentifier)
        throws IOException {
      return getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(
              reportIdentifier.getUserName()).
          append(reportIdentifier.getReportId());
    }

    @GET
    @Path("reports/download")
    public void getReportDetails(HttpServiceRequest request, HttpServiceResponder responder,
        @QueryParam("offset") @DefaultValue("0") long offset,
        @QueryParam("limit") @DefaultValue(DEFAULT_LIMIT) int limit,
        @QueryParam("report-id") String reportId,
        @QueryParam("share-id") String shareId) throws Exception {
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
      ReportIdentifier reportIdentifier;
      String idMessage;
      try {
        reportIdentifier = validateAndGetReportIdentifier(reportId, request.getAllHeaders(),
            shareId);
        idMessage = shareId == null ? String.format("ReportId : %s", reportId) :
            String.format("ShareId : %s", shareId);
      } catch (IllegalArgumentException iae) {
        responder.sendError(400, iae.getMessage());
        return;
      } catch (UnsupportedEncodingException | GeneralSecurityException e) {
        responder.sendError(500, String.format(
            "Error while decoding shareId %s, due to exception : ", shareId, e.getMessage()));
        return;
      }
      Location reportIdDir;
      try {
        reportIdDir = getLocationFromReportIdentifier(reportIdentifier);
      } catch (IOException e) {
        LOG.error("Failed to get location for report with {}", idMessage, e);
        responder.sendError(500,
            String.format("Failed to get location for report with %s because of error: %s",
                idMessage, e.getMessage()));
        return;
      }

      if (!reportIdDir.exists()) {
        responder.sendError(404, String.format("Report with %s does not exist.", idMessage));
        return;
      }
      ReportSummary summary = getReportSummaryIfExists(reportIdDir);
      if (summary != null) {
        // report completed succcessfully, might be expired
        if (summary.isExpired()) {
          responder.sendError(404, String.format("Report with %s does not exist.", idMessage));
          return;
        }
        // report completed and not expired, read contents and return
        long totalRecords = summary.getRecordCount();
        List<String> reportRecords = new ArrayList<>();
        if (totalRecords > 0) {
          long recordCount = 0;
          Location reportDir = reportIdDir.append(LocationName.REPORT_DIR);
          if (!reportDir.exists() || reportDir.list().size() < 1) {
            responder.sendError(404,
                String.format("Content files not found for report %s", idMessage));
            return;
          }
          // TODO: [CDAP-13291] need to support reading multiple report files
          Optional<Location> reportFile =
              reportDir.list().stream().filter(l -> l.getName().endsWith(".avro")).findFirst();
          // TODO: [CDAP-13292] use cache to store content of the reports
          // Read the report file and add lines starting from the position
          // of offset to the result until the result reaches
          // the limit
          if (reportFile.isPresent()) {
            Location reportFileLocation = reportFile.get();
            DataFileStream<GenericRecord> dataFileStream =
                new DataFileStream<>(reportFileLocation.getInputStream(),
                    new GenericDatumReader<>());
            while (dataFileStream.hasNext()) {
              GenericRecord record = dataFileStream.next();
              if (recordCount++ < offset) {
                continue;
              }
              reportRecords.add(record.toString());
              if (reportRecords.size() >= limit) {
                break;
              }
            }
          }
        }
        // call custom method to convert ReportContent to JSON to return report details as JSON objects directly
        // without stringifying them
        responder.sendString(200,
            new ReportContent(offset, limit, totalRecords, reportRecords).toJson(),
            StandardCharsets.UTF_8);
        return;
      }

      // Get the status of the report and only COMPLETED report can be read
      ReportStatus status;
      try {
        status = getReportStatus(reportIdDir).getReportStatus();
      } catch (Exception e) {
        LOG.error("Failed to get the status of the report {}", idMessage, e);
        responder.sendError(500,
            String.format("Failed to get the status of the %s because of error: %s",
                idMessage, e.getMessage()));
        return;
      }
      switch (status) {
        case RUNNING:
          responder.sendError(202,
              String.format("%s is still being generated, please retry later.", idMessage));
          return;
        case FAILED:
          responder.sendError(400,
              String.format("Reading details of the %s with failed status is not allowed.",
                  idMessage));
          return;
        default: // this should never happen
          responder.sendError(500,
              String.format("Unable to read report with %s with unknown status %s.",
                  idMessage, status));
          return;
      }
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
      ReportIdentifier reportIdentifier = new ReportIdentifier(getUserName(request.getAllHeaders()),
          reportId);
      Location reportIdDir = getLocationFromReportIdentifier(reportIdentifier);
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
      REPORT_EXECUTOR.execute(() -> {
        tryGenerateReport(reportRequest, reportIdDir, reportId);
      });
      responder.sendJson(200, ImmutableMap.of("id", reportId));
    }

    /**
     * Delegates report generation to {@link #generateReport(ReportGenerationRequest, Location)} and
     * writes errors caught during report generation to a _FAILURE file.
     *
     * @param reportRequest the request to generate report
     * @param reportIdDir the location of the directory which will be the parent directory of
     *     _FAILURE file and will be passed to {@link #generateReport(ReportGenerationRequest,
     *     Location)}
     * @param reportId the ID of the report being generated
     */
    private void tryGenerateReport(ReportGenerationRequest reportRequest, Location reportIdDir,
        String reportId) {
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
          try (PrintWriter writer = new PrintWriter(
              new OutputStreamWriter(failureFile.getOutputStream(),
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
     * @param reportIdDir the location of the directory where the report files directory, COUNT
     *     file, and _SUCCESS file will be created.
     */
    private void generateReport(ReportGenerationRequest reportRequest, Location reportIdDir)
        throws IOException {
      Location baseLocation = getDatasetBaseLocation(ReportGenerationApp.RUN_META_FILESET);
      // Get a list of directories of all namespaces under RunMetaFileset base location
      List<Location> nsLocations;
      nsLocations = baseLocation.list();
      // Get the namespace filter from the request if it exists
      final ValueFilter<String> nsFilter = getNamespaceFilterIfExists(reportRequest);
      Stream<Location> filteredNsLocations = nsLocations.stream();
      // If the namespace filter exists, apply the filter to get filtered namespace directories
      if (nsFilter != null) {
        filteredNsLocations = nsLocations.stream()
            .filter(nsLocation -> nsFilter.apply(nsLocation.getName()));
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
            //file name is of the format <event-time-millis>-<creation-time-millis>.avro
            && TimeUnit.MILLISECONDS.toSeconds(
            Long.parseLong(fileName.substring(0, fileName.indexOf("-")))) <
            reportRequest.getEnd();
      }).map(location -> location.toURI().toString()).collect(Collectors.toList());
      LOG.debug("Filtered meta files {}", metaFilePaths);
      // Generate the report with the request and program run meta files
      ReportGenerationHelper.generateReport(sqlContext, reportRequest,
          metaFilePaths, reportIdDir, reportsExpiryTimeMillis);
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

    /**
     * Returns the status of the report generation by checking the presence of the _SUMMARY file,
     * _FAILURE file. If none of these files exists, the report generation is still running.
     *
     * TODO: [CDAP-13215] failure file may not be written if the Spark program is killed. Status of killed
     * report generation job might be returned as RUNNING
     *
     * @param reportIdDir the base directory with report ID as directory name
     * @return {@link ExtendedReportStatus} if report is completed extended report status will have
     *     expiration time
     */
    private ExtendedReportStatus getReportStatus(Location reportIdDir) throws Exception {
      // if the report is completed but has expired, return EXPIRED as its status too
      ReportSummary reportSummary = getReportSummaryIfExists(reportIdDir);
      if (reportSummary != null) {
        return reportSummary.isExpired() ? new ExtendedReportStatus(ReportStatus.EXPIRED) :
            new ExtendedReportStatus(ReportStatus.COMPLETED,
                reportSummary.getyExpirationTimeMillis());
      }
      if (reportIdDir.append(FAILURE_FILE).exists()) {
        return new ExtendedReportStatus(ReportStatus.FAILED);
      }
      return new ExtendedReportStatus(ReportStatus.RUNNING);
    }

    private class ExtendedReportStatus {

      private final ReportStatus status;
      @Nullable
      private final Long expirationTimeMillis;

      private ExtendedReportStatus(ReportStatus status) {
        this(status, null);
      }

      private ExtendedReportStatus(ReportStatus status, @Nullable Long expirationTime) {
        this.status = status;
        this.expirationTimeMillis = expirationTime;
      }

      private ReportStatus getReportStatus() {
        return status;
      }

      @Nullable
      private Long getExpirationTime() {
        return expirationTimeMillis;
      }
    }

    private Location getDatasetBaseLocation(String datasetName) {
      return Transactionals.execute(getContext(), context -> {
        return context.<FileSet>getDataset(datasetName).getBaseLocation();
      });
    }

    private <T> T decodeRequestBody(String request, Type type) {
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
