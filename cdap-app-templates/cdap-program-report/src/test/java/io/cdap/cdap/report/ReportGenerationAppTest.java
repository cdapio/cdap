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

import com.databricks.spark.avro.DefaultSource;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.report.main.ProgramRunInfo;
import io.cdap.cdap.report.main.ProgramRunInfoSerializer;
import io.cdap.cdap.report.main.ProgramStartInfo;
import io.cdap.cdap.report.proto.Filter;
import io.cdap.cdap.report.proto.FilterCodec;
import io.cdap.cdap.report.proto.ProgramRunStartMethod;
import io.cdap.cdap.report.proto.RangeFilter;
import io.cdap.cdap.report.proto.ReportContent;
import io.cdap.cdap.report.proto.ReportGenerationInfo;
import io.cdap.cdap.report.proto.ReportGenerationRequest;
import io.cdap.cdap.report.proto.ReportList;
import io.cdap.cdap.report.proto.ReportSaveRequest;
import io.cdap.cdap.report.proto.ReportStatus;
import io.cdap.cdap.report.proto.ShareId;
import io.cdap.cdap.report.proto.Sort;
import io.cdap.cdap.report.proto.ValueFilter;
import io.cdap.cdap.report.proto.summary.ArtifactAggregate;
import io.cdap.cdap.report.proto.summary.DurationStats;
import io.cdap.cdap.report.proto.summary.NamespaceAggregate;
import io.cdap.cdap.report.proto.summary.ReportSummary;
import io.cdap.cdap.report.proto.summary.StartMethodAggregate;
import io.cdap.cdap.report.proto.summary.StartStats;
import io.cdap.cdap.report.proto.summary.UserAggregate;
import io.cdap.cdap.report.util.Constants;
import io.cdap.cdap.report.util.ReportContentDeserializer;
import io.cdap.cdap.report.util.ReportField;
import io.cdap.cdap.report.util.ReportIds;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestBase;
import io.cdap.cdap.test.TestConfiguration;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.ApplicationBundler;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests {@link ReportGenerationApp}.
 */
public class ReportGenerationAppTest extends TestBase {
  @ClassRule
  public static final TestConfiguration SPARK_VERSION_CONFIG =
    new TestConfiguration("app.program.spark.compat", SparkCompat.getSparkVersion());
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  @ClassRule
  public static final TestConfiguration CONF =
    new TestConfiguration(io.cdap.cdap.common.conf.Constants.Explore.EXPLORE_ENABLED, "false");

  private static final Logger LOG = LoggerFactory.getLogger(ReportGenerationAppTest.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ReportContent.class, new ReportContentDeserializer())
    .registerTypeAdapter(Filter.class, new FilterCodec())
    .disableHtmlEscaping()
    .create();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type REPORT_GEN_INFO_TYPE = new TypeToken<ReportGenerationInfo>() { }.getType();
  private static final Type REPORT_CONTENT_TYPE = new TypeToken<ReportContent>() { }.getType();
  private static final String USER_ALICE = "alice";
  private static final String USER_BOB = "bob";
  private static final String TEST_ARTIFACT_NAME = "TestArtifact";

  @Test
  public void testGenerateReport() throws Exception {
    Map<String, String> runTimeArguments = new HashMap<>();
    // disable tms subscriber thread as the RunMetaFileSet avro files are written directly by the test case
    // if the tms subscriber thread is enabled, in order to find the latest message id to start fetching from,
    // we read the latest RunMetaFileSet avro file's content
    // whereas the derived message_id will be invalid in TMS as these runs aren't in TMS,
    // in order to avoid the exception we disable the tms subscriber thread for the test case
    runTimeArguments.put(Constants.DISABLE_TMS_SUBSCRIBER_THREAD, "true");

    Long currentTimeMillis = System.currentTimeMillis();
    DatasetId metaFileset = createAndInitializeDataset(NamespaceId.DEFAULT, currentTimeMillis);

    SparkManager sparkManager = deployAndStartReportingApplication(NamespaceId.DEFAULT, runTimeArguments);
    URL url = sparkManager.getServiceURL(1, TimeUnit.MINUTES);
    Assert.assertNotNull(url);
    URL reportURL = url.toURI().resolve("reports/").toURL();
    List<Filter> filters =
      ImmutableList.of(
        // white list filter
        new ValueFilter<>(Constants.NAMESPACE, ImmutableSet.of("ns1", "ns2"), null),
        new RangeFilter<>(Constants.DURATION, new RangeFilter.Range<>(null, 500L)),
        // black list filter
        new ValueFilter<>(Constants.ARTIFACT_NAME, null, ImmutableSet.of("cdap-data-streams", "cdap-data-pipeline")));
    long startSecs = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis);
    ReportGenerationRequest request =
      new ReportGenerationRequest("ns1_ns2_report", startSecs, startSecs + 30,
                                  new ArrayList<>(ReportField.FIELD_NAME_MAP.keySet()),
                                  ImmutableList.of(new Sort(Constants.DURATION, Sort.Order.DESCENDING)), filters);
    HttpURLConnection urlConn = (HttpURLConnection) reportURL.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod("POST");
    urlConn.getOutputStream().write(GSON.toJson(request).getBytes(StandardCharsets.UTF_8));
    if (urlConn.getErrorStream() != null) {
      Assert.fail(Bytes.toString(ByteStreams.toByteArray(urlConn.getErrorStream())));
    }
    Assert.assertEquals(200, urlConn.getResponseCode());
    Map<String, String> reportIdMap = getResponseObject(urlConn, STRING_STRING_MAP);
    String reportId = reportIdMap.get("id");
    Assert.assertNotNull(reportId);
    URL reportIdURL = reportURL.toURI().resolve("info?report-id=" + reportId).toURL();
    validateReportSummary(reportIdURL, startSecs);

    // share the report to get the share id
    URL shareReportURL = reportURL.toURI().resolve(reportId + "/share").toURL();
    HttpURLConnection shareURLConnection = (HttpURLConnection) shareReportURL.openConnection();
    shareURLConnection.setRequestMethod("POST");
    ShareId shareId = getResponseObject(shareURLConnection, ShareId.class);

    // test if we are able to get summary and read the summary using share id
    URL shareIdSummaryURL = reportURL.toURI().resolve("info?share-id=" + shareId.getShareId()).toURL();
    validateReportSummary(shareIdSummaryURL, startSecs);

    // assert the number of report details is correct
    URL reportRunsURL = reportURL.toURI().resolve("download?report-id=" + reportId).toURL();
    validateReportContent(reportRunsURL);

    // test if we are able to download and read the report using share id
    URL shareIdRunsURL = reportURL.toURI().resolve("download?share-id=" + shareId.getShareId()).toURL();
    validateReportContent(shareIdRunsURL);

    // save the report with a new name and description
    URL reportSaveURL = reportURL.toURI().resolve(reportId + "/" + "save").toURL();
    urlConn = (HttpURLConnection) reportSaveURL.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod("POST");
    urlConn.getOutputStream().write(GSON.toJson(new ReportSaveRequest("newName", "newDescription"))
                                      .getBytes(StandardCharsets.UTF_8));
    if (urlConn.getErrorStream() != null) {
      Assert.fail(Bytes.toString(ByteStreams.toByteArray(urlConn.getErrorStream())));
    }
    Assert.assertEquals(200, urlConn.getResponseCode());
    // verify that the name and description of the report have been updated, and the expiry time is null
    ReportGenerationInfo reportGenerationInfo = getResponseObject(reportIdURL.openConnection(), REPORT_GEN_INFO_TYPE);
    Assert.assertEquals("newName", reportGenerationInfo.getName());
    Assert.assertEquals("newDescription", reportGenerationInfo.getDescription());
    Assert.assertNull(reportGenerationInfo.getExpiry());
    // save the report again should fail
    urlConn = (HttpURLConnection) reportSaveURL.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod("POST");
    urlConn.getOutputStream().write(GSON.toJson(new ReportSaveRequest("anotherNewName", "anotherNewDescription"))
                                      .getBytes(StandardCharsets.UTF_8));
    if (urlConn.getErrorStream() != null) {
      Assert.fail(Bytes.toString(ByteStreams.toByteArray(urlConn.getErrorStream())));
    }
    Assert.assertEquals(403, urlConn.getResponseCode());
    // delete the report
    URL reportDeleteURL = reportURL.toURI().resolve(reportId).toURL();
    urlConn = (HttpURLConnection) reportDeleteURL.openConnection();
    urlConn.setRequestMethod("DELETE");
    Assert.assertEquals(200, urlConn.getResponseCode());
    // getting status of a deleted report will get 404
    Assert.assertEquals(404, ((HttpURLConnection) reportIdURL.openConnection()).getResponseCode());
    // deleting a deleted report again will get 404
    urlConn = (HttpURLConnection) reportDeleteURL.openConnection();
    urlConn.setRequestMethod("DELETE");
    Assert.assertEquals(404, urlConn.getResponseCode());

    // test querying for time range before the start secs, to verify empty results contents
    validateEmptyReports(reportURL, startSecs - TimeUnit.HOURS.toSeconds(2), startSecs - 30, filters);
    // test querying for time range after the start secs, but with filter that doesnt match any records
    // to verify empty results contents
    List<Filter> filters2 =
      ImmutableList.of(
        new ValueFilter<>(Constants.NAMESPACE, ImmutableSet.of("ns1", "ns2"), null),
        // all the programs are run by user alice, user bob will match no records.
        new ValueFilter<>(Constants.USER, ImmutableSet.of(USER_BOB), null));
    validateEmptyReports(reportURL, startSecs, startSecs + 30, filters2);
    List<Filter>  filters3 =
      ImmutableList.of(
        new ValueFilter<>(Constants.NAMESPACE, ImmutableSet.of("ns1", "ns2"), null),
        // all the programs have the same test artifact name, blacklisting that will provide empty results
        new ValueFilter<>(Constants.ARTIFACT_NAME, null, ImmutableSet.of(TEST_ARTIFACT_NAME)));
    validateEmptyReports(reportURL, startSecs, startSecs + 30, filters3);
    sparkManager.stop();
    sparkManager.waitForStopped(60, TimeUnit.SECONDS);
    deleteDatasetInstance(metaFileset);
  }

  private void validateEmptyReports(URL reportURL, long startSecs,
                                    long endSecs, List<Filter> filters) throws Exception {
    ReportGenerationRequest request =
      new ReportGenerationRequest("ns1_ns2_report", startSecs, endSecs,
                                  new ArrayList<>(ReportField.FIELD_NAME_MAP.keySet()),
                                  ImmutableList.of(new Sort(Constants.DURATION, Sort.Order.DESCENDING)), filters);
    HttpURLConnection urlConn = (HttpURLConnection) reportURL.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod("POST");
    urlConn.getOutputStream().write(GSON.toJson(request).getBytes(StandardCharsets.UTF_8));
    if (urlConn.getErrorStream() != null) {
      Assert.fail(Bytes.toString(ByteStreams.toByteArray(urlConn.getErrorStream())));
    }
    Assert.assertEquals(200, urlConn.getResponseCode());
    Map<String, String> reportIdMap = getResponseObject(urlConn, STRING_STRING_MAP);
    String reportId = reportIdMap.get("id");
    Assert.assertNotNull(reportId);
    URL reportIdURL = reportURL.toURI().resolve("info?report-id=" + reportId).toURL();
    validateEmptyReportSummary(reportIdURL);
    URL reportRunsURL = reportURL.toURI().resolve("download?report-id=" + reportId).toURL();
    validateEmptyReportContent(reportRunsURL);
  }

  private DatasetId createAndInitializeDataset(NamespaceId namespaceId, long currentTimeMillis) throws Exception {
    DatasetId metaFileset = namespaceId.dataset(ReportGenerationApp.RUN_META_FILESET);
    addDatasetInstance(metaFileset, FileSet.class.getName());
    // TODO: [CDAP-13216] temporarily create the run meta fileset and generate mock program run meta files here.
    // Will remove once the TMS subscriber writing to the run meta fileset is implemented.
    DataSetManager<FileSet> fileSet = getDataset(metaFileset);
    populateMetaFiles(fileSet.get().getBaseLocation(), currentTimeMillis);
    return metaFileset;
  }

  private SparkManager deployAndStartReportingApplication(NamespaceId deployNamespace,
                                                          Map<String, String> runtimeArguments) throws IOException {
    // Trace the dependencies for the spark avro
    ApplicationBundler bundler = new ApplicationBundler(new ClassAcceptor() {
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        if (className.startsWith("org.apache.spark.")) {
          return false;
        }
        if (className.startsWith("scala.")) {
          return false;
        }
        if (className.startsWith("org.apache.hadoop.")) {
          return false;
        }
        if (className.startsWith("org.codehaus.janino.")) {
          return false;
        }
        return true;
      }
    });

    Location avroSparkBundle = Locations.toLocation(TEMP_FOLDER.newFile());
    // Since spark-avro and its dependencies need to be included into the application jar,
    // but spark-avro is not used directly in the application code, explicitly add a class DefaultSource
    // from spark-avro so that spark-avro and its dependencies will be included.
    bundler.createBundle(avroSparkBundle, DefaultSource.class);
    File unJarDir = BundleJarUtil.prepareClassLoaderFolder(avroSparkBundle, TEMP_FOLDER.newFolder());

    ApplicationManager app = deployApplication(deployNamespace,
                                               ReportGenerationApp.class, new File(unJarDir, "lib").listFiles());
    return app.getSparkManager(ReportGenerationSpark.class.getSimpleName()).start(runtimeArguments);
  }

  @Test
  public void testReportExpiration() throws Exception {
    NamespaceId testNamespace = new NamespaceId("reporting");
    NamespaceMeta reportingNamespace = new NamespaceMeta.Builder()
      .setName(testNamespace)
      .setDescription("Reporting namespace used to test report expiry")
      .build();
    getNamespaceAdmin().create(reportingNamespace);
    long currentTimeMillis = System.currentTimeMillis();
    DatasetId datasetId = createAndInitializeDataset(testNamespace, currentTimeMillis);
    Map<String, String> runTimeArguments = new HashMap<>();
    // set report expiration time to be 10 seconds
    runTimeArguments.put(Constants.Report.REPORT_EXPIRY_TIME_SECONDS, "1");
    runTimeArguments.put(Constants.DISABLE_TMS_SUBSCRIBER_THREAD, "true");
    SparkManager sparkManager = deployAndStartReportingApplication(testNamespace, runTimeArguments);

    URL url = sparkManager.getServiceURL(1, TimeUnit.MINUTES);
    Assert.assertNotNull(url);
    URL reportURL = url.toURI().resolve("reports/").toURL();
    List<Filter> filters =
      ImmutableList.of(
        new ValueFilter<>(Constants.NAMESPACE, ImmutableSet.of("ns1", "ns2"), null),
        new RangeFilter<>(Constants.DURATION, new RangeFilter.Range<>(null, 500L)));
    long startSecs = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis);
    ReportGenerationRequest request =
      new ReportGenerationRequest("ns1_ns2_report", startSecs, startSecs + 30,
                                  new ArrayList<>(ReportField.FIELD_NAME_MAP.keySet()),
                                  ImmutableList.of(new Sort(Constants.DURATION, Sort.Order.DESCENDING)), filters);
    HttpURLConnection urlConn = (HttpURLConnection) reportURL.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod("POST");
    urlConn.getOutputStream().write(GSON.toJson(request).getBytes(StandardCharsets.UTF_8));
    if (urlConn.getErrorStream() != null) {
      Assert.fail(Bytes.toString(ByteStreams.toByteArray(urlConn.getErrorStream())));
    }
    Assert.assertEquals(200, urlConn.getResponseCode());

    Map<String, String> reportIdMap = getResponseObject(urlConn, STRING_STRING_MAP);
    String reportId = reportIdMap.get("id");
    Assert.assertNotNull(reportId);

    // perform get reports list for 60 seconds and make sure we get 0 reports due to expiry
    Tasks.waitFor(0, () -> {
      return getReportsList(url);
    }, 5, TimeUnit.MINUTES, 1, TimeUnit.SECONDS);
    sparkManager.stop();
    sparkManager.waitForStopped(2, TimeUnit.MINUTES);
    deleteDatasetInstance(datasetId);
    getNamespaceAdmin().delete(testNamespace);
  }

  private int getReportsList(URL url) throws IOException, URISyntaxException {
    URL reportURL = url.toURI().resolve("reports/").toURL();
    HttpURLConnection reportsUrl = (HttpURLConnection) reportURL.openConnection();
    reportsUrl.setDoOutput(true);
    reportsUrl.setRequestMethod("GET");
    if (reportsUrl.getErrorStream() != null) {
      Assert.fail(Bytes.toString(ByteStreams.toByteArray(reportsUrl.getErrorStream())));
    }
    Assert.assertEquals(200, reportsUrl.getResponseCode());
    ReportList reportList = getResponseObject(reportsUrl, ReportList.class);
    // make sure the total number of reports returned and the actual report list returned are equal in size
    Assert.assertEquals(reportList.getTotal(), reportList.getReports().size());
    return reportList.getReports().size();
  }

  @Test
  public void testFilterSerialization() throws Exception {
    List<Filter> filters =
      ImmutableList.of(
        new ValueFilter<>(Constants.NAMESPACE, ImmutableSet.of("ns1", "ns2"), null),
        new RangeFilter<>(Constants.DURATION, new RangeFilter.Range<>(null, 500L)));
    long startSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    ReportGenerationRequest request =
      new ReportGenerationRequest("ns1_ns2_report", startSecs, startSecs + 30,
                                  new ArrayList<>(ReportField.FIELD_NAME_MAP.keySet()),
                                  ImmutableList.of(new Sort(Constants.DURATION, Sort.Order.DESCENDING)), filters);
    String serialized = GSON.toJson(request);
    Assert.assertNotNull(serialized);
    ReportGenerationRequest deserialized = GSON.fromJson(serialized, ReportGenerationRequest.class);
    Assert.assertEquals(request, deserialized);
  }


  private void validateReportSummary(URL reportIdURL, long startSecs)
    throws InterruptedException, ExecutionException, TimeoutException, IOException {
    Tasks.waitFor(ReportStatus.COMPLETED, () -> {
      ReportGenerationInfo reportGenerationInfo = getResponseObject(reportIdURL.openConnection(),
                                                                    REPORT_GEN_INFO_TYPE);
      if (ReportStatus.FAILED.equals(reportGenerationInfo.getStatus())) {
        Assert.fail("Report generation failed");
      }
      return reportGenerationInfo.getStatus();
    }, 5, TimeUnit.MINUTES, 2, TimeUnit.SECONDS);

    ReportGenerationInfo reportGenerationInfo = getResponseObject(reportIdURL.openConnection(),
                                                                  REPORT_GEN_INFO_TYPE);
    // assert the summary content is expected
    ReportSummary summary = reportGenerationInfo.getSummary();
    Assert.assertNotNull(summary);
    Assert.assertEquals(ImmutableSet.of(new NamespaceAggregate("ns1", 1), new NamespaceAggregate("ns2" , 1)),
                        new HashSet<>(summary.getNamespaces()));
    Assert.assertEquals(ImmutableSet.of(new ArtifactAggregate(TEST_ARTIFACT_NAME, "1.0.0", "USER", 2)),
                        new HashSet<>(summary.getArtifacts()));
    DurationStats durationStats = summary.getDurations();
    Assert.assertEquals(300L, durationStats.getMin());
    Assert.assertEquals(300L, durationStats.getMax());
    // averages with difference smaller than 0.01 are considered equal
    Assert.assertTrue(Math.abs(300.0 - durationStats.getAverage()) < 0.01);
    Assert.assertEquals(new StartStats(startSecs, startSecs), summary.getStarts());
    Assert.assertEquals(ImmutableSet.of(new UserAggregate(USER_ALICE, 2)), new HashSet<>(summary.getOwners()));
    Assert.assertEquals(ImmutableSet.of(new StartMethodAggregate(ProgramRunStartMethod.TRIGGERED, 2)),
                        new HashSet<>(summary.getStartMethods()));
  }

  private void validateEmptyReportSummary(URL reportIdURL)
    throws InterruptedException, ExecutionException, TimeoutException, IOException {
    Tasks.waitFor(ReportStatus.COMPLETED, () -> {
      ReportGenerationInfo reportGenerationInfo = getResponseObject(reportIdURL.openConnection(),
                                                                    REPORT_GEN_INFO_TYPE);
      if (ReportStatus.FAILED.equals(reportGenerationInfo.getStatus())) {
        Assert.fail("Report generation failed");
      }
      return reportGenerationInfo.getStatus();
    }, 5, TimeUnit.MINUTES, 2, TimeUnit.SECONDS);

    ReportGenerationInfo reportGenerationInfo = getResponseObject(reportIdURL.openConnection(),
                                                                  REPORT_GEN_INFO_TYPE);
    // assert the summary content is expected
    ReportSummary summary = reportGenerationInfo.getSummary();
    Assert.assertNotNull(summary);
    Assert.assertEquals(ImmutableSet.of(new NamespaceAggregate("ns1", 0), new NamespaceAggregate("ns2" , 0)),
                        new HashSet<>(summary.getNamespaces()));
    Assert.assertEquals(new DurationStats(0L, 0L, 0.0), summary.getDurations());
    Assert.assertEquals(new StartStats(0, 0), summary.getStarts());
    Assert.assertEquals(0, summary.getOwners().size());
    Assert.assertEquals(0, summary.getStartMethods().size());
  }

  private void validateEmptyReportContent(URL reportRunsURL) throws IOException {
    ReportContent reportContent = getResponseObject(reportRunsURL.openConnection(), REPORT_CONTENT_TYPE);
    Assert.assertEquals(0, reportContent.getTotal());
  }

  private void validateReportContent(URL reportRunsURL) throws IOException {
    ReportContent reportContent = getResponseObject(reportRunsURL.openConnection(), REPORT_CONTENT_TYPE);
    Assert.assertEquals(2, reportContent.getTotal());
    // Assert that all the records in the report contain startMethod TRIGGERED
    boolean startMethodIsCorrect =
      reportContent.getDetails().stream().allMatch(content -> content.contains("\"startMethod\":\"TRIGGERED\""));
    if (!startMethodIsCorrect) {
      Assert.fail("All report records are expected to contain startMethod TRIGGERED, " +
                    "but actual results do not meet this requirement: " + reportContent.getDetails());
    }
  }

  private static <T> T getResponseObject(URLConnection urlConnection, Type typeOfT) throws IOException {
    String response;
    try {
      response = new String(ByteStreams.toByteArray(urlConnection.getInputStream()), Charsets.UTF_8);
    } finally {
      ((HttpURLConnection) urlConnection).disconnect();
    }
    LOG.info(response);
    return GSON.fromJson(response, typeOfT);
  }

  /**
   * Adds mock program run meta files to the given location.
   * TODO: [CDAP-13216] this method should be marked as @VisibleForTesting. Temporarily calling this method
   * when initializing report generation Spark program to add mock data
   *
   * @param metaBaseLocation the location to add files
   * @param currentTime the current time in millis
   */
  private static void populateMetaFiles(Location metaBaseLocation, Long currentTime) throws Exception {
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(ProgramRunInfoSerializer.SCHEMA);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    String appName = "Pipeline";
    String version = "-SNAPSHOT";
    String type = "WORKFLOW";
    String program1 = "SmartWorkflow_1";
    String program2 = "SmartWorkflow_2";
    // add a schedule info with program status trigger
    String scheduleInfo = "{\"name\": \"sched\",\"description\": \"desc\",\"triggerInfos\": [" +
      "{\"namespace\": \"default\",\"application\": \"app\",\"version\": \"-SNAPSHOT\",\"programType\": \"WORKFLOW\"," +
      "\"run\":\"randomRunId\",\"entity\": \"PROGRAM\",\"program\": \"wf\",\"programStatus\": \"KILLED\"," +
      "\"type\": \"PROGRAM_STATUS\"}]}";
    ProgramStartInfo startInfo =
      new ProgramStartInfo(ImmutableMap.of(),
                           new ArtifactId(TEST_ARTIFACT_NAME,
                                          new ArtifactVersion("1.0.0"), ArtifactScope.USER), USER_ALICE,
                           ImmutableMap.of(Constants.Notification.SCHEDULE_INFO_KEY, scheduleInfo));
    long delay = TimeUnit.MINUTES.toMillis(5);
    int mockMessageId = 0;
    for (String namespace : ImmutableList.of("default", "ns1", "ns2")) {
      Location nsLocation = metaBaseLocation.append(namespace);
      nsLocation.mkdirs();
      for (int i = 0; i < 5; i++) {
        long time = currentTime + TimeUnit.HOURS.toMillis(i);
        //file name is of the format <event-time-millis>-<creation-time-millis>.avro
        Location reportLocation = nsLocation.append(String.format("%d-%d.avro", time, System.currentTimeMillis()));
        reportLocation.createNew();
        dataFileWriter.create(ProgramRunInfoSerializer.SCHEMA, reportLocation.getOutputStream());
        String run1 = ReportIds.generate().toString();
        String run2 = ReportIds.generate().toString();
        dataFileWriter.append(createRecord(namespace, appName, version, type, program1, run1,
                                           "STARTING", time, startInfo, Integer.toString(++mockMessageId)));
        dataFileWriter.append(createRecord(namespace, appName, version, type, program1, run1,
                                           "FAILED", time + delay, null, Integer.toString(++mockMessageId)));
        dataFileWriter.append(createRecord(namespace, appName, version, type, program2, run2,
                                           "STARTING", time + delay, startInfo, Integer.toString(++mockMessageId)));
        dataFileWriter.append(createRecord(namespace, appName, version, type, program2, run2,
                                           "RUNNING", time + 2 * delay, null, Integer.toString(++mockMessageId)));
        dataFileWriter.append(createRecord(namespace, appName, version, type, program2, run2,
                                           "COMPLETED", time + 4 * delay, null, Integer.toString(++mockMessageId)));
        dataFileWriter.close();
      }
      LOG.debug("nsLocation.list() = {}", nsLocation.list());
    }
  }

  private static GenericData.Record createRecord(String namespace, String application, String version,
                                                 String type, String program, String run, String status,
                                                 Long timestamp, ProgramStartInfo startInfo, String messageId) {
    ProgramRunInfo runInfo = new ProgramRunInfo(namespace, application, version, type, program, run);
    runInfo.setStatus(status);
    runInfo.setTime(timestamp);
    runInfo.setStartInfo(startInfo);
    runInfo.setMessageId(messageId);
    return ProgramRunInfoSerializer.createRecord(runInfo);
  }
}
