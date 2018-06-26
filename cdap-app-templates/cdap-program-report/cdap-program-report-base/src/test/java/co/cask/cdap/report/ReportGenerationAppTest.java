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

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.report.main.ProgramRunInfo;
import co.cask.cdap.report.main.ProgramRunInfoSerializer;
import co.cask.cdap.report.main.ProgramStartInfo;
import co.cask.cdap.report.proto.Filter;
import co.cask.cdap.report.proto.FilterDeserializer;
import co.cask.cdap.report.proto.ProgramRunStartMethod;
import co.cask.cdap.report.proto.RangeFilter;
import co.cask.cdap.report.proto.ReportContent;
import co.cask.cdap.report.proto.ReportGenerationInfo;
import co.cask.cdap.report.proto.ReportGenerationRequest;
import co.cask.cdap.report.proto.ReportSaveRequest;
import co.cask.cdap.report.proto.ReportStatus;
import co.cask.cdap.report.proto.Sort;
import co.cask.cdap.report.proto.ValueFilter;
import co.cask.cdap.report.proto.summary.ArtifactAggregate;
import co.cask.cdap.report.proto.summary.DurationStats;
import co.cask.cdap.report.proto.summary.NamespaceAggregate;
import co.cask.cdap.report.proto.summary.ReportSummary;
import co.cask.cdap.report.proto.summary.StartMethodAggregate;
import co.cask.cdap.report.proto.summary.StartStats;
import co.cask.cdap.report.proto.summary.UserAggregate;
import co.cask.cdap.report.util.Constants;
import co.cask.cdap.report.util.ReportContentDeserializer;
import co.cask.cdap.report.util.ReportField;
import co.cask.cdap.report.util.ReportIds;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import com.databricks.spark.avro.DefaultSource;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.ApplicationBundler;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
    new TestConfiguration(co.cask.cdap.common.conf.Constants.Explore.EXPLORE_ENABLED, "false");

  private static final Logger LOG = LoggerFactory.getLogger(ReportGenerationAppTest.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ReportContent.class, new ReportContentDeserializer())
    .create();
  // have a separate Gson for deserializing Filter to avoid error when serializing Filter to JSON
  private static final Gson DES_GSON = new GsonBuilder()
    .registerTypeAdapter(ReportContent.class, new ReportContentDeserializer())
    .registerTypeAdapter(Filter.class, new FilterDeserializer())
    .create();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type REPORT_GEN_INFO_TYPE = new TypeToken<ReportGenerationInfo>() { }.getType();
  private static final Type REPORT_CONTENT_TYPE = new TypeToken<ReportContent>() { }.getType();

  @Test
  public void testGenerateReport() throws Exception {
    DatasetId metaFileset = NamespaceId.DEFAULT.dataset(ReportGenerationApp.RUN_META_FILESET);
    addDatasetInstance(metaFileset, FileSet.class.getName());
    // generate mock program run meta files
    DataSetManager<FileSet> fileSet = getDataset(metaFileset);
    Long currentTime = System.currentTimeMillis();
    populateMetaFiles(fileSet.get().getBaseLocation(), currentTime);

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
    File unJarDir = BundleJarUtil.unJar(avroSparkBundle, TEMP_FOLDER.newFolder());
    // deploy report generation app
    ApplicationManager app = deployApplication(ReportGenerationApp.class, new File(unJarDir, "lib").listFiles());
    // start report generation Spark
    SparkManager sparkManager = app.getSparkManager(ReportGenerationSpark.class.getSimpleName()).start();
    URL url = sparkManager.getServiceURL(1, TimeUnit.MINUTES);
    Assert.assertNotNull(url);
    URL reportURL = url.toURI().resolve("reports/").toURL();
    // create a report generation request with a time range that contains no program run
    long startSecs = TimeUnit.MILLISECONDS.toSeconds(currentTime) + TimeUnit.DAYS.toSeconds(2);
    ReportGenerationRequest request =
      new ReportGenerationRequest("empty_report", startSecs, startSecs + 30,
                                  new ArrayList<>(ReportField.FIELD_NAME_MAP.keySet()),
                                  Collections.emptyList(), Collections.emptyList());
    // generate a report and wait until it is completed
    String reportId = generateReport(request, reportURL);
    URL reportIdURL = reportURL.toURI().resolve(reportId + "/").toURL();
    // assert the report details is empty
    URL reportRunsURL = reportIdURL.toURI().resolve("details").toURL();
    ReportContent reportContent = getResponseObject(reportRunsURL.openConnection(), REPORT_CONTENT_TYPE);
    Assert.assertEquals(0, reportContent.getTotal());
    Assert.assertEquals(0, reportContent.getDetails().size());
    // create a report generation request with filters on namespaces and duration. Include all fields in the report
    // and sort the report by duration in descending order
    List<Filter> filters =
      ImmutableList.of(
        new ValueFilter<>(Constants.NAMESPACE, ImmutableSet.of("ns1", "ns2"), null),
        new RangeFilter<>(Constants.DURATION, new RangeFilter.Range<>(null, 500L)));
    startSecs = TimeUnit.MILLISECONDS.toSeconds(currentTime);
    request =
      new ReportGenerationRequest("ns1_ns2_report", startSecs, startSecs + 30,
                                  new ArrayList<>(ReportField.FIELD_NAME_MAP.keySet()),
                                  ImmutableList.of(new Sort(Constants.DURATION, Sort.Order.DESCENDING)), filters);
    // generate a report and wait until it is completed
    reportId = generateReport(request, reportURL);
    reportIdURL = reportURL.toURI().resolve(reportId + "/").toURL();
    ReportGenerationInfo reportGenerationInfo = getResponseObject(reportIdURL.openConnection(),
                                                                  REPORT_GEN_INFO_TYPE);
    // assert the summary content is expected
    ReportSummary summary = reportGenerationInfo.getSummary();
    Assert.assertNotNull(summary);
    Assert.assertEquals(ImmutableSet.of(new NamespaceAggregate("ns1", 1), new NamespaceAggregate("ns2" , 1)),
                        new HashSet<>(summary.getNamespaces()));
    Assert.assertEquals(ImmutableSet.of(new ArtifactAggregate("Artifact", "1.0.0", "USER", 2)),
                        new HashSet<>(summary.getArtifacts()));
    DurationStats durationStats = summary.getDurations();
    Assert.assertEquals(300L, durationStats.getMin());
    Assert.assertEquals(300L, durationStats.getMax());
    // averages with difference smaller than 0.01 are considered equal
    Assert.assertTrue(Math.abs(300.0 - durationStats.getAverage()) < 0.01);
    Assert.assertEquals(new StartStats(startSecs, startSecs), summary.getStarts());
    Assert.assertEquals(ImmutableSet.of(new UserAggregate("alice", 2)), new HashSet<>(summary.getOwners()));
    Assert.assertEquals(ImmutableSet.of(new StartMethodAggregate(ProgramRunStartMethod.TRIGGERED, 2)),
                        new HashSet<>(summary.getStartMethods()));
    // assert the number of report details is correct
    reportRunsURL = reportIdURL.toURI().resolve("details").toURL();
    reportContent = getResponseObject(reportRunsURL.openConnection(), REPORT_CONTENT_TYPE);
    Assert.assertEquals(2, reportContent.getTotal());
    // Assert that all the records in the report contain startMethod TRIGGERED
    boolean startMethodIsCorrect =
      reportContent.getDetails().stream().allMatch(content -> content.contains("\"startMethod\":\"TRIGGERED\""));
    if (!startMethodIsCorrect) {
      Assert.fail("All report records are expected to contain startMethod TRIGGERED, " +
                    "but actual results do not meet this requirement: " + reportContent.getDetails());
    }
    // save the report with a new name and description
    URL reportSaveURL = reportURL.toURI().resolve(reportId + "/" + "save").toURL();
    HttpURLConnection urlConn = (HttpURLConnection) reportSaveURL.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod("POST");
    urlConn.getOutputStream().write(GSON.toJson(new ReportSaveRequest("newName", "newDescription"))
                                      .getBytes(StandardCharsets.UTF_8));
    if (urlConn.getErrorStream() != null) {
      Assert.fail(Bytes.toString(ByteStreams.toByteArray(urlConn.getErrorStream())));
    }
    Assert.assertEquals(200, urlConn.getResponseCode());
    // verify that the name and description of the report have been updated, and the expiry time is null
    reportGenerationInfo = getResponseObject(reportIdURL.openConnection(), REPORT_GEN_INFO_TYPE);
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
    urlConn = (HttpURLConnection) reportIdURL.openConnection();
    urlConn.setRequestMethod("DELETE");
    Assert.assertEquals(200, urlConn.getResponseCode());
    // getting status of a deleted report will get 404
    Assert.assertEquals(404, ((HttpURLConnection) reportIdURL.openConnection()).getResponseCode());
    // deleting a deleted report again will get 404
    urlConn = (HttpURLConnection) reportIdURL.openConnection();
    urlConn.setRequestMethod("DELETE");
    Assert.assertEquals(404, urlConn.getResponseCode());
  }

  private static String generateReport(ReportGenerationRequest request, URL reportURL) throws Exception {
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
    URL reportIdURL = reportURL.toURI().resolve(reportId + "/").toURL();
    Tasks.waitFor(ReportStatus.COMPLETED, () -> {
      ReportGenerationInfo reportGenerationInfo = getResponseObject(reportIdURL.openConnection(),
                                                                    REPORT_GEN_INFO_TYPE);
      if (ReportStatus.FAILED.equals(reportGenerationInfo.getStatus())) {
        Assert.fail("Report generation failed");
      }
      return reportGenerationInfo.getStatus();
    }, 5, TimeUnit.MINUTES, 2, TimeUnit.SECONDS);
    return reportId;
  }

  private static <T> T getResponseObject(URLConnection urlConnection, Type typeOfT) throws IOException {
    String response;
    try {
      response = new String(ByteStreams.toByteArray(urlConnection.getInputStream()), Charsets.UTF_8);
    } finally {
      ((HttpURLConnection) urlConnection).disconnect();
    }
    LOG.info(response);
    return DES_GSON.fromJson(response, typeOfT);
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
      new ProgramStartInfo(ImmutableMap.of(Constants.Notification.SCHEDULE_INFO_KEY, scheduleInfo),
                           new ArtifactId("Artifact", new ArtifactVersion("1.0.0"), ArtifactScope.USER), "alice");
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
