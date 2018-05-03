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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.report.proto.Filter;
import co.cask.cdap.report.proto.RangeFilter;
import co.cask.cdap.report.proto.ReportContent;
import co.cask.cdap.report.proto.ReportGenerationInfo;
import co.cask.cdap.report.proto.ReportGenerationRequest;
import co.cask.cdap.report.proto.ReportStatus;
import co.cask.cdap.report.proto.Sort;
import co.cask.cdap.report.proto.ValueFilter;
import co.cask.cdap.report.util.ProgramRunMetaFileUtil;
import co.cask.cdap.report.util.ReportIds;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestBaseWithSpark2;
import co.cask.cdap.test.TestConfiguration;
import com.databricks.spark.avro.DefaultSource;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.file.DataFileWriter;
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
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link ReportGenerationApp}.
 */
public class ReportGenerationAppTest extends TestBaseWithSpark2 {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  @ClassRule
  public static final TestConfiguration CONF = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, "false");

  private static final Logger LOG = LoggerFactory.getLogger(ReportGenerationAppTest.class);
  private static final Gson GSON = new Gson();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type REPORT_GEN_INFO_TYPE = new TypeToken<ReportGenerationInfo>() { }.getType();
  private static final Type REPORT_CONTENT_TYPE = new TypeToken<ReportContent>() { }.getType();

  @Test
  public void testGenerateReport() throws Exception {
    DatasetId metaFileset = NamespaceId.DEFAULT.dataset(ReportGenerationApp.RUN_META_FILESET);
    addDatasetInstance(metaFileset, FileSet.class.getName());
    // TODO: [CDAP-13216] temporarily create the run meta fileset and generate mock program run meta files here.
    // Will remove once the TMS subscriber writing to the run meta fileset is implemented.
    DataSetManager<FileSet> fileSet = getDataset(metaFileset);
    populateMetaFiles(fileSet.get().getBaseLocation());

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

    ApplicationManager app = deployApplication(ReportGenerationApp.class, new File(unJarDir, "lib").listFiles());
    SparkManager sparkManager = app.getSparkManager(ReportGenerationSpark.class.getSimpleName()).start();
    URL url = sparkManager.getServiceURL(1, TimeUnit.MINUTES);
    Assert.assertNotNull(url);
    URL reportURL = url.toURI().resolve("reports/").toURL();
    List<Filter> filters =
      ImmutableList.of(
        new ValueFilter<>("namespace", ImmutableSet.of("ns1", "ns2"), null),
        new RangeFilter<>("duration", new RangeFilter.Range<>(500L, null)));
    ReportGenerationRequest request =
      new ReportGenerationRequest(1520808000L, 1520808301L, ImmutableList.of("namespace", "duration"),
                                  ImmutableList.of(
                                    new Sort("duration",
                                             Sort.Order.DESCENDING)),
                                  filters);
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
    URL reportStatusURL = reportURL.toURI().resolve(reportId + "/").toURL();
    Tasks.waitFor(ReportStatus.COMPLETED, () -> {
      ReportGenerationInfo reportGenerationInfo = getResponseObject(reportStatusURL.openConnection(),
                                                                    REPORT_GEN_INFO_TYPE);
      if (ReportStatus.FAILED.equals(reportGenerationInfo.getStatus())) {
        Assert.fail("Report generation failed");
      }
      return reportGenerationInfo.getStatus();
    }, 5, TimeUnit.MINUTES, 2, TimeUnit.SECONDS);
    URL reportRunsURL = reportStatusURL.toURI().resolve("details").toURL();
    ReportContent reportContent = getResponseObject(reportRunsURL.openConnection(), REPORT_CONTENT_TYPE);
    Assert.assertEquals(2, reportContent.getTotal());
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
   */
  private static void populateMetaFiles(Location metaBaseLocation) throws Exception {
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(ProgramRunMetaFileUtil.SCHEMA);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    for (String namespace : ImmutableList.of("default", "ns1", "ns2")) {
      Location nsLocation = metaBaseLocation.append(namespace);
      nsLocation.mkdirs();
      for (int i = 0; i < 5; i++) {
        long time = 1520808000L + 1000 * i;
        // expected format is <event-timestamp-millis>-<creation-timestamp-millis>.avro
        Location reportLocation = nsLocation.append(String.format("%d-%d.avro",
                                                                  TimeUnit.SECONDS.toMillis(time),
                                                                  System.currentTimeMillis()));
        reportLocation.createNew();
        dataFileWriter.create(ProgramRunMetaFileUtil.SCHEMA, reportLocation.getOutputStream());
        String program = "SmartWorkflow";
        String run1 = ReportIds.generate().toString();
        String run2 = ReportIds.generate().toString();
        long delay = TimeUnit.MINUTES.toSeconds(5);
        dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(namespace, program, run1, "STARTING",
                                                                  time, new StartInfo("user",
                                                                                      ImmutableMap.of("k1", "v1",
                                                                                                      "k2", "v2"))));
        dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(namespace, program, run1,
                                                                  "FAILED", time + delay, null));
        dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(namespace, program + "_1", run2,
                                                                  "STARTING", time + delay, null));
        dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(namespace, program + "_1", run2,
                                                                  "RUNNING", time + 2 * delay, null));
        dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(namespace, program + "_1", run2,
                                                                  "COMPLETED", time + 4 * delay, null));
        dataFileWriter.close();
      }
      LOG.debug("nsLocation.list() = {}", nsLocation.list());
    }
  }
}
