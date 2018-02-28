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

import co.cask.cdap.api.app.Application;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ops.ReportGenerationRequest;
import co.cask.cdap.proto.ops.ReportGenerationStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Test {@link ReportGenerationSpark}
 */
public class ReportGenerationTest extends TestFrameworkTestBase {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = new Gson();
  private static final ReportGenerationRequest MOCK_REPORT_GENERATION_REQUEST =
    new ReportGenerationRequest(0, 1, ImmutableList.of("namespace", "duration", "user"),
                                ImmutableList.of(
                                  new ReportGenerationRequest.Sort("duration",
                                                                   ReportGenerationRequest.Order.DESCENDING)),
                                ImmutableList.of(
                                  new ReportGenerationRequest.RangeFilter<>("duration",
                                                                            new ReportGenerationRequest.Range<>(null,
                                                                                                                30L))));

  private static String output;
  private static String metaFile;
  private static Class<? extends Application> reportAppClass;
  private static File reportApp;

  @BeforeClass
  public static void init() throws Exception {
    output = TEMP_FOLDER.newFile("reportId.json").getAbsolutePath();
    reportAppClass = ProgramOperationReportApp.class;
    reportApp = createArtifactJar(reportAppClass);
    File tmpFile = File.createTempFile("ProgramRunMeta", ".avro", TEMP_FOLDER.newFolder());
    metaFile = tmpFile.getAbsolutePath();
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(ProgramRunMetaFileUtil.SCHEMA);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(ProgramRunMetaFileUtil.SCHEMA, tmpFile);
    String program = "SmartWorkflow";
    String run = "randomRunId";
    long time = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - TimeUnit.DAYS.toSeconds(1);
    long delay = TimeUnit.MINUTES.toSeconds(5);
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program, run, ProgramRunStatus.STARTING, time,
                                                              ProgramRunMetaFileUtil.startingInfo("user")));
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program, run, ProgramRunStatus.RUNNING,
                                                              time + delay, null));
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program + "_1", run, ProgramRunStatus.STARTING,
                                                              time + delay, null));
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program + "_1", run, ProgramRunStatus.RUNNING,
                                                              time + 2 * delay, null));
    dataFileWriter.close();
  }

  @Test
  public void testReportGeneration() throws Exception {
    ApplicationManager app = deployApplication(reportAppClass, reportApp);
    SparkManager sparkManager = app.getSparkManager(ReportGenerationSpark.class.getSimpleName())
      .start(ImmutableMap.of("input", metaFile, "output", output));
    URL url = sparkManager.getServiceURL(5, TimeUnit.MINUTES);
    Assert.assertNotNull(url);
    URL reportURL = url.toURI().resolve("reports").toURL();
    HttpURLConnection urlConnection = (HttpURLConnection) reportURL.openConnection();

    // POST request
    urlConnection.setDoOutput(true);
    try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(urlConnection.getOutputStream(), "UTF-8"))) {
      writer.print(GSON.toJson(MOCK_REPORT_GENERATION_REQUEST));
    }
    Assert.assertEquals(200, urlConnection.getResponseCode());
    URL reportStatusURL = url.toURI().resolve("reports/reportId").toURL();
    Tasks.waitFor(true, () -> ((HttpURLConnection) reportStatusURL.openConnection()).getResponseMessage()
       .contains(ReportGenerationStatus.COMPLETED.name()), 5, TimeUnit.MINUTES);
    Assert.assertEquals(200, urlConnection.getResponseCode());
  }
}
