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

import co.cask.cdap.api.app.Application;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;

/**
 * Test for {@link ProgramOperationReportApp}.
 */
public class ProgramRunReportTest extends TestBase {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Logger LOG = LoggerFactory.getLogger(ProgramRunReportTest.class);
  private static final Gson GSON = new Gson();
  private static String output;
  private static String metaFile;
  private static Class<? extends Application> reportAppClass;
  private static File reportApp;

  @BeforeClass
  public static void init() throws Exception {
    output = TEMP_FOLDER.newFolder().getAbsolutePath();
    reportAppClass = ProgramOperationReportApp.class;
    reportApp = createArtifactJarWithAvro(reportAppClass);
    File tmpFile = File.createTempFile("ProgramRunMeta", ".avro", TEMP_FOLDER.newFolder());
    metaFile = tmpFile.getAbsolutePath();
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(ProgramRunMetaFileUtil.SCHEMA);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(ProgramRunMetaFileUtil.SCHEMA, tmpFile);
    String program = "SmartWorkflow";
    String run = "randomRunId";
    long time = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - TimeUnit.DAYS.toSeconds(1);
    long delay = TimeUnit.MINUTES.toSeconds(5);
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program, run, "STARTING", time,
                                                              ProgramRunMetaFileUtil.startingInfo("user")));
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program, run, "RUNNING",
                                                              time + delay, null));
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program + "_1", run, "STARTING",
                                                              time + delay, null));
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program + "_1", run, "RUNNING",
                                                              time + 2 * delay, null));
    dataFileWriter.close();
  }

  private static File createArtifactJarWithAvro(Class<? extends Application> appClass) throws IOException {
    Manifest manifest = new Manifest();
    File avroJar =
      new File("/Users/Chengfeng/.m2/repository/com/databricks/spark-avro_2.10/3.2.0/spark-avro_2.10-3.2.0.jar");
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, appClass.getPackage().getName());
    return new File(AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
                                                     appClass, manifest, avroJar).toURI());
  }

  @Test
  public void testReportGeneration() throws Exception {
    File avroJar =
      new File("/Users/Chengfeng/.m2/repository/com/databricks/spark-avro_2.10/3.2.0/spark-avro_2.10-3.2.0.jar");
    ApplicationManager app = deployApplication(reportAppClass, reportApp, avroJar);
    SparkManager sparkManager = app.getSparkManager(ReportGenerationSpark.class.getSimpleName())
      .start(ImmutableMap.of("input", metaFile, "output", output));
    URL url = sparkManager.getServiceURL(5, TimeUnit.MINUTES);
    Assert.assertNotNull(url);
//    URL reportURL = url.toURI().resolve("reports").toURL();
//    HttpURLConnection urlConnection = (HttpURLConnection) reportURL.openConnection();
//
//    // POST request
//    urlConnection.setDoOutput(true);
//    try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(urlConnection.getOutputStream(), "UTF-8"))) {
//      writer.print(GSON.toJson(MOCK_REPORT_GENERATION_REQUEST));
//    }
//    Assert.assertEquals(200, urlConnection.getResponseCode());
    File successFile = new File(output + "/_SUCCESS");
    Tasks.waitFor(true, () -> (successFile.exists()), 5, TimeUnit.MINUTES);
    LOG.info("Report file: {}", Files.readAllLines(Paths.get(output + "/reportId.json")));
  }
}
