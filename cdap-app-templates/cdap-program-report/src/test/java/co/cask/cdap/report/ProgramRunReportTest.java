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

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestBaseWithSpark2;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ProgramRunReportTest extends TestBaseWithSpark2 {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Logger LOG = LoggerFactory.getLogger(ProgramRunReportTest.class);
  private static final Gson GSON = new Gson();
  private static String reportBasePath;
  private static String metaBasePath;

  @BeforeClass
  public static void init() throws Exception {
    reportBasePath = new File(TEMP_FOLDER.newFolder(), "report").getAbsolutePath();
    metaBasePath =  new File(TEMP_FOLDER.newFolder(), "meta").getAbsolutePath();
  }

  @Test
  public void testGenerateReport() throws Exception {
    generateProgramRunMetaFiles();
    LOG.info("Generated run meta files");
    ApplicationManager app = deployApplication(ProgramOperationReportApp.class);
    SparkManager sparkManager = app.getSparkManager(ReportGenerationSpark.class.getSimpleName()).start();
    URL url = sparkManager.getServiceURL(1, TimeUnit.MINUTES);
    Assert.assertNotNull(url);
    URL reportURL = url.toURI().resolve("reports").toURL();
    List<ReportGenerationRequest.Filter> filters =
      ImmutableList.of(
        new ReportGenerationRequest.ValueFilter<>("namespace", ImmutableList.of("ns1", "ns2"), null),
        new ReportGenerationRequest.RangeFilter<>("duration", new ReportGenerationRequest.Range<>(null, 30L)));
    ReportGenerationRequest request =
      new ReportGenerationRequest(1520808000L, 1520808301L, ImmutableList.of("namespace", "duration", "user"),
                                  ImmutableList.of(
                                    new ReportGenerationRequest.Sort("duration",
                                                                     ReportGenerationRequest.Order.DESCENDING)),
                                  filters);
    HttpURLConnection urlConn = (HttpURLConnection) reportURL.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod("POST");
    urlConn.getOutputStream().write(GSON.toJson(request).getBytes(StandardCharsets.UTF_8));
    int responseCode = urlConn.getResponseCode();
    Assert.assertEquals(200, responseCode);
    String msg = urlConn.getResponseMessage();
    Map<String, String> responseMap = GSON.fromJson(msg, new TypeToken<Map<String, String>>() { }.getType());
  }

  private void generateProgramRunMetaFiles() throws Exception {
    addDatasetInstance(FileSet.class.getName(), ProgramOperationReportApp.RUN_META_FILESET,
                       FileSetProperties.builder().setBasePath(metaBasePath).build());
    DataSetManager<FileSet> runMeta = getDataset(ProgramOperationReportApp.RUN_META_FILESET);
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(ProgramRunMetaFileUtil.SCHEMA);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    for (String namespace : ImmutableList.of("default", "ns1", "ns2")) {
      Location nsLocation = runMeta.get().getBaseLocation().append(namespace);
      nsLocation.mkdirs();
      LOG.info("nsLocation {} exists='{}', isDir='{}'", nsLocation, nsLocation.exists(), nsLocation.isDirectory());
      for (int i = 0; i < 5; i++) {
        long time = 1520808000L + 1000 * i;
        Location reportLocation = nsLocation.append(Long.toString(time));
        reportLocation.createNew();
        LOG.info("reportLocation {} exists='{}', isDir='{}'", reportLocation, reportLocation.exists(),
                 reportLocation.isDirectory());
        dataFileWriter.create(ProgramRunMetaFileUtil.SCHEMA, reportLocation.getOutputStream());
        String program = namespace + ".SmartWorkflow";
        String run = "randomRunId";
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
      LOG.info("nsLocation.list() = {}", nsLocation.list());
    }
  }
}
