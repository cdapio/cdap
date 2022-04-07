/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.datastreams;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.etl.mock.spark.streaming.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestConfiguration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Test cases in this class should be part of {@link DataStreamsTest}.
 * However we are running into permgen issues after adding it in the DataStreamsTest.
 * Corresponding JIRA is CDAP-11577. Once the JIRA is fixed, these tests can be
 * moved to DataStreamsTest.
 */
public class DataStreamsSparkSinkTest  extends HydratorTestBase {

  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");
  private static int startCount;
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.AppFabric.SPARK_COMPAT,
                                                                       Compat.SPARK_COMPAT);

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    setupStreamingArtifacts(APP_ARTIFACT_ID, DataStreamsApp.class);
  }

  @Test
  // TODO Move this test case to DataStreamsTest once CDAP-11577 is fixed.
  // In the heapdump, we noticed that multiple instances of the SparkRunnerClassLoader were held by
  // stream-rate-updater thread in Spark.
  public void testSparkSink() throws Exception {
    Schema schema = Schema.recordOf(
      "test",
      Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );
    List<StructuredRecord> input = new ArrayList<>();
    StructuredRecord samuelRecord = StructuredRecord.builder(schema).set("id", "0").set("name", "samuel").build();
    StructuredRecord jacksonRecord = StructuredRecord.builder(schema).set("id", "1").set("name", "jackson").build();
    StructuredRecord dwayneRecord = StructuredRecord.builder(schema).set("id", "2").set("name", "dwayne").build();
    StructuredRecord johnsonRecord = StructuredRecord.builder(schema).set("id", "3").set("name", "johnson").build();
    input.add(samuelRecord);
    input.add(jacksonRecord);
    input.add(dwayneRecord);
    input.add(johnsonRecord);

    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(schema, input)))
      .addStage(new ETLStage("sink", io.cdap.cdap.etl.mock.spark.streaming.MockSink.getPlugin("${tablename}")))
      .addConnection("source", "sink")
      .setCheckpointDir("file://" + TMP_FOLDER.getRoot().toPath().toString())
      .setBatchInterval("1s")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("sparksinkapp");
    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    testSparkSink(appManager, "output1");
    testSparkSink(appManager, "output2");
  }

  private void testSparkSink(ApplicationManager appManager, final String output) throws Exception {
    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start(ImmutableMap.of("tablename", output));
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return getDataset(output).get() != null;
      }
    }, 1, TimeUnit.MINUTES);

    final DataSetManager<KeyValueTable> outputManager = getDataset(output);
    final Map<String, String> expectedKeyValues = ImmutableMap.of("0", "samuel", "1", "jackson", "2", "dwayne", "3",
                                                                  "johnson");
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          outputManager.flush();
          Map<String, String> keyValues
            = io.cdap.cdap.etl.mock.spark.streaming.MockSink.getValues(expectedKeyValues.keySet(), outputManager);
          return expectedKeyValues.equals(keyValues);
        }
      }, 1, TimeUnit.MINUTES);

    sparkManager.stop(null);
    sparkManager.waitForStopped(10, TimeUnit.SECONDS);
    sparkManager.waitForRun(ProgramRunStatus.KILLED, 10, TimeUnit.SECONDS);
  }
}
