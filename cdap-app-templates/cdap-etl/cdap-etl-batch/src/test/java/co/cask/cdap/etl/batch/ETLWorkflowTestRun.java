/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.workflow.NodeStatus;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.mock.batch.MockExternalSink;
import co.cask.cdap.etl.mock.batch.MockExternalSource;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.batch.NodeStatesAction;
import co.cask.cdap.etl.mock.transform.ErrorTransform;
import co.cask.cdap.etl.mock.transform.StringValueFilterTransform;
import co.cask.cdap.etl.proto.Engine;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.cdap.proto.PluginInstanceDetail;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests for ETLBatch.
 */
public class ETLWorkflowTestRun extends ETLBatchTestBase {

  @Test
  public void testInvalidTransformConfigFailsToDeploy() {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("inputTable")))
      .addStage(new ETLStage("sink", MockSink.getPlugin("outputTable")))
      .addStage(new ETLStage("transform", StringValueFilterTransform.getPlugin(null, null)))
      .addConnection("source", "transform")
      .addConnection("transform", "sink")
      .build();
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);

    ApplicationId appId = NamespaceId.DEFAULT.app("badConfig");
    try {
      deployApplication(appId.toId(), appRequest);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testPostAction() throws Exception {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("actionInput")))
      .addStage(new ETLStage("sink", MockSink.getPlugin("actionOutput")))
      .addPostAction(new ETLStage("tokenWriter", NodeStatesAction.getPlugin("tokenTable")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ActionApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(schema).set("name", "jane").build();

    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.toId(), "actionInput");
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob, recordJane));

    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> tokenTableManager = getDataset(NamespaceId.DEFAULT.toId(), "tokenTable");
    Table tokenTable = tokenTableManager.get();
    NodeStatus status = NodeStatus.valueOf(Bytes.toString(
      tokenTable.get(Bytes.toBytes(ETLMapReduce.NAME), Bytes.toBytes("status"))));
    Assert.assertEquals(NodeStatus.COMPLETED, status);
  }

  @Test
  public void testDAG() throws Exception {
    /*
     *            ----- error transform ------------
     *           |                                  |---- sink1
     * source --------- string value filter --------
     *           |                                  |---- sink2
     *            ----------------------------------
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("daginput")))
      .addStage(new ETLStage("sink1", MockSink.getPlugin("dagoutput1")))
      .addStage(new ETLStage("sink2", MockSink.getPlugin("dagoutput2")))
      .addStage(new ETLStage("error", ErrorTransform.getPlugin(), "errors"))
      .addStage(new ETLStage("filter", StringValueFilterTransform.getPlugin("name", "samuel")))
      .addConnection("source", "error")
      .addConnection("source", "filter")
      .addConnection("source", "sink2")
      .addConnection("error", "sink1")
      .addConnection("filter", "sink1")
      .addConnection("filter", "sink2")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("DagApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(schema).set("name", "jane").build();

    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.toId(), "daginput");
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob, recordJane));

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    // sink1 should only get non-samuel records
    DataSetManager<Table> sink1Manager = getDataset("dagoutput1");
    Set<StructuredRecord> expected = ImmutableSet.of(recordBob, recordJane);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sink1Manager));
    Assert.assertEquals(expected, actual);

    // sink2 should get bob and jane from the filter, plus everything again from the source
    Map<String, Integer> expectedCounts = ImmutableMap.of("samuel", 1, "bob", 2, "jane", 2);
    DataSetManager<Table> sink2Manager = getDataset("dagoutput2");
    Map<String, Integer> actualCounts = new HashMap<>();
    actualCounts.put("samuel", 0);
    actualCounts.put("bob", 0);
    actualCounts.put("jane", 0);
    for (StructuredRecord record : MockSink.readOutput(sink2Manager)) {
      String name = record.get("name");
      actualCounts.put(name, actualCounts.get(name) + 1);
    }
    Assert.assertEquals(expectedCounts, actualCounts);

    // error dataset should have all records
    Set<StructuredRecord> expectedErrors = ImmutableSet.of(recordSamuel, recordBob, recordJane);
    Set<StructuredRecord> actualErrors = new HashSet<>();
    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset("errors");
    try (TimePartitionedFileSet fileSet = fileSetManager.get()) {
      List<GenericRecord> records = readOutput(fileSet, Constants.ERROR_SCHEMA);
      for (GenericRecord record : records) {
        StructuredRecord invalidRecord = StructuredRecordStringConverter.fromJsonString(
          record.get(Constants.ErrorDataset.INVALIDENTRY).toString(), schema);
        actualErrors.add(invalidRecord);
      }
    }
    Assert.assertEquals(expectedErrors, actualErrors);
  }

  @Test
  public void testExternalDatasetTrackingMR() throws Exception {
    testExternalDatasetTracking(Engine.MAPREDUCE, false);
  }

  @Test
  public void testExternalDatasetTrackingSpark() throws Exception {
    testExternalDatasetTracking(Engine.SPARK, false);
  }

  @Test
  public void testBackwardsCompatibleExternalDatasetTrackingMR() throws Exception {
    testExternalDatasetTracking(Engine.MAPREDUCE, true);
  }

  @Test
  public void testBackwardsCompatibleExternalDatasetTrackingSpark() throws Exception {
    testExternalDatasetTracking(Engine.SPARK, true);
  }

  // todo : move this test to AppLifecycleHTTPHandlerTest, here currently due to availability of app with plugins.
  @Test
  public void testGetPlugins() throws Exception {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("daginput")))
      .addStage(new ETLStage("sink1", MockSink.getPlugin("dagoutput1")))
      .addStage(new ETLStage("sink2", MockSink.getPlugin("dagoutput2")))
      .addStage(new ETLStage("filter", StringValueFilterTransform.getPlugin("name", "samuel")))
      .addConnection("source", "filter")
      .addConnection("source", "sink1")
      .addConnection("filter", "sink2")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("DagApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);
    Set<String> expectedPluginIds = new HashSet<String>();
    expectedPluginIds.add("source");
    expectedPluginIds.add("sink1");
    expectedPluginIds.add("sink2");
    expectedPluginIds.add("filter");

    Assert.assertEquals(expectedPluginIds.size(), appManager.getPlugins().size());
    for (PluginInstanceDetail pluginInstanceDetail : appManager.getPlugins()) {
      Assert.assertTrue(expectedPluginIds.contains(pluginInstanceDetail.getId()));
      expectedPluginIds.remove(pluginInstanceDetail.getId());
    }
    Assert.assertTrue(expectedPluginIds.isEmpty());
  }

  private void testExternalDatasetTracking(Engine engine, boolean backwardsCompatible) throws Exception {
    String suffix = engine.name() + (backwardsCompatible ? "-bc" : "");

    // Define input/output datasets
    String expectedExternalDatasetInput = "fileInput-" + suffix;
    String expectedExternalDatasetOutput = "fileOutput-" + suffix;

    // Define input/output directories
    File inputDir = TMP_FOLDER.newFolder("input-" + suffix);
    String inputFile = "input-file1.txt";
    File outputDir = TMP_FOLDER.newFolder("output-" + suffix);
    File outputSubDir1 = new File(outputDir, "subdir1");
    File outputSubDir2 = new File(outputDir, "subdir2");

    if (!backwardsCompatible) {
      // Assert that there are no external datasets
      Assert.assertNull(getDataset(NamespaceId.DEFAULT.toId(), expectedExternalDatasetInput).get());
      Assert.assertNull(getDataset(NamespaceId.DEFAULT.toId(), expectedExternalDatasetOutput).get());
    }

    ETLBatchConfig.Builder builder = ETLBatchConfig.builder("* * * * *");
    ETLBatchConfig etlConfig = builder
      .setEngine(engine)
      // TODO: test multiple inputs CDAP-5654
      .addStage(new ETLStage("source", MockExternalSource.getPlugin(expectedExternalDatasetInput,
                                                                    inputDir.getAbsolutePath())))
      .addStage(new ETLStage("sink1", MockExternalSink.getPlugin(
        backwardsCompatible ? null : expectedExternalDatasetOutput, "dir1", outputSubDir1.getAbsolutePath())))
      .addStage(new ETLStage("sink2", MockExternalSink.getPlugin(
        backwardsCompatible ? null : expectedExternalDatasetOutput, "dir2", outputSubDir2.getAbsolutePath())))
      .addConnection("source", "sink1")
      .addConnection("source", "sink2")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ExternalDatasetApp-" + suffix);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(schema).set("name", "jane").build();
    ImmutableList<StructuredRecord> allInput = ImmutableList.of(recordSamuel, recordBob, recordJane);

    // Create input files
    MockExternalSource.writeInput(new File(inputDir, inputFile).getAbsolutePath(), allInput);

    final WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
    // make sure the completed run record is stamped, before asserting it.
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return workflowManager.getHistory(ProgramRunStatus.COMPLETED).size();
      }
    }, 5, TimeUnit.MINUTES);
    List<RunRecord> history = workflowManager.getHistory();
    // there should be only one completed run
    Assert.assertEquals(1, history.size());
    Assert.assertEquals(ProgramRunStatus.COMPLETED, history.get(0).getStatus());

    // Assert output
    Assert.assertEquals(allInput, MockExternalSink.readOutput(outputSubDir1.getAbsolutePath()));
    Assert.assertEquals(allInput, MockExternalSink.readOutput(outputSubDir2.getAbsolutePath()));

    if (!backwardsCompatible) {
      // Assert that external datasets got created
      Assert.assertNotNull(getDataset(NamespaceId.DEFAULT.toId(), expectedExternalDatasetInput).get());
      Assert.assertNotNull(getDataset(NamespaceId.DEFAULT.toId(), expectedExternalDatasetOutput).get());
    }
  }
}
