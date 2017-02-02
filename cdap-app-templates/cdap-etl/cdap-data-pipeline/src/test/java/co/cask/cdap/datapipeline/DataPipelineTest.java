/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.datapipeline;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.workflow.NodeStatus;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.datapipeline.mock.NaiveBayesClassifier;
import co.cask.cdap.datapipeline.mock.NaiveBayesTrainer;
import co.cask.cdap.datapipeline.mock.SpamMessage;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.mock.action.MockAction;
import co.cask.cdap.etl.mock.batch.MockExternalSink;
import co.cask.cdap.etl.mock.batch.MockExternalSource;
import co.cask.cdap.etl.mock.batch.MockRuntimeDatasetSink;
import co.cask.cdap.etl.mock.batch.MockRuntimeDatasetSource;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.batch.NodeStatesAction;
import co.cask.cdap.etl.mock.batch.aggregator.FieldCountAggregator;
import co.cask.cdap.etl.mock.batch.aggregator.GroupFilterAggregator;
import co.cask.cdap.etl.mock.batch.aggregator.IdentityAggregator;
import co.cask.cdap.etl.mock.batch.joiner.MockJoiner;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.mock.transform.DropNullTransform;
import co.cask.cdap.etl.mock.transform.FilterErrorTransform;
import co.cask.cdap.etl.mock.transform.FlattenErrorTransform;
import co.cask.cdap.etl.mock.transform.IdentityTransform;
import co.cask.cdap.etl.mock.transform.StringValueFilterTransform;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.proto.Engine;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class DataPipelineTest extends HydratorTestBase {

  protected static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  protected static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");
  private static int startCount = 0;
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.Security.Store.PROVIDER, "file");

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }
    setupBatchArtifacts(APP_ARTIFACT_ID, DataPipelineApp.class);

    // add some test plugins
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), APP_ARTIFACT_ID,
                      NaiveBayesTrainer.class, NaiveBayesClassifier.class);
  }

  @After
  public void cleanupTest() throws Exception {
    getMetricsManager().resetAll();
  }

  @Test
  public void testMacroActionPipelines() throws Exception {
    testMacroEvaluationActionPipeline(Engine.MAPREDUCE);
    testMacroEvaluationActionPipeline(Engine.SPARK);
  }

  public void testMacroEvaluationActionPipeline(Engine engine) throws Exception {
    ETLStage action1 = new ETLStage("action1", MockAction.getPlugin("actionTable", "action1.row", "action1.column",
                                                                    "${value}"));
    ETLStage action2 = new ETLStage("action2", MockAction.getPlugin("actionTable", "action2.row", "action2.column",
                                                                    "action2.value"));

    ETLBatchConfig etlConfig = co.cask.cdap.etl.proto.v2.ETLBatchConfig.builder("* * * * *")
      .addStage(action1)
      .addStage(action2)
      .addConnection(new Connection(action1.getName(), action2.getName()))
      .setEngine(engine)
      .build();

    // set runtime arguments for macro substitution
    Map<String, String> runtimeArguments = ImmutableMap.of("value", "macroValue");

    AppRequest<co.cask.cdap.etl.proto.v2.ETLBatchConfig> appRequest =
      new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("macroActionTest-" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.setRuntimeArgs(runtimeArguments);
    manager.start(ImmutableMap.of("logical.start.time", "0"));
    manager.waitForRun(ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    DataSetManager<Table> actionTableDS = getDataset("actionTable");
    Assert.assertEquals("macroValue", MockAction.readOutput(actionTableDS, "action1.row", "action1.column"));

    appManager.getHistory(appId.workflow(SmartWorkflow.NAME).toId(), ProgramRunStatus.FAILED);
  }

  @Test
  public void testErrorTransform() throws Exception {
    testErrorTransform(Engine.MAPREDUCE);
    testErrorTransform(Engine.SPARK);
  }

  private void testErrorTransform(Engine engine) throws Exception {
    String source1TableName = "errTestIn1-" + engine;
    String source2TableName = "errTestIn2-" + engine;
    String sink1TableName = "errTestOut1-" + engine;
    String sink2TableName = "errTestOut2-" + engine;

    Schema inputSchema = Schema.recordOf("user",
                                         Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of("id", Schema.of(Schema.Type.INT)));
    /*
     *
     * source1 --> filter1 --> filter2 --> agg1 --> agg2
     *                |           |         |        |
     *                |-----------|---------|--------|--------|--> errorflatten --> sink1
     *                |                                       |
     *                |                                       |--> errorfilter --> sink2
     *                |
     * source2 --> dropnull
     *
     * arrows coming out the right represent output records
     * arrows coming out the bottom represent error records
     * this will test multiple stages from multiple phases emitting errors to the same stage
     * as well as errors from one stage going to multiple stages
     * and transforms that have an error schema different from their output schema
     */
    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .setEngine(engine)
      .addStage(new ETLStage("source1", MockSource.getPlugin(source1TableName, inputSchema)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(source2TableName, inputSchema)))
      .addStage(new ETLStage("filter1", StringValueFilterTransform.getPlugin("name", "Leo")))
      .addStage(new ETLStage("filter2", StringValueFilterTransform.getPlugin("name", "Ralph")))
      .addStage(new ETLStage("agg1", GroupFilterAggregator.getPlugin("name", "Don")))
      .addStage(new ETLStage("agg2", GroupFilterAggregator.getPlugin("name", "Mike")))
      .addStage(new ETLStage("errorflatten", FlattenErrorTransform.getPlugin()))
      .addStage(new ETLStage("errorfilter", FilterErrorTransform.getPlugin(3)))
      .addStage(new ETLStage("dropnull", DropNullTransform.getPlugin("name")))
      .addStage(new ETLStage("sink1", MockSink.getPlugin(sink1TableName)))
      .addStage(new ETLStage("sink2", MockSink.getPlugin(sink2TableName)))
      .addConnection("source1", "filter1")
      .addConnection("source2", "dropnull")
      .addConnection("filter1", "filter2")
      .addConnection("filter2", "agg1")
      .addConnection("agg1", "agg2")
      .addConnection("filter1", "errorflatten")
      .addConnection("filter1", "errorfilter")
      .addConnection("filter2", "errorflatten")
      .addConnection("filter2", "errorfilter")
      .addConnection("agg1", "errorflatten")
      .addConnection("agg1", "errorfilter")
      .addConnection("agg2", "errorflatten")
      .addConnection("agg2", "errorfilter")
      .addConnection("dropnull", "errorflatten")
      .addConnection("dropnull", "errorfilter")
      .addConnection("errorflatten", "sink1")
      .addConnection("errorfilter", "sink2")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("ErrTransformTest-" + engine);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("name", "Leo").set("id", 1).build(),
      StructuredRecord.builder(inputSchema).set("name", "Ralph").set("id", 2).build(),
      StructuredRecord.builder(inputSchema).set("name", "Don").set("id", 3).build(),
      StructuredRecord.builder(inputSchema).set("name", "Mike").set("id", 4).build());
    DataSetManager<Table> source1Table = getDataset(source1TableName);
    MockSource.writeInput(source1Table, input);
    input = ImmutableList.of(StructuredRecord.builder(inputSchema).set("name", "April").set("id", 5).build());
    DataSetManager<Table> source2Table = getDataset(source2TableName);
    MockSource.writeInput(source2Table, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);


    Schema flattenSchema =
      Schema.recordOf("erroruser",
                      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("errMsg", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("errCode", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                      Schema.Field.of("errStage", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Set<StructuredRecord> expected = ImmutableSet.of(
      StructuredRecord.builder(flattenSchema).set("name", "Leo").set("id", 1)
        .set("errMsg", "bad string value").set("errCode", 1).set("errStage", "filter1").build(),
      StructuredRecord.builder(flattenSchema).set("name", "Ralph").set("id", 2)
        .set("errMsg", "bad string value").set("errCode", 1).set("errStage", "filter2").build(),
      StructuredRecord.builder(flattenSchema).set("name", "Don").set("id", 3)
        .set("errMsg", "bad val").set("errCode", 3).set("errStage", "agg1").build(),
      StructuredRecord.builder(flattenSchema).set("name", "Mike").set("id", 4)
        .set("errMsg", "bad val").set("errCode", 3).set("errStage", "agg2").build(),
      StructuredRecord.builder(flattenSchema).set("name", "April").set("id", 5)
        .set("errMsg", "Field name was not null").set("errCode", 5).set("errStage", "dropnull").build());
    DataSetManager<Table> sink1Table = getDataset(sink1TableName);
    Assert.assertEquals(expected, ImmutableSet.copyOf(MockSink.readOutput(sink1Table)));

    expected = ImmutableSet.of(
      StructuredRecord.builder(inputSchema).set("name", "Leo").set("id", 1).build(),
      StructuredRecord.builder(inputSchema).set("name", "Ralph").set("id", 2).build(),
      StructuredRecord.builder(inputSchema).set("name", "April").set("id", 5).build());
    DataSetManager<Table> sink2Table = getDataset(sink2TableName);
    Assert.assertEquals(expected, ImmutableSet.copyOf(MockSink.readOutput(sink2Table)));

    /*
     *
     * source1 (4) --> filter1 (3) --> filter2 (2) --> agg1 (1) --> agg2
     *                   |                |              |            |
     *                  (1)              (1)            (1)          (1)
     *                   |----------------|--------------|------------|--------|--> errorflatten (5) --> sink1
     *                   |                                                     |
     *                  (1)                                                    |--> errorfilter (3) --> sink2
     *                   |
     * source2 --> dropnull
     */
    validateMetric(4, appId, "source1.records.out");
    validateMetric(1, appId, "source2.records.out");
    validateMetric(1, appId, "dropnull.records.error");
    validateMetric(3, appId, "filter1.records.out");
    validateMetric(1, appId, "filter1.records.error");
    validateMetric(2, appId, "filter2.records.out");
    validateMetric(1, appId, "filter2.records.error");
    validateMetric(1, appId, "agg1.records.out");
    validateMetric(1, appId, "agg1.records.error");
    validateMetric(1, appId, "agg2.records.error");
    validateMetric(5, appId, "errorflatten.records.out");
    validateMetric(3, appId, "errorfilter.records.out");
    validateMetric(5, appId, "sink1.records.in");
    validateMetric(3, appId, "sink2.records.in");
  }

  @Test
  public void testPipelineWithAllActions() throws Exception {
    String actionTable = "actionTable";
    String action1RowKey = "action1.row";
    String action1ColumnKey = "action1.column";
    String action1Value = "action1.value";
    String action2RowKey = "action2.row";
    String action2ColumnKey = "action2.column";
    String action2Value = "action2.value";
    String action3RowKey = "action3.row";
    String action3ColumnKey = "action3.column";
    String action3Value = "action3.value";

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("action1", MockAction.getPlugin(actionTable, action1RowKey, action1ColumnKey,
                                                             action1Value)))
      .addStage(new ETLStage("action2", MockAction.getPlugin(actionTable, action2RowKey, action2ColumnKey,
                                                             action2Value)))
      .addStage(new ETLStage("action3", MockAction.getPlugin(actionTable, action3RowKey, action3ColumnKey,
                                                             action3Value)))
      .addConnection("action1", "action2")
      .addConnection("action1", "action3")
      .setEngine(Engine.MAPREDUCE)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MyActionOnlyApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> actionTableDS = getDataset(actionTable);
    Assert.assertEquals(action1Value, MockAction.readOutput(actionTableDS, action1RowKey, action1ColumnKey));
    Assert.assertEquals(action2Value, MockAction.readOutput(actionTableDS, action2RowKey, action2ColumnKey));
    Assert.assertEquals(action3Value, MockAction.readOutput(actionTableDS, action3RowKey, action3ColumnKey));

    List<RunRecord> history = workflowManager.getHistory(ProgramRunStatus.COMPLETED);
    Assert.assertEquals(1, history.size());
    String runId = history.get(0).getPid();

    WorkflowTokenDetail tokenDetail = workflowManager.getToken(runId, WorkflowToken.Scope.USER,
                                                               action1RowKey + action1ColumnKey);

    validateToken(tokenDetail, action1RowKey + action1ColumnKey, action1Value);

    tokenDetail = workflowManager.getToken(runId, WorkflowToken.Scope.USER, action2RowKey + action2ColumnKey);
    validateToken(tokenDetail, action2RowKey + action2ColumnKey, action2Value);

    tokenDetail = workflowManager.getToken(runId, WorkflowToken.Scope.USER, action3RowKey + action3ColumnKey);
    validateToken(tokenDetail, action3RowKey + action3ColumnKey, action3Value);
  }

  private void validateToken(WorkflowTokenDetail tokenDetail, String key, String value) {
    List<WorkflowTokenDetail.NodeValueDetail> nodeValueDetails = tokenDetail.getTokenData().get(key);
    // Only one node should have corresponding key
    Assert.assertEquals(1, nodeValueDetails.size());
    Assert.assertEquals(value, nodeValueDetails.get(0).getValue());
  }

  @Test
  public void testPipelineWithActionsMR() throws Exception {
    testPipelineWithActions(Engine.MAPREDUCE);
  }

  @Test
  public void testPipelineWithActionsSpark() throws Exception {
    testPipelineWithActions(Engine.SPARK);
  }

  private void testPipelineWithActions(Engine engine) throws Exception {
    String actionTable = "actionTable-" + engine;
    String action1RowKey = "action1.row";
    String action1ColumnKey = "action1.column";
    String action1Value = "action1.value";
    String action2RowKey = "action2.row";
    String action2ColumnKey = "action2.column";
    String action2Value = "action2.value";
    String action3RowKey = "action3.row";
    String action3ColumnKey = "action3.column";
    String action3Value = "action3.value";

    String sourceName = "actionSource-" + engine;
    String sinkName = "actionSink-" + engine;
    String sourceTableName = "actionSourceTable-" + engine;
    String sinkTableName = "actionSinkTable-" + engine;
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("action1", MockAction.getPlugin(actionTable, action1RowKey, action1ColumnKey,
                                                             action1Value)))
      .addStage(new ETLStage("action2", MockAction.getPlugin(actionTable, action2RowKey, action2ColumnKey,
                                                             action2Value)))
      .addStage(new ETLStage("action3", MockAction.getPlugin(actionTable, action3RowKey, action3ColumnKey,
                                                             action3Value)))
      .addStage(new ETLStage(sourceName, MockSource.getPlugin(sourceTableName, schema)))
      .addStage(new ETLStage(sinkName, MockSink.getPlugin(sinkTableName)))
      .addConnection(sourceName, sinkName)
      .addConnection("action1", "action2")
      .addConnection("action2", sourceName)
      .addConnection(sinkName, "action3")
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MyApp-" + engine);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);


    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();

    // write records to source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(sourceTableName));
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // check sink
    DataSetManager<Table> sinkManager = getDataset(sinkTableName);
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    DataSetManager<Table> actionTableDS = getDataset(actionTable);
    Assert.assertEquals(action1Value, MockAction.readOutput(actionTableDS, action1RowKey, action1ColumnKey));
    Assert.assertEquals(action2Value, MockAction.readOutput(actionTableDS, action2RowKey, action2ColumnKey));
    Assert.assertEquals(action3Value, MockAction.readOutput(actionTableDS, action3RowKey, action3ColumnKey));

    validateMetric(2, appId, sourceName + ".records.out");
    validateMetric(2, appId, sinkName + ".records.in");
  }

  @Test
  public void testSinglePhase() throws Exception {
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );
    /*
     * source --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("singleInput", schema)))
      .addStage(new ETLStage("sink", MockSink.getPlugin("singleOutput")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();

    // write records to source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset("singleInput"));
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // check sink
    DataSetManager<Table> sinkManager = getDataset("singleOutput");
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    validateMetric(2, appId, "source.records.out");
    validateMetric(2, appId, "sink.records.in");
  }

  @Test
  public void testMapRedSimpleMultipleSource() throws Exception {
    testSimpleMultiSource(Engine.MAPREDUCE);
  }

  @Test
  public void testSparkSimpleMultipleSource() throws Exception {
    testSimpleMultiSource(Engine.SPARK);
  }

  private void testSimpleMultiSource(Engine engine) throws Exception {
    /*
     * source1 --|
     *           |--> sink
     * source2 --|
     */
    String source1Name = String.format("simpleMSInput1-%s", engine);
    String source2Name = String.format("simpleMSInput2-%s", engine);
    String sinkName = String.format("simpleMSOutput-%s", engine);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source1", MockSource.getPlugin(source1Name)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(source2Name)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addConnection("source1", "sink")
      .addConnection("source2", "sink")
      .setEngine(engine)
      .build();


    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SimpleMultiSourceApp-" + engine);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // there should be only two programs - one workflow and one mapreduce/spark
    Assert.assertEquals(2, appManager.getInfo().getPrograms().size());
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );
    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(source1Name));
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel));
    inputManager = getDataset(NamespaceId.DEFAULT.dataset(source2Name));
    MockSource.writeInput(inputManager, ImmutableList.of(recordBob));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // check sink
    DataSetManager<Table> sinkManager = getDataset(sinkName);
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    validateMetric(1, appId, "source1.records.out");
    validateMetric(1, appId, "source2.records.out");
    validateMetric(2, appId, "sink.records.in");
  }

  @Test
  public void testMapRedMultiSource() throws Exception {
    testMultiSource(Engine.MAPREDUCE);
  }

  @Test
  public void testSparkMultiSource() throws Exception {
    testMultiSource(Engine.SPARK);
  }

  private void testMultiSource(Engine engine) throws Exception {
    /*
     * source1 --|                 |--> sink1
     *           |--> transform1 --|
     * source2 --|                 |
     *                             |--> transform2 --> sink2
     *                                     ^
     *                                     |
     * source3 ----------------------------|
     */
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );
    String source1Name = String.format("msInput1-%s", engine);
    String source2Name = String.format("msInput2-%s", engine);
    String source3Name = String.format("msInput3-%s", engine);
    String sink1Name = String.format("msOutput1-%s", engine);
    String sink2Name = String.format("msOutput2-%s", engine);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source1", MockSource.getPlugin(source1Name, schema)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(source2Name, schema)))
      .addStage(new ETLStage("source3", MockSource.getPlugin(source3Name, schema)))
      .addStage(new ETLStage("transform1", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("transform2", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("sink1", MockSink.getPlugin(sink1Name)))
      .addStage(new ETLStage("sink2", MockSink.getPlugin(sink2Name)))
      .addConnection("source1", "transform1")
      .addConnection("source2", "transform1")
      .addConnection("transform1", "sink1")
      .addConnection("transform1", "transform2")
      .addConnection("transform2", "sink2")
      .addConnection("source3", "transform2")
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MultiSourceApp-" + engine);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // there should be only two programs - one workflow and one mapreduce/spark
    Assert.assertEquals(2, appManager.getInfo().getPrograms().size());

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(schema).set("name", "jane").build();

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(source1Name));
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel));
    inputManager = getDataset(NamespaceId.DEFAULT.dataset(source2Name));
    MockSource.writeInput(inputManager, ImmutableList.of(recordBob));
    inputManager = getDataset(NamespaceId.DEFAULT.dataset(source3Name));
    MockSource.writeInput(inputManager, ImmutableList.of(recordJane));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // sink1 should get records from source1 and source2
    DataSetManager<Table> sinkManager = getDataset(sink1Name);
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    // sink2 should get all records
    sinkManager = getDataset(sink2Name);
    expected = ImmutableSet.of(recordSamuel, recordBob, recordJane);
    actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    validateMetric(1, appId, "source1.records.out");
    validateMetric(1, appId, "source2.records.out");
    validateMetric(1, appId, "source3.records.out");
    validateMetric(2, appId, "transform1.records.in");
    validateMetric(2, appId, "transform1.records.out");
    validateMetric(3, appId, "transform2.records.in");
    validateMetric(3, appId, "transform2.records.out");
    validateMetric(2, appId, "sink1.records.in");
    validateMetric(3, appId, "sink2.records.in");
  }

  @Test
  public void testMapRedSequentialAggregators() throws Exception {
    testSequentialAggregators(Engine.MAPREDUCE);
  }

  @Test
  public void testSparkSequentialAggregators() throws Exception {
    testSequentialAggregators(Engine.SPARK);
  }

  @Test
  public void testMapRedParallelAggregators() throws Exception {
    testParallelAggregators(Engine.MAPREDUCE);
  }

  @Test
  public void testSparkParallelAggregators() throws Exception {
    testParallelAggregators(Engine.SPARK);
  }

  private void testSequentialAggregators(Engine engine) throws Exception {
    String sourceName = "linearAggInput-" + engine.name();
    String sinkName = "linearAggOutput-" + engine.name();
    /*
     * source --> filter1 --> aggregator1 --> aggregator2 --> filter2 --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setEngine(engine)
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addStage(new ETLStage("filter1", StringValueFilterTransform.getPlugin("name", "bob")))
      .addStage(new ETLStage("filter2", StringValueFilterTransform.getPlugin("name", "jane")))
      .addStage(new ETLStage("aggregator1", IdentityAggregator.getPlugin()))
      .addStage(new ETLStage("aggregator2", IdentityAggregator.getPlugin()))
      .addConnection("source", "filter1")
      .addConnection("filter1", "aggregator1")
      .addConnection("aggregator1", "aggregator2")
      .addConnection("aggregator2", "filter2")
      .addConnection("filter2", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("LinearAggApp-" + engine);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(schema).set("name", "jane").build();

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(sourceName));
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob, recordJane));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // check output
    DataSetManager<Table> sinkManager = getDataset(sinkName);
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    validateMetric(3, appId, "source.records.out");
    validateMetric(3, appId, "filter1.records.in");
    validateMetric(2, appId, "filter1.records.out");
    validateMetric(2, appId, "aggregator1.records.in");
    validateMetric(2, appId, "aggregator1.records.out");
    validateMetric(2, appId, "aggregator2.records.in");
    validateMetric(2, appId, "aggregator2.records.out");
    validateMetric(2, appId, "filter2.records.in");
    validateMetric(1, appId, "filter2.records.out");
    validateMetric(1, appId, "sink.records.out");
  }

  private void testParallelAggregators(Engine engine) throws Exception {
    String source1Name = "pAggInput1-" + engine.name();
    String source2Name = "pAggInput2-" + engine.name();
    String sink1Name = "pAggOutput1-" + engine.name();
    String sink2Name = "pAggOutput2-" + engine.name();
    Schema inputSchema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.LONG))
    );
    /*
       source1 --|--> agg1 --> sink1
                 |
       source2 --|--> agg2 --> sink2
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setEngine(engine)
      .addStage(new ETLStage("source1", MockSource.getPlugin(source1Name, inputSchema)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(source2Name, inputSchema)))
      .addStage(new ETLStage("sink1", MockSink.getPlugin(sink1Name)))
      .addStage(new ETLStage("sink2", MockSink.getPlugin(sink2Name)))
      .addStage(new ETLStage("agg1", FieldCountAggregator.getPlugin("user", "string")))
      .addStage(new ETLStage("agg2", FieldCountAggregator.getPlugin("item", "long")))
      .addConnection("source1", "agg1")
      .addConnection("source1", "agg2")
      .addConnection("source2", "agg1")
      .addConnection("source2", "agg2")
      .addConnection("agg1", "sink1")
      .addConnection("agg2", "sink2")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ParallelAggApp-" + engine);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // write few records to each source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(source1Name));
    MockSource.writeInput(inputManager, ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("user", "samuel").set("item", 1L).build(),
      StructuredRecord.builder(inputSchema).set("user", "samuel").set("item", 2L).build()));

    inputManager = getDataset(NamespaceId.DEFAULT.dataset(source2Name));
    MockSource.writeInput(inputManager, ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("user", "samuel").set("item", 3L).build(),
      StructuredRecord.builder(inputSchema).set("user", "john").set("item", 4L).build(),
      StructuredRecord.builder(inputSchema).set("user", "john").set("item", 3L).build()));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    Schema outputSchema1 = Schema.recordOf(
      "user.count",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("ct", Schema.of(Schema.Type.LONG))
    );
    Schema outputSchema2 = Schema.recordOf(
      "item.count",
      Schema.Field.of("item", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("ct", Schema.of(Schema.Type.LONG))
    );

    // check output
    DataSetManager<Table> sinkManager = getDataset(sink1Name);
    Set<StructuredRecord> expected = ImmutableSet.of(
      StructuredRecord.builder(outputSchema1).set("user", "all").set("ct", 5L).build(),
      StructuredRecord.builder(outputSchema1).set("user", "samuel").set("ct", 3L).build(),
      StructuredRecord.builder(outputSchema1).set("user", "john").set("ct", 2L).build());
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    sinkManager = getDataset(sink2Name);
    expected = ImmutableSet.of(
      StructuredRecord.builder(outputSchema2).set("item", 0L).set("ct", 5L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 1L).set("ct", 1L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 2L).set("ct", 1L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 3L).set("ct", 2L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 4L).set("ct", 1L).build());
    actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    validateMetric(2, appId, "source1.records.out");
    validateMetric(3, appId, "source2.records.out");
    validateMetric(5, appId, "agg1.records.in");
    // 2 users, but FieldCountAggregator always emits an 'all' group
    validateMetric(3, appId, "agg1.aggregator.groups");
    validateMetric(3, appId, "agg1.records.out");
    validateMetric(5, appId, "agg2.records.in");
    // 4 items, but FieldCountAggregator always emits an 'all' group
    validateMetric(5, appId, "agg2.aggregator.groups");
    validateMetric(5, appId, "agg2.records.out");
    validateMetric(3, appId, "sink1.records.in");
    validateMetric(5, appId, "sink2.records.in");
  }

  @Test
  public void testSparkSinkAndCompute() throws Exception {
    // use the SparkSink to train a model
    testSinglePhaseWithSparkSink();
    // use a SparkCompute to classify all records going through the pipeline, using the model build with the SparkSink
    testSinglePhaseWithSparkCompute();
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

    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset("actionInput"));
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob, recordJane));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> tokenTableManager = getDataset(NamespaceId.DEFAULT.dataset("tokenTable"));
    Table tokenTable = tokenTableManager.get();
    NodeStatus status = NodeStatus.valueOf(Bytes.toString(
      tokenTable.get(Bytes.toBytes("phase-1"), Bytes.toBytes("status"))));
    Assert.assertEquals(NodeStatus.COMPLETED, status);
  }

  private void testSinglePhaseWithSparkSink() throws Exception {
    /*
     * source1 ---|
     *            |--> sparksink
     * source2 ---|
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source1", MockSource.getPlugin("messages1", SpamMessage.SCHEMA)))
      .addStage(new ETLStage("source2", MockSource.getPlugin("messages2", SpamMessage.SCHEMA)))
      .addStage(new ETLStage("customsink",
                             new ETLPlugin(NaiveBayesTrainer.PLUGIN_NAME, SparkSink.PLUGIN_TYPE,
                                           ImmutableMap.of("fileSetName", "modelFileSet",
                                                           "path", "output",
                                                           "fieldToClassify", SpamMessage.TEXT_FIELD,
                                                           "predictionField", SpamMessage.SPAM_PREDICTION_FIELD),
                                           null)))
      .addConnection("source1", "customsink")
      .addConnection("source2", "customsink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SparkSinkApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);


    // set up five spam messages and five non-spam messages to be used for classification
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
    messagesToWrite.add(new SpamMessage("buy our clothes", 1.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("sell your used books to us", 1.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("earn money for free", 1.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("this is definitely not spam", 1.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("you won the lottery", 1.0).toStructuredRecord());

    // write records to source1
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset("messages1"));
    MockSource.writeInput(inputManager, messagesToWrite);

    messagesToWrite.clear();
    messagesToWrite.add(new SpamMessage("how was your day", 0.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("what are you up to", 0.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("this is a genuine message", 0.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("this is an even more genuine message", 0.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("could you send me the report", 0.0).toStructuredRecord());

    // write records to source2
    inputManager = getDataset(NamespaceId.DEFAULT.dataset("messages2"));
    MockSource.writeInput(inputManager, messagesToWrite);

    // ingest in some messages to be classified
    StreamManager textsToClassify = getStreamManager(NaiveBayesTrainer.TEXTS_TO_CLASSIFY);
    textsToClassify.send("how are you doing today");
    textsToClassify.send("free money money");
    textsToClassify.send("what are you doing today");
    textsToClassify.send("genuine report");

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> classifiedTexts = getDataset(NaiveBayesTrainer.CLASSIFIED_TEXTS);

    Assert.assertEquals(0.0d, Bytes.toDouble(classifiedTexts.get().read("how are you doing today")), 0.01d);
    // only 'free money money' should be predicated as spam
    Assert.assertEquals(1.0d, Bytes.toDouble(classifiedTexts.get().read("free money money")), 0.01d);
    Assert.assertEquals(0.0d, Bytes.toDouble(classifiedTexts.get().read("what are you doing today")), 0.01d);
    Assert.assertEquals(0.0d, Bytes.toDouble(classifiedTexts.get().read("genuine report")), 0.01d);

    validateMetric(5, appId, "source1.records.out");
    validateMetric(5, appId, "source2.records.out");
    validateMetric(10, appId, "customsink.records.in");
  }

  private void testSinglePhaseWithSparkCompute() throws Exception {
    /*
     * source --> sparkcompute --> sink
     */
    String classifiedTextsTable = "classifiedTextTable";

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(NaiveBayesTrainer.TEXTS_TO_CLASSIFY, SpamMessage.SCHEMA)))
      .addStage(new ETLStage("sparkcompute",
                             new ETLPlugin(NaiveBayesClassifier.PLUGIN_NAME, SparkCompute.PLUGIN_TYPE,
                                           ImmutableMap.of("fileSetName", "modelFileSet",
                                                           "path", "output",
                                                           "fieldToClassify", SpamMessage.TEXT_FIELD,
                                                           "fieldToSet", SpamMessage.SPAM_PREDICTION_FIELD),
                                           null)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(classifiedTextsTable)))
      .addConnection("source", "sparkcompute")
      .addConnection("sparkcompute", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SparkComputeApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);


    // write some some messages to be classified
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
    messagesToWrite.add(new SpamMessage("how are you doing today").toStructuredRecord());
    messagesToWrite.add(new SpamMessage("free money money").toStructuredRecord());
    messagesToWrite.add(new SpamMessage("what are you doing today").toStructuredRecord());
    messagesToWrite.add(new SpamMessage("genuine report").toStructuredRecord());

    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(NaiveBayesTrainer.TEXTS_TO_CLASSIFY));
    MockSource.writeInput(inputManager, messagesToWrite);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> classifiedTexts = getDataset(classifiedTextsTable);
    List<StructuredRecord> structuredRecords = MockSink.readOutput(classifiedTexts);


    Set<SpamMessage> results = new HashSet<>();
    for (StructuredRecord structuredRecord : structuredRecords) {
      results.add(SpamMessage.fromStructuredRecord(structuredRecord));
    }

    Set<SpamMessage> expected = new HashSet<>();
    expected.add(new SpamMessage("how are you doing today", 0.0));
    // only 'free money money' should be predicated as spam
    expected.add(new SpamMessage("free money money", 1.0));
    expected.add(new SpamMessage("what are you doing today", 0.0));
    expected.add(new SpamMessage("genuine report", 0.0));

    Assert.assertEquals(expected, results);

    validateMetric(4, appId, "source.records.out");
    validateMetric(4, appId, "sparkcompute.records.in");
    validateMetric(4, appId, "sink.records.in");
  }

  @Test
  public void testInnerJoinMR() throws Exception {
    testInnerJoinWithMultiOutput(Engine.MAPREDUCE);
  }

  @Test
  public void testInnerJoinSpark() throws Exception {
    testInnerJoinWithMultiOutput(Engine.SPARK);
  }

  public void testInnerJoinWithMultiOutput(Engine engine) throws Exception {
    Schema inputSchema1 = Schema.recordOf(
      "customerRecord",
      Schema.Field.of("customer_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("customer_name", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema2 = Schema.recordOf(
      "itemRecord",
      Schema.Field.of("item_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item_price", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("cust_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("cust_name", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema3 = Schema.recordOf(
      "transactionRecord",
      Schema.Field.of("t_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("c_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("c_name", Schema.of(Schema.Type.STRING))
    );

    String input1Name = "source1InnerJoinInput-" + engine;
    String input2Name = "source2InnerJoinInput-" + engine;
    String input3Name = "source3InnerJoinInput-" + engine;
    String outputName = "innerJoinOutput-" + engine;
    String outputName2 = "innerJoinOutput2-" + engine;
    String joinerName = "innerJoiner-" + engine;
    String sinkName = "innerJoinSink-" + engine;
    String sinkName2 = "innerJoinSink-2" + engine;
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source1", MockSource.getPlugin(input1Name, inputSchema1)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(input2Name, inputSchema2)))
      .addStage(new ETLStage("source3", MockSource.getPlugin(input3Name, inputSchema3)))
      .addStage(new ETLStage("t1", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t2", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t3", IdentityTransform.getPlugin()))
      .addStage(new ETLStage(joinerName, MockJoiner.getPlugin("t1.customer_id=t2.cust_id=t3.c_id&" +
                                                                  "t1.customer_name=t2.cust_name=t3.c_name",
                                                                "t1,t2,t3", "")))
      .addStage(new ETLStage(sinkName, MockSink.getPlugin(outputName)))
      .addStage(new ETLStage(sinkName2, MockSink.getPlugin(outputName2)))
      .addConnection("source1", "t1")
      .addConnection("source2", "t2")
      .addConnection("source3", "t3")
      .addConnection("t1", joinerName)
      .addConnection("t2", joinerName)
      .addConnection("t3", joinerName)
      .addConnection(joinerName, sinkName)
      .addConnection(joinerName, sinkName2)
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("InnerJoinApp-" + engine);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    Schema outSchema = Schema.recordOf(
      "join.output",
      Schema.Field.of("customer_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("customer_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item_price", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("cust_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("cust_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("t_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("c_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("c_name", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord recordSamuel = StructuredRecord.builder(inputSchema1).set("customer_id", "1")
      .set("customer_name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(inputSchema1).set("customer_id", "2")
      .set("customer_name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(inputSchema1).set("customer_id", "3")
      .set("customer_name", "jane").build();

    StructuredRecord recordCar = StructuredRecord.builder(inputSchema2).set("item_id", "11").set("item_price", 10000L)
      .set("cust_id", "1").set("cust_name", "samuel").build();
    StructuredRecord recordBike = StructuredRecord.builder(inputSchema2).set("item_id", "22").set("item_price", 100L)
      .set("cust_id", "3").set("cust_name", "jane").build();

    StructuredRecord recordTrasCar = StructuredRecord.builder(inputSchema3).set("t_id", "1").set("c_id", "1")
      .set("c_name", "samuel").build();
    StructuredRecord recordTrasBike = StructuredRecord.builder(inputSchema3).set("t_id", "2").set("c_id", "3")
      .set("c_name", "jane").build();

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(input1Name));
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob, recordJane));
    inputManager = getDataset(NamespaceId.DEFAULT.dataset(input2Name));
    MockSource.writeInput(inputManager, ImmutableList.of(recordCar, recordBike));
    inputManager = getDataset(NamespaceId.DEFAULT.dataset(input3Name));
    MockSource.writeInput(inputManager, ImmutableList.of(recordTrasCar, recordTrasBike));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    StructuredRecord joinRecordSamuel = StructuredRecord.builder(outSchema)
      .set("customer_id", "1").set("customer_name", "samuel")
      .set("item_id", "11").set("item_price", 10000L).set("cust_id", "1").set("cust_name", "samuel")
      .set("t_id", "1").set("c_id", "1").set("c_name", "samuel").build();

    StructuredRecord joinRecordJane = StructuredRecord.builder(outSchema)
      .set("customer_id", "3").set("customer_name", "jane")
      .set("item_id", "22").set("item_price", 100L).set("cust_id", "3").set("cust_name", "jane")
      .set("t_id", "2").set("c_id", "3").set("c_name", "jane").build();

    DataSetManager<Table> sinkManager = getDataset(outputName);
    Set<StructuredRecord> expected = ImmutableSet.of(joinRecordSamuel, joinRecordJane);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    sinkManager = getDataset(outputName2);
    actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    validateMetric(2, appId, joinerName + ".records.out");
    validateMetric(2, appId, sinkName + ".records.in");
    validateMetric(2, appId, sinkName2 + ".records.in");
  }

  @Test
  public void testOuterJoinMR() throws Exception {
    testOuterJoin(Engine.MAPREDUCE);
  }

  @Test
  public void testOuterJoinSpark() throws Exception {
    testOuterJoin(Engine.SPARK);
  }

  public void testOuterJoin(Engine engine) throws Exception {
    Schema inputSchema1 = Schema.recordOf(
      "customerRecord",
      Schema.Field.of("customer_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("customer_name", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema2 = Schema.recordOf(
      "itemRecord",
      Schema.Field.of("item_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item_price", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("cust_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("cust_name", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema3 = Schema.recordOf(
      "transactionRecord",
      Schema.Field.of("t_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("c_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("c_name", Schema.of(Schema.Type.STRING))
    );

    String input1Name = "source1OuterJoinInput-" + engine;
    String input2Name = "source2OuterJoinInput-" + engine;
    String input3Name = "source3OuterJoinInput-" + engine;
    String outputName = "outerJoinOutput-" + engine;
    String joinerName = "outerJoiner-" + engine;
    String sinkName = "outerJoinSink-" + engine;
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source1", MockSource.getPlugin(input1Name, inputSchema1)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(input2Name, inputSchema2)))
      .addStage(new ETLStage("source3", MockSource.getPlugin(input3Name, inputSchema3)))
      .addStage(new ETLStage("t1", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t2", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t3", IdentityTransform.getPlugin()))
      .addStage(new ETLStage(joinerName, MockJoiner.getPlugin("t1.customer_id=t2.cust_id=t3.c_id&" +
                                                                  "t1.customer_name=t2.cust_name=t3.c_name", "t1", "")))
      .addStage(new ETLStage(sinkName, MockSink.getPlugin(outputName)))
      .addConnection("source1", "t1")
      .addConnection("source2", "t2")
      .addConnection("source3", "t3")
      .addConnection("t1", joinerName)
      .addConnection("t2", joinerName)
      .addConnection("t3", joinerName)
      .addConnection(joinerName, sinkName)
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("OuterJoinApp-" + engine);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    Schema outSchema = Schema.recordOf(
      "join.output",
      Schema.Field.of("customer_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("customer_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("item_price", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("cust_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("cust_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("t_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("c_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("c_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    StructuredRecord recordSamuel = StructuredRecord.builder(inputSchema1).set("customer_id", "1")
      .set("customer_name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(inputSchema1).set("customer_id", "2")
      .set("customer_name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(inputSchema1).set("customer_id", "3")
      .set("customer_name", "jane").build();
    StructuredRecord recordMartha = StructuredRecord.builder(inputSchema1).set("customer_id", "4")
      .set("customer_name", "martha").build();

    StructuredRecord recordCar = StructuredRecord.builder(inputSchema2).set("item_id", "11").set("item_price", 10000L)
      .set("cust_id", "1").set("cust_name", "samuel").build();
    StructuredRecord recordBike = StructuredRecord.builder(inputSchema2).set("item_id", "22").set("item_price", 100L)
      .set("cust_id", "3").set("cust_name", "jane").build();

    StructuredRecord recordTrasCar = StructuredRecord.builder(inputSchema3).set("t_id", "1").set("c_id", "1")
      .set("c_name", "samuel").build();
    StructuredRecord recordTrasPlane = StructuredRecord.builder(inputSchema3).set("t_id", "2").set("c_id", "2")
      .set("c_name", "bob").build();
    StructuredRecord recordTrasBike = StructuredRecord.builder(inputSchema3).set("t_id", "3").set("c_id", "3")
      .set("c_name", "jane").build();

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(input1Name));
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob, recordJane, recordMartha));
    inputManager = getDataset(NamespaceId.DEFAULT.dataset(input2Name));
    MockSource.writeInput(inputManager, ImmutableList.of(recordCar, recordBike));
    inputManager = getDataset(NamespaceId.DEFAULT.dataset(input3Name));
    MockSource.writeInput(inputManager, ImmutableList.of(recordTrasCar, recordTrasPlane, recordTrasBike));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    StructuredRecord joinRecordSamuel = StructuredRecord.builder(outSchema)
      .set("customer_id", "1").set("customer_name", "samuel")
      .set("item_id", "11").set("item_price", 10000L).set("cust_id", "1").set("cust_name", "samuel")
      .set("t_id", "1").set("c_id", "1").set("c_name", "samuel").build();

    StructuredRecord joinRecordBob = StructuredRecord.builder(outSchema)
      .set("customer_id", "2").set("customer_name", "bob").set("t_id", "2")
      .set("c_id", "2").set("c_name", "bob").build();

    StructuredRecord joinRecordJane = StructuredRecord.builder(outSchema)
      .set("customer_id", "3").set("customer_name", "jane")
      .set("item_id", "22").set("item_price", 100L).set("cust_id", "3").set("cust_name", "jane")
      .set("t_id", "3").set("c_id", "3").set("c_name", "jane").build();

    StructuredRecord joinRecordMartha = StructuredRecord.builder(outSchema)
      .set("customer_id", "4").set("customer_name", "martha").build();

    DataSetManager<Table> sinkManager = getDataset(outputName);
    Set<StructuredRecord> expected = ImmutableSet.of(joinRecordSamuel, joinRecordJane, joinRecordBob, joinRecordMartha);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    validateMetric(4, appId, joinerName + ".records.out");
    validateMetric(4, appId, sinkName + ".records.in");
  }

  @Test
  public void testMultiPhaseJoinerMR() throws Exception {
    testMultipleJoiner(Engine.MAPREDUCE);
  }

  @Test
  public void testMultipleJoinerSpark() throws Exception {
    testMultipleJoiner(Engine.SPARK);
  }

  private void testMultipleJoiner(Engine engine) throws Exception {
    /*
     * source1 ----> t1 ------
     *                        | --> innerjoin ----> t4 ------
     * source2 ----> t2 ------                                 |
     *                                                         | ---> outerjoin --> sink1
     *                                                         |
     * source3 -------------------- t3 ------------------------
     */

    Schema inputSchema1 = Schema.recordOf(
      "customerRecord",
      Schema.Field.of("customer_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("customer_name", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema2 = Schema.recordOf(
      "itemRecord",
      Schema.Field.of("item_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item_price", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("cust_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("cust_name", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema3 = Schema.recordOf(
      "transactionRecord",
      Schema.Field.of("t_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("c_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("i_id", Schema.of(Schema.Type.STRING))
    );

    Schema outSchema1 = Schema.recordOf(
      "join.output",
      Schema.Field.of("customer_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("customer_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item_price", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("cust_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("cust_name", Schema.of(Schema.Type.STRING))
    );

    Schema outSchema2 = Schema.recordOf(
      "join.output",
      Schema.Field.of("t_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("c_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("i_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("customer_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("customer_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("item_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("item_price", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("cust_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("cust_name", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    String source1MulitJoinInput = "multiJoinSource1-" + engine;
    String source2MultiJoinInput = "multiJoinSource2-" + engine;
    String source3MultiJoinInput = "multiJoinSource3-" + engine;
    String outputName = "multiJoinOutput-" + engine;
    String sinkName = "multiJoinOutputSink-" + engine;
    String outerJoinName = "multiJoinOuter-" + engine;
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source1", MockSource.getPlugin(source1MulitJoinInput, inputSchema1)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(source2MultiJoinInput, inputSchema2)))
      .addStage(new ETLStage("source3", MockSource.getPlugin(source3MultiJoinInput, inputSchema3)))
      .addStage(new ETLStage("t1", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t2", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t3", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t4", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("innerjoin", MockJoiner.getPlugin("t1.customer_id=t2.cust_id",
                                                               "t1,t2", "")))
      .addStage(new ETLStage(outerJoinName, MockJoiner.getPlugin("t4.item_id=t3.i_id",
                                                               "", "")))
      .addStage(new ETLStage(sinkName, MockSink.getPlugin(outputName)))
      .addConnection("source1", "t1")
      .addConnection("source2", "t2")
      .addConnection("source3", "t3")
      .addConnection("t1", "innerjoin")
      .addConnection("t2", "innerjoin")
      .addConnection("innerjoin", "t4")
      .addConnection("t3", outerJoinName)
      .addConnection("t4", outerJoinName)
      .addConnection(outerJoinName, sinkName)
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("JoinerApp-" + engine);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    StructuredRecord recordSamuel = StructuredRecord.builder(inputSchema1).set("customer_id", "1")
      .set("customer_name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(inputSchema1).set("customer_id", "2")
      .set("customer_name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(inputSchema1).set("customer_id", "3")
      .set("customer_name", "jane").build();

    StructuredRecord recordCar = StructuredRecord.builder(inputSchema2).set("item_id", "11").set("item_price", 10000L)
      .set("cust_id", "1").set("cust_name", "samuel").build();
    StructuredRecord recordBike = StructuredRecord.builder(inputSchema2).set("item_id", "22").set("item_price", 100L)
      .set("cust_id", "3").set("cust_name", "jane").build();

    StructuredRecord recordTrasCar = StructuredRecord.builder(inputSchema3).set("t_id", "1").set("c_id", "1")
      .set("i_id", "11").build();
    StructuredRecord recordTrasBike = StructuredRecord.builder(inputSchema3).set("t_id", "2").set("c_id", "3")
      .set("i_id", "22").build();
    StructuredRecord recordTrasPlane = StructuredRecord.builder(inputSchema3).set("t_id", "3").set("c_id", "4")
      .set("i_id", "33").build();

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(source1MulitJoinInput));
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob, recordJane));
    inputManager = getDataset(NamespaceId.DEFAULT.dataset(source2MultiJoinInput));
    MockSource.writeInput(inputManager, ImmutableList.of(recordCar, recordBike));
    inputManager = getDataset(NamespaceId.DEFAULT.dataset(source3MultiJoinInput));
    MockSource.writeInput(inputManager, ImmutableList.of(recordTrasCar, recordTrasBike, recordTrasPlane));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    StructuredRecord joinRecordSamuel = StructuredRecord.builder(outSchema2)
      .set("customer_id", "1").set("customer_name", "samuel")
      .set("item_id", "11").set("item_price", 10000L).set("cust_id", "1").set("cust_name", "samuel")
      .set("t_id", "1").set("c_id", "1").set("i_id", "11").build();

    StructuredRecord joinRecordJane = StructuredRecord.builder(outSchema2)
      .set("customer_id", "3").set("customer_name", "jane")
      .set("item_id", "22").set("item_price", 100L).set("cust_id", "3").set("cust_name", "jane")
      .set("t_id", "2").set("c_id", "3").set("i_id", "22").build();

    StructuredRecord joinRecordPlane = StructuredRecord.builder(outSchema2)
      .set("t_id", "3").set("c_id", "4").set("i_id", "33").build();

    DataSetManager<Table> sinkManager = getDataset(outputName);
    Set<StructuredRecord> expected = ImmutableSet.of(joinRecordSamuel, joinRecordJane, joinRecordPlane);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    validateMetric(3, appId, outerJoinName + ".records.out");
    validateMetric(3, appId, sinkName + ".records.in");
  }

  @Test
  public void testSecureStoreMacroPipelines() throws Exception {
    testSecureStorePipeline(Engine.MAPREDUCE, "mr");
    testSecureStorePipeline(Engine.SPARK, "spark");
  }

  /**
   * Tests the secure storage macro function in a pipelines by creating datasets from the secure store data.
   */
  private void testSecureStorePipeline(Engine engine, String prefix) throws Exception {
    /*
     * Trivial pipeline from batch source to batch sink.
     *
     * source --------- sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockRuntimeDatasetSource.getPlugin("input", "${secure(" + prefix + "source)}")))
      .addStage(new ETLStage("sink", MockRuntimeDatasetSink.getPlugin("output", "${secure(" + prefix + "sink)}")))
      .addConnection("source", "sink")
      .setEngine(engine)
      .build();

    // place dataset names into secure storage
    getSecureStoreManager().putSecureData("default", prefix + "source", prefix + "MockSecureSourceDataset",
                                          "secure source dataset name", new HashMap<String, String>());
    getSecureStoreManager().putSecureData("default", prefix + "sink", prefix + "MockSecureSinkDataset",
                                          "secure dataset name", new HashMap<String, String>());

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("App-" + engine);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);

    // make sure the datasets don't exist beforehand
    Assert.assertNull(getDataset(prefix + "MockSecureSourceDataset").get());
    Assert.assertNull(getDataset(prefix + "MockSecureSinkDataset").get());

    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // now the datasets should exist
    Assert.assertNotNull(getDataset(prefix + "MockSecureSourceDataset").get());
    Assert.assertNotNull(getDataset(prefix + "MockSecureSinkDataset").get());
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
      Assert.assertNull(getDataset(NamespaceId.DEFAULT.dataset(expectedExternalDatasetInput)).get());
      Assert.assertNull(getDataset(NamespaceId.DEFAULT.dataset(expectedExternalDatasetOutput)).get());
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

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    List<RunRecord> history = workflowManager.getHistory();
    // there should be only one completed run
    Assert.assertEquals(1, history.size());
    Assert.assertEquals(ProgramRunStatus.COMPLETED, history.get(0).getStatus());

    // Assert output
    Assert.assertEquals(allInput, MockExternalSink.readOutput(outputSubDir1.getAbsolutePath()));
    Assert.assertEquals(allInput, MockExternalSink.readOutput(outputSubDir2.getAbsolutePath()));

    if (!backwardsCompatible) {
      // Assert that external datasets got created
      Assert.assertNotNull(getDataset(NamespaceId.DEFAULT.dataset(expectedExternalDatasetInput)).get());
      Assert.assertNotNull(getDataset(NamespaceId.DEFAULT.dataset(expectedExternalDatasetOutput)).get());
    }
  }

  @Test
  public void testMacrosMapReducePipeline() throws Exception {
    /*
     * Trivial MapReduce pipeline from batch source to batch sink.
     *
     * source --------- sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockRuntimeDatasetSource.getPlugin("mrinput", "${runtime${source}}")))
      .addStage(new ETLStage("sink", MockRuntimeDatasetSink.getPlugin("mroutput", "${runtime}${sink}")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MRApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // set runtime arguments for macro substitution
    Map<String, String> runtimeArguments = ImmutableMap.of("runtime", "mockRuntime",
                                                           "sink", "MRSinkDataset",
                                                           "source", "Source",
                                                           "runtimeSource",
                                                           "mockRuntimeMRSourceDataset");

    // make sure the datasets don't exist beforehand
    Assert.assertNull(getDataset("mockRuntimeMRSourceDataset").get());
    Assert.assertNull(getDataset("mockRuntimeMRSinkDataset").get());

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.setRuntimeArgs(runtimeArguments);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // now the datasets should exist
    Assert.assertNotNull(getDataset("mockRuntimeMRSourceDataset").get());
    Assert.assertNotNull(getDataset("mockRuntimeMRSinkDataset").get());
  }

  /**
   * Tests that if macros are provided
   */
  @Test
  public void testMacrosSparkPipeline() throws Exception {
    /*
     * Trivial Spark pipeline from batch source to batch sink.
     *
     * source --------- sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setEngine(Engine.SPARK)
      .addStage(new ETLStage("source", MockRuntimeDatasetSource.getPlugin("sparkinput", "${runtime${source}}")))
      .addStage(new ETLStage("sink", MockRuntimeDatasetSink.getPlugin("sparkoutput", "${runtime}${sink}")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SparkApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // set runtime arguments for macro substitution
    Map<String, String> runtimeArguments = ImmutableMap.of("runtime", "mockRuntime",
                                                           "sink", "SparkSinkDataset",
                                                           "source", "Source",
                                                           "runtimeSource",
                                                           "mockRuntimeSparkSourceDataset");

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.setRuntimeArgs(runtimeArguments);

    // make sure the datasets don't exist beforehand
    Assert.assertNull(getDataset("mockRuntimeSparkSourceDataset").get());
    Assert.assertNull(getDataset("mockRuntimeSparkSinkDataset").get());

    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // now the datasets should exist
    Assert.assertNotNull(getDataset("mockRuntimeSparkSourceDataset").get());
    Assert.assertNotNull(getDataset("mockRuntimeSparkSinkDataset").get());
  }


  /**
   * Tests that if no macro is provided to the dataset name property, datasets will be created at config time.
   */
  @Test
  public void testNoMacroMapReduce() throws Exception {
    /*
     * Trivial MapReduce pipeline from batch source to batch sink.
     *
     * source --------- sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockRuntimeDatasetSource.getPlugin("mrinput", "configTimeMockSourceDataset")))
      .addStage(new ETLStage("sink", MockRuntimeDatasetSink.getPlugin("mroutput", "configTimeMockSinkDataset")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MRApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // set runtime arguments for macro substitution
    Map<String, String> runtimeArguments = ImmutableMap.of("runtime", "mockRuntime",
                                                           "sink", "SinkDataset",
                                                           "source", "Source",
                                                           "runtimeSource", "mockRuntimeSourceDataset");

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);

    // make sure the datasets were created at configure time
    Assert.assertNotNull(getDataset("configTimeMockSourceDataset").get());
    Assert.assertNotNull(getDataset("configTimeMockSinkDataset").get());

    workflowManager.setRuntimeArgs(runtimeArguments);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
  }

  private void validateMetric(long expected, ApplicationId appId,
                              String metric) throws TimeoutException, InterruptedException {
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName(),
                                               Constants.Metrics.Tag.WORKFLOW, SmartWorkflow.NAME);
    getMetricsManager().waitForTotalMetricCount(tags, "user." + metric, expected, 20, TimeUnit.SECONDS);
    // wait for won't throw an exception if the metric count is greater than expected
    Assert.assertEquals(expected, getMetricsManager().getTotalMetric(tags, "user." + metric));
  }
}
