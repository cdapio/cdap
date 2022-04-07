/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetArguments;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.InputField;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.lineage.field.TransformOperation;
import io.cdap.cdap.api.lineage.field.WriteOperation;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.workflow.NodeStatus;
import io.cdap.cdap.api.workflow.ScheduleProgramInfo;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.metadata.lineage.Lineage;
import io.cdap.cdap.data2.metadata.lineage.Relation;
import io.cdap.cdap.data2.metadata.writer.MetadataOperation;
import io.cdap.cdap.datapipeline.mock.NaiveBayesClassifier;
import io.cdap.cdap.datapipeline.mock.NaiveBayesTrainer;
import io.cdap.cdap.datapipeline.mock.SpamMessage;
import io.cdap.cdap.datapipeline.plugin.PluggableFilterTransform;
import io.cdap.cdap.datapipeline.plugin.ValueFilter;
import io.cdap.cdap.datapipeline.service.ServiceApp;
import io.cdap.cdap.datapipeline.spark.LineFilterProgram;
import io.cdap.cdap.datapipeline.spark.WordCount;
import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.mock.action.FieldLineageAction;
import io.cdap.cdap.etl.mock.action.MockAction;
import io.cdap.cdap.etl.mock.alert.NullAlertTransform;
import io.cdap.cdap.etl.mock.alert.TMSAlertPublisher;
import io.cdap.cdap.etl.mock.batch.FilterTransform;
import io.cdap.cdap.etl.mock.batch.IncapableSink;
import io.cdap.cdap.etl.mock.batch.IncapableSource;
import io.cdap.cdap.etl.mock.batch.LookupTransform;
import io.cdap.cdap.etl.mock.batch.MockExternalSink;
import io.cdap.cdap.etl.mock.batch.MockExternalSource;
import io.cdap.cdap.etl.mock.batch.MockRuntimeDatasetSink;
import io.cdap.cdap.etl.mock.batch.MockRuntimeDatasetSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.batch.NodeStatesAction;
import io.cdap.cdap.etl.mock.batch.aggregator.FieldCountAggregator;
import io.cdap.cdap.etl.mock.batch.aggregator.GroupFilterAggregator;
import io.cdap.cdap.etl.mock.batch.aggregator.IdentityAggregator;
import io.cdap.cdap.etl.mock.batch.joiner.MockJoiner;
import io.cdap.cdap.etl.mock.condition.MockCondition;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.mock.transform.DropNullTransform;
import io.cdap.cdap.etl.mock.transform.FilterErrorTransform;
import io.cdap.cdap.etl.mock.transform.FlattenErrorTransform;
import io.cdap.cdap.etl.mock.transform.IdentityTransform;
import io.cdap.cdap.etl.mock.transform.IntValueFilterTransform;
import io.cdap.cdap.etl.mock.transform.NullFieldSplitterTransform;
import io.cdap.cdap.etl.mock.transform.SleepTransform;
import io.cdap.cdap.etl.mock.transform.StringValueFilterTransform;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.v2.ArgumentMapping;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.proto.v2.PluginPropertyMapping;
import io.cdap.cdap.etl.proto.v2.TriggeringPropertyMapping;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import io.cdap.cdap.metadata.DatasetFieldLineageSummary;
import io.cdap.cdap.metadata.FieldLineageAdmin;
import io.cdap.cdap.metadata.FieldRelation;
import io.cdap.cdap.metadata.LineageAdmin;
import io.cdap.cdap.metadata.MetadataAdmin;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.WorkflowTokenDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.api.RunId;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 *
 */
public class DataPipelineTest extends HydratorTestBase {
  private static final Gson GSON = new Gson();
  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");
  private static final ArtifactId UPGRADE_APP_ARTIFACT_ID_1 = NamespaceId.DEFAULT.artifact("app", "1.1.0");
  private static final ArtifactId UPGRADE_APP_ARTIFACT_ID_2 = NamespaceId.DEFAULT.artifact("app", "1.2.0");
  private static final ArtifactId UPGRADE_APP_ARTIFACT_ID_3_SNAPSHOT =
    NamespaceId.DEFAULT.artifact("app", "1.3.0-SNAPSHOT");
  private static final ArtifactId OLD_APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "0.0.9");

  private static final ArtifactSummary APP_ARTIFACT_RANGE = new ArtifactSummary("app", "[0.1.0,1.1.0)");
  private static int startCount;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.Security.Store.PROVIDER, "file",
                                                                       Constants.AppFabric.SPARK_COMPAT,
                                                                       Compat.SPARK_COMPAT);
  private static final String WORDCOUNT_PLUGIN = "wordcount";
  private static final String FILTER_PLUGIN = "filterlines";
  private static final String SPARK_TYPE = io.cdap.cdap.etl.common.Constants.SPARK_PROGRAM_PLUGIN_TYPE;

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }
    setupBatchArtifacts(APP_ARTIFACT_ID, DataPipelineApp.class);

    // external spark programs must be explicitly specified
    Map<String, PluginPropertyField> emptyMap = ImmutableMap.of();
    Set<PluginClass> extraPlugins = ImmutableSet.of(
      PluginClass.builder().setName(WORDCOUNT_PLUGIN).setType(SPARK_TYPE)
        .setDescription("").setClassName(WordCount.class.getName()).setProperties(emptyMap).build());
    // add some test plugins
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), APP_ARTIFACT_ID, extraPlugins,
                      NaiveBayesTrainer.class, NaiveBayesClassifier.class, WordCount.class, LineFilterProgram.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("test-plugins", "1.0.0"), APP_ARTIFACT_ID,
                      PluggableFilterTransform.class);

    // Upgrade tests related setup.

    // Set up 3 extra version of artifacts for DataPipelineApp apart from with APP_ARTIFACT_ID which is created in test
    // setup.
    setupBatchArtifacts(OLD_APP_ARTIFACT_ID, DataPipelineApp.class);
    setupBatchArtifacts(UPGRADE_APP_ARTIFACT_ID_1, DataPipelineApp.class);
    setupBatchArtifacts(UPGRADE_APP_ARTIFACT_ID_2, DataPipelineApp.class);
    setupBatchArtifacts(UPGRADE_APP_ARTIFACT_ID_3_SNAPSHOT, DataPipelineApp.class);

    // Bind different version of filter plugin with different version of data pipeline app in SYSTEM namespace.
    // Will be used to make sure upgrade choose latest version of artifact and only plugin mapped to corresponding
    // artifact version.
    addPluginArtifact(NamespaceId.SYSTEM.artifact("test-plugins", "1.2.0"), UPGRADE_APP_ARTIFACT_ID_1,
                      PluggableFilterTransform.class);
    addPluginArtifact(NamespaceId.SYSTEM.artifact("test-plugins", "1.0.5"), UPGRADE_APP_ARTIFACT_ID_2,
                      PluggableFilterTransform.class);
    addPluginArtifact(NamespaceId.SYSTEM.artifact("test-plugins", "1.1.0"), UPGRADE_APP_ARTIFACT_ID_2,
                      PluggableFilterTransform.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("test-plugins", "1.0.8"), UPGRADE_APP_ARTIFACT_ID_2,
                      PluggableFilterTransform.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("test-plugins", "1.1.1-SNAPSHOT"),
                      UPGRADE_APP_ARTIFACT_ID_3_SNAPSHOT, PluggableFilterTransform.class);
  }

  @After
  public void cleanupTest() {
    getMetricsManager().resetAll();
  }

  @Test
  public void testActionFieldLineage() throws Exception {
    testActionFieldLineage(Engine.MAPREDUCE);
    testActionFieldLineage(Engine.SPARK);
  }

  private void testActionFieldLineage(Engine engine) throws Exception {
    String readDataset = "ActionReadDataset" + engine;
    String writeDataset = "ActionWriteDataset" + engine;
    List<String> srcFields = ImmutableList.of("srcField1", "srcField2", "srcField3");
    Set<String> destFields = ImmutableSet.of("destField1", "destField2", "destField3");

    List<Operation> operations = new ArrayList<>();
    /*
     *          |---------> srcField1 -> destField1----|
     *          |                                      |
     * ActionReadDataset -> srcField2 -> destField2 ---|-> ActionWriteDataset
     *          |                                      |
     *          |---------> srcField3 -> destField3 ---|
     */
    operations.add(new ReadOperation("Read", "1st operation", EndPoint.of("default", readDataset), srcFields));
    operations.add(new TransformOperation("Transform1", "2nd operation",
                                          Collections.singletonList(InputField.of("Read", "srcField1")),
                                          "destField1"));
    operations.add(new TransformOperation("Transform2", "3rd operation",
                                          Collections.singletonList(InputField.of("Read", "srcField2")),
                                          "destField2"));
    operations.add(new TransformOperation("Transform3", "4th operation",
                                          Collections.singletonList(InputField.of("Read", "srcField3")),
                                          "destField3"));
    operations.add(new WriteOperation("Write", "5th operation", EndPoint.of("default", writeDataset),
                                      ImmutableList.of(InputField.of("Transform1", "destField1"),
                                                       InputField.of("Transform2", "destField2"),
                                                       InputField.of("Transform3", "destField3"))));
    ETLStage action = new ETLStage("action", FieldLineageAction.getPlugin(readDataset, writeDataset, operations));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder().addStage(action).setEngine(engine).build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ActionFieldLineage-" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForGoodRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    FieldLineageAdmin fieldAdmin = getFieldLineageAdmin();

    // get field lineage for dest dataset
    DatasetFieldLineageSummary summary =
      fieldAdmin.getDatasetFieldLineage(Constants.FieldLineage.Direction.BOTH, EndPoint.of("default", writeDataset),
                                        0, System.currentTimeMillis());
    Assert.assertEquals(NamespaceId.DEFAULT.dataset(writeDataset), summary.getDatasetId());
    Assert.assertEquals(destFields, summary.getFields());
    Assert.assertTrue(summary.getOutgoing().isEmpty());
    Assert.assertEquals(1, summary.getIncoming().size());
    Set<FieldRelation> fieldRelations = ImmutableSet.of(new FieldRelation("srcField1", "destField1"),
                                                        new FieldRelation("srcField2", "destField2"),
                                                        new FieldRelation("srcField3", "destField3"));
    DatasetFieldLineageSummary.FieldLineageRelations expectedRelations =
      new DatasetFieldLineageSummary.FieldLineageRelations(NamespaceId.DEFAULT.dataset(readDataset), 3, fieldRelations);
    Assert.assertEquals(expectedRelations, summary.getIncoming().iterator().next());

    // get field lineage for src dataset
    summary =
      fieldAdmin.getDatasetFieldLineage(Constants.FieldLineage.Direction.BOTH, EndPoint.of("default", readDataset),
                                        0, System.currentTimeMillis());
    Assert.assertEquals(NamespaceId.DEFAULT.dataset(readDataset), summary.getDatasetId());
    Assert.assertEquals(new HashSet<>(srcFields), summary.getFields());
    Assert.assertTrue(summary.getIncoming().isEmpty());
    Assert.assertEquals(1, summary.getOutgoing().size());
    expectedRelations =
      new DatasetFieldLineageSummary.FieldLineageRelations(NamespaceId.DEFAULT.dataset(writeDataset), 3,
                                                           fieldRelations);
    Assert.assertEquals(expectedRelations, summary.getOutgoing().iterator().next());

    LineageAdmin lineageAdmin = getLineageAdmin();
    ProgramId programId = appId.workflow(SmartWorkflow.NAME);
    RunId runId = RunIds.fromString(workflowManager.getHistory().iterator().next().getPid());

    // get dataset lineage for src dataset
    Tasks.waitFor(2, () -> {
      Lineage lineage = lineageAdmin.computeLineage(NamespaceId.DEFAULT.dataset(readDataset),
                                                    0, System.currentTimeMillis(), 1, "workflow");
      return lineage.getRelations().size();
    }, 10, TimeUnit.SECONDS);

    Lineage lineage = lineageAdmin.computeLineage(NamespaceId.DEFAULT.dataset(readDataset),
                                                  0, System.currentTimeMillis(), 1, "workflow");
    Set<Relation> expectedLineage =
      ImmutableSet.of(new Relation(NamespaceId.DEFAULT.dataset(readDataset), programId, AccessType.READ, runId),
                      new Relation(NamespaceId.DEFAULT.dataset(writeDataset), programId, AccessType.WRITE, runId));
    Assert.assertEquals(expectedLineage, lineage.getRelations());

    // get dataset lineage for dest dataset, in this test they should be same
    lineage = lineageAdmin.computeLineage(NamespaceId.DEFAULT.dataset(writeDataset),
                                          0, System.currentTimeMillis(), 1, "workflow");
    Assert.assertEquals(2, lineage.getRelations().size());
    Assert.assertEquals(expectedLineage, lineage.getRelations());
  }

  @Test
  public void testPluginOfPluginMacros() throws Exception {
    testPluginOfPluginMacros(Engine.MAPREDUCE);
    testPluginOfPluginMacros(Engine.SPARK);
  }

  private void testPluginOfPluginMacros(Engine engine) throws Exception {
    String sourceName = UUID.randomUUID().toString();
    String sinkName = UUID.randomUUID().toString();

    // source -> filter -> sink
    // the filter uses the plugin framework to instantiate a ValueFilter
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName)))
      .addStage(new ETLStage("filter", PluggableFilterTransform.getPlugin(
        ValueFilter.NAME, ValueFilter.getProperties("${field}", "${value}"))))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addConnection("source", "filter")
      .addConnection("filter", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    Schema schema = Schema.recordOf("x", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    StructuredRecord samuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord dwayne = StructuredRecord.builder(schema).set("name", "dwayne").build();

    DataSetManager<Table> sourceTable = getDataset(sourceName);
    MockSource.writeInput(sourceTable, ImmutableList.of(samuel, dwayne));

    // test macro evaluation of the ValueFilter plugin
    // pipeline should filter out records where the value of 'name' is 'samuel'
    Map<String, String> runtimeArgs = new HashMap<>();
    runtimeArgs.put("field", "name");
    runtimeArgs.put("value", "samuel");
    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.startAndWaitForGoodRun(runtimeArgs, ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    DataSetManager<Table> sinkTable = getDataset(sinkName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(sinkTable);
    Assert.assertEquals(Collections.singletonList(dwayne), outputRecords);
  }

  @Test
  public void testAlertPublisher() throws Exception {
    testAlertPublisher(Engine.MAPREDUCE);
    testAlertPublisher(Engine.SPARK);
  }

  private void testAlertPublisher(Engine engine) throws Exception {
    String sourceName = "alertSource" + engine.name();
    String sinkName = "alertSink" + engine.name();
    String topic = "alertTopic" + engine.name();
    /*
     * source --> nullAlert --> sink
     *               |
     *               |--> TMS publisher
     */
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName)))
      .addStage(new ETLStage("nullAlert", NullAlertTransform.getPlugin("id")))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addStage(new ETLStage("tms alert", TMSAlertPublisher.getPlugin(topic, NamespaceId.DEFAULT.getNamespace())))
      .addConnection("source", "nullAlert")
      .addConnection("nullAlert", "sink")
      .addConnection("nullAlert", "tms alert")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("AlertTest-" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    Schema schema = Schema.recordOf("x", Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
    StructuredRecord record1 = StructuredRecord.builder(schema).set("id", 1L).build();
    StructuredRecord record2 = StructuredRecord.builder(schema).set("id", 2L).build();
    StructuredRecord alertRecord = StructuredRecord.builder(schema).build();

    DataSetManager<Table> sourceTable = getDataset(sourceName);
    MockSource.writeInput(sourceTable, ImmutableList.of(record1, record2, alertRecord));

    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.start();
    manager.waitForRun(ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    DataSetManager<Table> sinkTable = getDataset(sinkName);
    Set<StructuredRecord> actual = new HashSet<>(MockSink.readOutput(sinkTable));
    Set<StructuredRecord> expected = ImmutableSet.of(record1, record2);
    Assert.assertEquals(expected, actual);

    MessageFetcher messageFetcher = getMessagingContext().getMessageFetcher();
    Set<Alert> actualMessages = new HashSet<>();
    try (CloseableIterator<Message> iter = messageFetcher.fetch(NamespaceId.DEFAULT.getNamespace(), topic, 5, 0)) {
      while (iter.hasNext()) {
        Message message = iter.next();
        Alert alert = message.decodePayload(r -> GSON.fromJson(r, Alert.class));
        actualMessages.add(alert);
      }
    }
    Set<Alert> expectedMessages = ImmutableSet.of(new Alert("nullAlert", new HashMap<>()));
    Assert.assertEquals(expectedMessages, actualMessages);

    validateMetric(3, appId, "source.records.out");
    validateMetric(3, appId, "nullAlert.records.in");
    validateMetric(2, appId, "nullAlert.records.out");
    validateMetric(1, appId, "nullAlert.records.alert");
    validateMetric(2, appId, "sink.records.in");
    validateMetric(1, appId, "tms alert.records.in");
  }

  @Test
  public void testExternalSparkProgramPipelines() throws Exception {
    File testDir = TMP_FOLDER.newFolder("sparkProgramTest");

    File input = new File(testDir, "poem.txt");
    try (PrintWriter writer = new PrintWriter(input.getAbsolutePath())) {
      writer.println("this");
      writer.println("is");
      writer.println("a");
      writer.println("poem");
      writer.println("it");
      writer.println("is");
      writer.println("a");
      writer.println("bad");
      writer.println("poem");
    }
    File wordCountOutput = new File(testDir, "poem_counts");
    File filterOutput = new File(testDir, "poem_filtered");

    String args = String.format("%s %s", input.getAbsolutePath(), wordCountOutput.getAbsolutePath());
    Map<String, String> wordCountProperties = ImmutableMap.of("program.args", args);
    Map<String, String> filterProperties = ImmutableMap.of(
      "inputPath", input.getAbsolutePath(),
      "outputPath", filterOutput.getAbsolutePath(),
      "filterStr", "bad");

    ETLBatchConfig etlConfig = io.cdap.cdap.etl.proto.v2.ETLBatchConfig.builder()
      .addStage(new ETLStage("wordcount", new ETLPlugin(WORDCOUNT_PLUGIN, SPARK_TYPE, wordCountProperties, null)))
      .addStage(new ETLStage("filter", new ETLPlugin(FILTER_PLUGIN, SPARK_TYPE, filterProperties, null)))
      .addConnection("wordcount", "filter")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("sparkProgramTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.start();
    manager.waitForRun(ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    // check wordcount output
    /*
        this is a poem
        it is a bad poem
     */
    Map<String, Integer> expected = new HashMap<>();
    expected.put("this", 1);
    expected.put("is", 2);
    expected.put("a", 2);
    expected.put("poem", 2);
    expected.put("it", 1);
    expected.put("bad", 1);
    Map<String, Integer> counts = new HashMap<>();
    File[] files = wordCountOutput.listFiles();
    Assert.assertNotNull("No output files for wordcount found.", files);
    for (File file : files) {
      String fileName = file.getName();
      if (fileName.startsWith(".") || fileName.equals("_SUCCESS")) {
        continue;
      }
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] fields = line.split(" ");
          counts.put(fields[0], Integer.parseInt(fields[1]));
        }
      }
    }
    Assert.assertEquals(expected, counts);

    // check filter output
    files = filterOutput.listFiles();
    Assert.assertNotNull("No output files for filter program found.", files);
    // Note: we are only interested in the word "bad" being filtered out for the assertion hence it is okay to use a
    // set here even though it will not assert for the cardinality.
    Set<String> expectedLines = ImmutableSet.of("this", "is", "a", "poem", "it", "is", "a", "poem");
    Set<String> actualLines = new HashSet<>();
    for (File file : files) {
      String fileName = file.getName();
      if (fileName.startsWith(".") || fileName.equals("_SUCCESS")) {
        continue;
      }
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        String line;
        while ((line = reader.readLine()) != null) {
          actualLines.add(line);
        }
      }
    }
    Assert.assertEquals(expectedLines, actualLines);
  }

  @Test
  public void testScheduledPipelines() throws Exception {
    // Deploy middle pipeline scheduled to be triggered by the completion of head pipeline
    String expectedValue1 = "headArgValue";
    String expectedValue2 = "headPluginValue";
    WorkflowManager middleWorkflowManagerMR =
      deployPipelineWithSchedule("middle", Engine.SPARK, "head",
                                 new ArgumentMapping("head-arg", "middle-arg"), expectedValue1,
                                 new PluginPropertyMapping("action1", "value", "middle-plugin"), expectedValue2);
    // Deploy tail pipeline scheduled to be triggered by the completion of middle pipeline
    WorkflowManager tailWorkflowManagerMR =
      deployPipelineWithSchedule("tail", Engine.MAPREDUCE, "middle",
                                 new ArgumentMapping("middle-arg", "tail-arg"), expectedValue1,
                                 new PluginPropertyMapping("action2", "value", "tail-plugin"), expectedValue2);
    // Run the head pipeline and wait for its completion
    runHeadTriggeringPipeline(Engine.MAPREDUCE, expectedValue1, expectedValue2);
    // After the completion of the head pipeline, verify the results of middle pipeline
    assertTriggeredPipelinesResult(middleWorkflowManagerMR, "middle", Engine.SPARK, expectedValue1, expectedValue2);
    // After the completion of the middle pipeline, verify the results of tail pipeline
    assertTriggeredPipelinesResult(tailWorkflowManagerMR, "tail", Engine.MAPREDUCE, expectedValue1, expectedValue2);
  }

  private void runHeadTriggeringPipeline(Engine engine, String expectedValue1, String expectedValue2) throws Exception {
    // set runtime arguments
    Map<String, String> runtimeArguments = ImmutableMap.of("head-arg", expectedValue1);
    ETLStage action1 = new ETLStage("action1", MockAction.getPlugin("actionTable", "action1.row", "action1.column",
                                                                    expectedValue2));
    ETLBatchConfig etlConfig = io.cdap.cdap.etl.proto.v2.ETLBatchConfig.builder()
      .addStage(action1)
      .setEngine(engine)
      .build();

    AppRequest<io.cdap.cdap.etl.proto.v2.ETLBatchConfig> appRequest =
      new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("head");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.setRuntimeArgs(runtimeArguments);
    manager.start(ImmutableMap.of("logical.start.time", "0"));
    manager.waitForRun(ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);
  }

  private WorkflowManager deployPipelineWithSchedule(String pipelineName, Engine engine, String triggeringPipelineName,
                                                     ArgumentMapping key1Mapping, String expectedKey1Value,
                                                     PluginPropertyMapping key2Mapping, String expectedKey2Value)
    throws Exception {
    String tableName = "actionScheduleTable" + pipelineName + engine;
    String sourceName = "macroActionWithScheduleInput-" + pipelineName + engine;
    String sinkName = "macroActionWithScheduleOutput-" + pipelineName + engine;
    String key1 = key1Mapping.getTarget();
    String key2 = key2Mapping.getTarget();
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      // 'filter' stage is configured to remove samuel, but action will set an argument that will make it filter dwayne
      .addStage(new ETLStage("action1", MockAction.getPlugin(tableName, "row1", "column1",
                                                             String.format("${%s}", key1))))
      .addStage(new ETLStage("action2", MockAction.getPlugin(tableName, "row2", "column2",
                                                             String.format("${%s}", key2))))
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName)))
      .addStage(new ETLStage("filter1", StringValueFilterTransform.getPlugin("name", String.format("${%s}", key1))))
      .addStage(new ETLStage("filter2", StringValueFilterTransform.getPlugin("name", String.format("${%s}", key2))))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addConnection("action1", "action2")
      .addConnection("action2", "source")
      .addConnection("source", "filter1")
      .addConnection("filter1", "filter2")
      .addConnection("filter2", "sink")
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(pipelineName);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // there should be only two programs - one workflow and one mapreduce/spark
    Schema schema = Schema.recordOf("testRecord", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    // Use the expectedKey1Value and expectedKey2Value as values for two records, so that Only record "samuel"
    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordKey1Value = StructuredRecord.builder(schema).set("name", expectedKey1Value).build();
    StructuredRecord recordKey2Value = StructuredRecord.builder(schema).set("name", expectedKey2Value).build();

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(sourceName);
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordKey1Value, recordKey2Value));

    String defaultNamespace = NamespaceId.DEFAULT.getNamespace();
    // Use properties from the triggering pipeline as values for runtime argument key1, key2
    TriggeringPropertyMapping propertyMapping =
      new TriggeringPropertyMapping(ImmutableList.of(key1Mapping), ImmutableList.of(key2Mapping));
    ProgramStatusTrigger completeTrigger =
      new ProgramStatusTrigger(new WorkflowId(defaultNamespace, triggeringPipelineName, SmartWorkflow.NAME),
                               ImmutableSet.of(ProgramStatus.COMPLETED));
    ScheduleId scheduleId = appId.schedule("completeSchedule");
    appManager.addSchedule(
      new ScheduleDetail(scheduleId.getNamespace(), scheduleId.getApplication(), scheduleId.getVersion(),
                         scheduleId.getSchedule(), "",
                         new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW, SmartWorkflow.NAME),
                         ImmutableMap.of(SmartWorkflow.TRIGGERING_PROPERTIES_MAPPING, GSON.toJson(propertyMapping)),
                         completeTrigger, ImmutableList.of(), Schedulers.JOB_QUEUE_TIMEOUT_MILLIS, null, null));
    appManager.enableSchedule(scheduleId);
    return appManager.getWorkflowManager(SmartWorkflow.NAME);
  }

  private void assertTriggeredPipelinesResult(WorkflowManager workflowManager, String pipelineName, Engine engine,
                                              String expectedKey1Value, String expectedKey2Value)
    throws Exception {
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);
    List<RunRecord> runRecords = workflowManager.getHistory(ProgramRunStatus.COMPLETED);
    Assert.assertEquals(1, runRecords.size());
    String tableName = "actionScheduleTable"  + pipelineName + engine;
    DataSetManager<Table> actionTableDS = getDataset(tableName);
    Assert.assertEquals(expectedKey1Value, MockAction.readOutput(actionTableDS, "row1", "column1"));
    Assert.assertEquals(expectedKey2Value, MockAction.readOutput(actionTableDS, "row2", "column2"));

    // check sink
    DataSetManager<Table> sinkManager = getDataset("macroActionWithScheduleOutput-" + pipelineName + engine);
    Schema schema = Schema.recordOf("testRecord", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    Set<StructuredRecord> expected =
      ImmutableSet.of(StructuredRecord.builder(schema).set("name", "samuel").build());
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMacroActionPipelines() throws Exception {
    testMacroEvaluationActionPipeline(Engine.MAPREDUCE);
    testMacroEvaluationActionPipeline(Engine.SPARK);
  }

  private void testMacroEvaluationActionPipeline(Engine engine) throws Exception {
    ETLStage action1 = new ETLStage("action1", MockAction.getPlugin("actionTable", "action1.row", "action1.column",
                                                                    "${value}"));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(action1)
      .setEngine(engine)
      .build();

    // set runtime arguments for macro substitution
    Map<String, String> runtimeArguments = ImmutableMap.of("value", "macroValue");

    AppRequest<io.cdap.cdap.etl.proto.v2.ETLBatchConfig> appRequest =
      new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("macroActionTest-" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.setRuntimeArgs(runtimeArguments);
    manager.start(ImmutableMap.of("logical.start.time", "0"));
    manager.waitForRun(ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    DataSetManager<Table> actionTableDS = getDataset("actionTable");
    Assert.assertEquals("macroValue", MockAction.readOutput(actionTableDS, "action1.row", "action1.column"));
  }

  @Test
  public void testErrorTransform() throws Exception {
    testErrorTransform(Engine.MAPREDUCE);
    testErrorTransform(Engine.SPARK);
  }

  private void testErrorTransform(Engine engine) throws Exception {
    String source1TableName = "errTestIn1-" + engine;
    String source2TableName = "errTestIn2-" + engine;

    Schema inputSchema = Schema.recordOf("user",
                                         Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of("id", Schema.of(Schema.Type.INT)));
    File baseOutput = TMP_FOLDER.newFolder();
    String outputDir1 = new File(baseOutput, "output1").getAbsolutePath();
    String outputDir2 = new File(baseOutput, "output2").getAbsolutePath();
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
    ETLBatchConfig config = ETLBatchConfig.builder()
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
      .addStage(new ETLStage("sink1", MockExternalSink.getPlugin("sink1", "sink1", outputDir1)))
      .addStage(new ETLStage("sink2", MockExternalSink.getPlugin("sink2", "sink2", outputDir2)))
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
    ApplicationManager appManager = deployApplication(appId, appRequest);

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
    Map<String, String> args = Collections.singletonMap(io.cdap.cdap.etl.common.Constants.CONSOLIDATE_STAGES, "true");
    workflowManager.startAndWaitForGoodRun(args, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);


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
    Set<StructuredRecord> actual = new HashSet<>(MockExternalSink.readOutput(outputDir1, flattenSchema));
    Assert.assertEquals(expected, actual);

    expected = ImmutableSet.of(
      StructuredRecord.builder(inputSchema).set("name", "Leo").set("id", 1).build(),
      StructuredRecord.builder(inputSchema).set("name", "Ralph").set("id", 2).build(),
      StructuredRecord.builder(inputSchema).set("name", "April").set("id", 5).build());
    actual = new HashSet<>(MockExternalSink.readOutput(outputDir2, inputSchema));
    Assert.assertEquals(expected, actual);

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

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
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
    ApplicationManager appManager = deployApplication(appId, appRequest);

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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
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
    ApplicationManager appManager = deployApplication(appId, appRequest);


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

    List<RunRecord> history = workflowManager.getHistory(ProgramRunStatus.COMPLETED);
    Assert.assertEquals(1, history.size());
    String runId = history.get(0).getPid();

    for (WorkflowToken.Scope scope : Arrays.asList(WorkflowToken.Scope.SYSTEM, WorkflowToken.Scope.USER)) {
      WorkflowTokenDetail token = workflowManager.getToken(runId, scope, null);
      for (Map.Entry<String, List<WorkflowTokenDetail.NodeValueDetail>> tokenData : token.getTokenData().entrySet()) {
        Assert.assertTrue(!tokenData.getKey().startsWith(io.cdap.cdap.etl.common.Constants.StageStatistics.PREFIX));
      }
    }
  }

  @Test
  public void testSimpleCondition() throws Exception {
    testSimpleCondition(Engine.MAPREDUCE);
    testSimpleCondition(Engine.SPARK);
  }

  private void testSimpleCondition(Engine engine) throws Exception {
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    /*
     * source --> condition --> trueSink
     *              |
     *              |-------> falseSink
     *
     */

    String appName = "SimpleCondition-" + engine;
    String source = appName + "Source-" + engine;
    String trueSink = "true" + appName + "Sink-" + engine;
    String falseSink = "false" + appName + "Sink-" + engine;
    String conditionTableName = "condition-" + engine;

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(source, schema)))
      .addStage(new ETLStage("trueSink", MockSink.getPlugin(trueSink)))
      .addStage(new ETLStage("falseSink", MockSink.getPlugin(falseSink)))
      .addStage(new ETLStage("condition", MockCondition.getPlugin("condition", conditionTableName)))
      .addConnection("source", "condition")
      .addConnection("condition", "trueSink", true)
      .addConnection("condition", "falseSink", false)
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);

    ApplicationManager appManager = deployApplication(appId, appRequest);

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();

    // write records to source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(source));

    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob));

    WorkflowManager workflowManager = null;
    for (String branch : Arrays.asList("true", "false")) {
      String sink = branch.equals("true") ? trueSink : falseSink;
      workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
      workflowManager.start(ImmutableMap.of("condition.branch.to.execute", branch));

      if (branch.equals("true")) {
        workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
      } else {
        workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 5, TimeUnit.MINUTES);
      }

      // check sink
      DataSetManager<Table> sinkManager = getDataset(sink);
      Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
      Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
      Assert.assertEquals(expected, actual);

      // check condition table
      DataSetManager<Table> conditionTableDS = getDataset(conditionTableName);
      Assert.assertEquals("2", MockCondition.readOutput(conditionTableDS, "stats", "source.input.records"));
      Assert.assertEquals("2", MockCondition.readOutput(conditionTableDS, "stats", "source.output.records"));
      Assert.assertEquals("0", MockCondition.readOutput(conditionTableDS, "stats", "source.error.records"));

      validateMetric(branch.equals("true") ? 2 : 4, appId, "source.records.out");
      validateMetric(2, appId, branch + "Sink.records.in");
    }

    boolean foundStatisticsInToken = false;
    if (workflowManager != null) {
      List<RunRecord> history = workflowManager.getHistory(ProgramRunStatus.COMPLETED);
      // Checking for single run should be fine.
      String runId = history.get(0).getPid();
      for (WorkflowToken.Scope scope : Arrays.asList(WorkflowToken.Scope.SYSTEM, WorkflowToken.Scope.USER)) {
        WorkflowTokenDetail token = workflowManager.getToken(runId, scope, null);
        for (Map.Entry<String, List<WorkflowTokenDetail.NodeValueDetail>> tokenData : token.getTokenData().entrySet()) {
          if (tokenData.getKey().startsWith(io.cdap.cdap.etl.common.Constants.StageStatistics.PREFIX)) {
            foundStatisticsInToken = true;
            break;
          }
        }
      }
    }
    Assert.assertTrue(foundStatisticsInToken);
  }

  @Test
  public void testSimpleConditionWithActions() throws Exception {
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    /*
     * action --> condition --> file ---> trueSink
     *              |
     *              |---file-->----> falseSink
     *
     */

    String appName = "SimpleConditionWithActions";
    String trueSource = "true" + appName + "Source";
    String falseSource = "false" + appName + "Source";
    String trueSink = "true" + appName + "Sink";
    String falseSink = "false" + appName + "Sink";
    String actionTable = "actionTable" + appName;

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("trueSource", MockSource.getPlugin(trueSource, schema)))
      .addStage(new ETLStage("falseSource", MockSource.getPlugin(falseSource, schema)))
      .addStage(new ETLStage("trueSink", MockSink.getPlugin(trueSink)))
      .addStage(new ETLStage("falseSink", MockSink.getPlugin(falseSink)))
      .addStage(new ETLStage("condition", MockCondition.getPlugin("condition")))
      .addStage(new ETLStage("action", MockAction.getPlugin(actionTable, "row1", "key1", "val1")))
      .addConnection("action", "condition")
      .addConnection("condition", "trueSource", true)
      .addConnection("condition", "falseSource", false)
      .addConnection("trueSource", "trueSink")
      .addConnection("falseSource", "falseSink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();

    for (String branch : Arrays.asList("true", "false")) {
      String source = branch.equals("true") ? trueSource : falseSource;
      String sink = branch.equals("true") ? trueSink : falseSink;
      // write records to source
      DataSetManager<Table> inputManager
        = getDataset(NamespaceId.DEFAULT.dataset(source));
      MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob));
      WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
      workflowManager.start(ImmutableMap.of("condition.branch.to.execute", branch));

      if (branch.equals("true")) {
        workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
      } else {
        workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 5, TimeUnit.MINUTES);
      }

      // check sink
      DataSetManager<Table> sinkManager = getDataset(sink);
      Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
      Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
      Assert.assertEquals(expected, actual);

      validateMetric(2, appId, branch + "Source.records.out");
      validateMetric(2, appId, branch + "Sink.records.in");
      // check Action is executed correctly
      DataSetManager<Table> actionTableDS = getDataset(actionTable);
      Assert.assertEquals("val1", MockAction.readOutput(actionTableDS, "row1", "key1"));
    }
  }

  @Test
  public void testSimpleConditionWithMultipleInputActions() throws Exception {
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    /*
     * action1--|
     *          |--> condition --> file ---> trueSink
     * action2--|      |
     *                 |--->file----> falseSink
     *
     */
    String appName = "SimpleConditionWithMultipleInputActions";
    String trueSource = "true" + appName + "Source";
    String falseSource = "false" + appName + "Source";
    String trueSink = "true" + appName + "Sink";
    String falseSink = "false" + appName + "Sink";
    String actionTable = "actionTable" + appName;

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("trueSource", MockSource.getPlugin(trueSource, schema)))
      .addStage(new ETLStage("falseSource", MockSource.getPlugin(falseSource, schema)))
      .addStage(new ETLStage("trueSink", MockSink.getPlugin(trueSink)))
      .addStage(new ETLStage("falseSink", MockSink.getPlugin(falseSink)))
      .addStage(new ETLStage("condition", MockCondition.getPlugin("condition")))
      .addStage(new ETLStage("action1", MockAction.getPlugin(actionTable, "row1", "key1", "val1")))
      .addStage(new ETLStage("action2", MockAction.getPlugin(actionTable, "row2", "key2", "val2")))
      .addConnection("action1", "condition")
      .addConnection("action2", "condition")
      .addConnection("condition", "trueSource", true)
      .addConnection("condition", "falseSource", false)
      .addConnection("trueSource", "trueSink")
      .addConnection("falseSource", "falseSink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();

    for (String branch : Arrays.asList("true", "false")) {
      // write records to source
      String source = branch.equals("true") ? trueSource : falseSource;
      String sink = branch.equals("true") ? trueSink : falseSink;

      DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(source));
      MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob));
      WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
      workflowManager.start(ImmutableMap.of("condition.branch.to.execute", branch));

      if (branch.equals("true")) {
        workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
      } else {
        workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 5, TimeUnit.MINUTES);
      }

      // check sink
      DataSetManager<Table> sinkManager = getDataset(sink);
      Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
      Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
      Assert.assertEquals(expected, actual);

      validateMetric(2, appId, branch + "Source.records.out");
      validateMetric(2, appId, branch + "Sink.records.in");
      // check Action1 and Action2 is executed correctly
      DataSetManager<Table> actionTableDS = getDataset(actionTable);
      Assert.assertEquals("val1", MockAction.readOutput(actionTableDS, "row1", "key1"));
      Assert.assertEquals("val2", MockAction.readOutput(actionTableDS, "row2", "key2"));
    }
  }

  @Test
  public void testMultipleOrderedInputActions() throws Exception {
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    /*
     * action1--->action2---|
     *                      |--> condition --> file ---> trueSink
     * action3--->action4---|      |
     *                             |--->file----> falseSink
     *
     */

    String appName = "MultipleOrderedInputActions";
    String trueSource = "true" + appName + "Source";
    String falseSource = "false" + appName + "Source";
    String trueSink = "true" + appName + "Sink";
    String falseSink = "false" + appName + "Sink";
    String actionTable = "actionTable" + appName;

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("trueSource", MockSource.getPlugin(trueSource, schema)))
      .addStage(new ETLStage("falseSource", MockSource.getPlugin(falseSource, schema)))
      .addStage(new ETLStage("trueSink", MockSink.getPlugin(trueSink)))
      .addStage(new ETLStage("falseSink", MockSink.getPlugin(falseSink)))
      .addStage(new ETLStage("condition", MockCondition.getPlugin("condition")))
      .addStage(new ETLStage("action1", MockAction.getPlugin(actionTable, "row1", "key1", "val1")))
      .addStage(new ETLStage("action2", MockAction.getPlugin(actionTable, "row2", "key2", "val2", "row1key1", "val1")))
      .addStage(new ETLStage("action3", MockAction.getPlugin(actionTable, "row3", "key3", "val3")))
      .addStage(new ETLStage("action4", MockAction.getPlugin(actionTable, "row4", "key4", "val4", "row3key3", "val3")))
      .addConnection("action1", "action2")
      .addConnection("action3", "action4")
      .addConnection("action2", "condition")
      .addConnection("action4", "condition")
      .addConnection("condition", "trueSource", true)
      .addConnection("condition", "falseSource", false)
      .addConnection("trueSource", "trueSink")
      .addConnection("falseSource", "falseSink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();

    for (String branch : Arrays.asList("true", "false")) {
      // write records to source
      String source = branch.equals("true") ? trueSource : falseSource;
      String sink = branch.equals("true") ? trueSink : falseSink;
      DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(source));
      MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob));
      WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
      workflowManager.start(ImmutableMap.of("condition.branch.to.execute", branch));

      if (branch.equals("true")) {
        workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
      } else {
        workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 5, TimeUnit.MINUTES);
      }

      // check sink
      DataSetManager<Table> sinkManager = getDataset(sink);
      Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
      Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
      Assert.assertEquals(expected, actual);

      validateMetric(2, appId, branch + "Source.records.out");
      validateMetric(2, appId, branch + "Sink.records.in");
      // check Action1 and Action2 is executed correctly
      DataSetManager<Table> actionTableDS = getDataset(actionTable);
      Assert.assertEquals("val1", MockAction.readOutput(actionTableDS, "row1", "key1"));
      Assert.assertEquals("val2", MockAction.readOutput(actionTableDS, "row2", "key2"));
      Assert.assertEquals("val3", MockAction.readOutput(actionTableDS, "row3", "key3"));
      Assert.assertEquals("val4", MockAction.readOutput(actionTableDS, "row4", "key4"));
    }
  }

  @Test
  public void testConditionsOnBranches() throws Exception {
    /*
     *                            |-- true --> sink1
     *          |--> condition1 --|
     * source --|                 |-- false --> sink2
     *          |
     *          |                              |-- true --> sink3
     *          |-- transform --> condition2 --|
     *                                         |-- false --> sink4
     */
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );
    String sourceName = "branchConditionsSource";
    String sink1Name = "branchConditionsSink1";
    String sink2Name = "branchConditionsSink2";
    String sink3Name = "branchConditionsSink3";
    String sink4Name = "branchConditionsSink4";
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName, schema)))
      .addStage(new ETLStage("condition1", MockCondition.getPlugin("condition1")))
      .addStage(new ETLStage("transform", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("condition2", MockCondition.getPlugin("condition2")))
      .addStage(new ETLStage("sink1", MockSink.getPlugin(sink1Name)))
      .addStage(new ETLStage("sink2", MockSink.getPlugin(sink2Name)))
      .addStage(new ETLStage("sink3", MockSink.getPlugin(sink3Name)))
      .addStage(new ETLStage("sink4", MockSink.getPlugin(sink4Name)))
      .addConnection("source", "condition1")
      .addConnection("source", "transform")
      .addConnection("condition1", "sink1", true)
      .addConnection("condition1", "sink2", false)
      .addConnection("transform", "condition2")
      .addConnection("condition2", "sink3", true)
      .addConnection("condition2", "sink4", false)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("branchConditions");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    List<StructuredRecord> records = Collections.singletonList(
      StructuredRecord.builder(schema).set("name", "samuel").build());

    DataSetManager<Table> inputManager = getDataset(sourceName);
    MockSource.writeInput(inputManager, records);
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(ImmutableMap.of("condition1.branch.to.execute", "true",
                                          "condition2.branch.to.execute", "false"));
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> sink1Manager = getDataset(sink1Name);
    DataSetManager<Table> sink2Manager = getDataset(sink2Name);
    DataSetManager<Table> sink3Manager = getDataset(sink3Name);
    DataSetManager<Table> sink4Manager = getDataset(sink4Name);
    Assert.assertEquals(records, MockSink.readOutput(sink1Manager));
    Assert.assertTrue(MockSink.readOutput(sink2Manager).isEmpty());
    Assert.assertTrue(MockSink.readOutput(sink3Manager).isEmpty());
    Assert.assertEquals(records, MockSink.readOutput(sink4Manager));
  }

  @Test
  public void testSimpleConditionWithSingleOutputAction() throws Exception {
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    /*
     *
     * condition --Action--> file ---> trueSink
     *       |
     *       |--->file----> falseSink
     *
     */

    String appName = "SimpleConditionWithSingleOutputAction";
    String trueSource = "true" + appName + "Source";
    String falseSource = "false" + appName + "Source";
    String trueSink = "true" + appName + "Sink";
    String falseSink = "false" + appName + "Sink";
    String actionTable = "actionTable" + appName;

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("trueSource", MockSource.getPlugin(trueSource, schema)))
      .addStage(new ETLStage("falseSource", MockSource.getPlugin(falseSource, schema)))
      .addStage(new ETLStage("trueSink", MockSink.getPlugin(trueSink)))
      .addStage(new ETLStage("falseSink", MockSink.getPlugin(falseSink)))
      .addStage(new ETLStage("condition", MockCondition.getPlugin("condition")))
      .addStage(new ETLStage("action", MockAction.getPlugin(actionTable, "row1", "key1", "val1")))
      .addConnection("condition", "action", true)
      .addConnection("action", "trueSource")
      .addConnection("trueSource", "trueSink")
      .addConnection("condition", "falseSource", false)
      .addConnection("falseSource", "falseSink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();

    for (String branch : Arrays.asList("true", "false")) {
      String source = branch.equals("true") ? trueSource : falseSource;
      String sink = branch.equals("true") ? trueSink : falseSink;
      // write records to source
      DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(source));
      MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob));
      WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
      workflowManager.start(ImmutableMap.of("condition.branch.to.execute", branch));

      if (branch.equals("true")) {
        workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
      } else {
        workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 5, TimeUnit.MINUTES);
      }

      // check sink
      DataSetManager<Table> sinkManager = getDataset(sink);
      Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
      Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
      Assert.assertEquals(expected, actual);

      validateMetric(2, appId, branch + "Source.records.out");
      validateMetric(2, appId, branch + "Sink.records.in");
      // check Action1 and Action2 is executed correctly
      DataSetManager<Table> actionTableDS = getDataset(actionTable);
      if (branch.equals("true")) {
        Assert.assertEquals("val1", MockAction.readOutput(actionTableDS, "row1", "key1"));
      }
    }
  }

  @Test
  public void testSimpleConditionWithMultipleOutputActions() throws Exception {
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    /*
     *
     * condition --Action--> file ---> trueSink
     *       |
     *       |--->Action--->file----> falseSink
     *
     */
    String appName = "SimpleConditionWithMultipleOutputActions";
    String trueSource = "true" + appName + "Source";
    String falseSource = "false" + appName + "Source";
    String trueSink = "true" + appName + "Sink";
    String falseSink = "false" + appName + "Sink";
    String actionTable = "actionTable" + appName;

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("trueSource", MockSource.getPlugin(trueSource, schema)))
      .addStage(new ETLStage("falseSource", MockSource.getPlugin(falseSource, schema)))
      .addStage(new ETLStage("trueSink", MockSink.getPlugin(trueSink)))
      .addStage(new ETLStage("falseSink", MockSink.getPlugin(falseSink)))
      .addStage(new ETLStage("condition", MockCondition.getPlugin("condition")))
      .addStage(new ETLStage("action1", MockAction.getPlugin(actionTable, "row1", "key1", "val1")))
      .addStage(new ETLStage("action2", MockAction.getPlugin(actionTable, "row2", "key2", "val2")))
      .addConnection("condition", "action1", true)
      .addConnection("action1", "trueSource")
      .addConnection("trueSource", "trueSink")
      .addConnection("condition", "action2", false)
      .addConnection("action2", "falseSource")
      .addConnection("falseSource", "falseSink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();

    for (String branch : Arrays.asList("true", "false")) {
      String source = branch.equals("true") ? trueSource : falseSource;
      String sink = branch.equals("true") ? trueSink : falseSink;
      // write records to source
      DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(source));
      MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob));
      WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
      workflowManager.start(ImmutableMap.of("condition.branch.to.execute", branch));

      if (branch.equals("true")) {
        workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
      } else {
        workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 5, TimeUnit.MINUTES);
      }

      // check sink
      DataSetManager<Table> sinkManager = getDataset(sink);
      Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
      Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
      Assert.assertEquals(expected, actual);

      validateMetric(2, appId, branch + "Source.records.out");
      validateMetric(2, appId, branch + "Sink.records.in");
      // check Action1 and Action2 is executed correctly
      DataSetManager<Table> actionTableDS = getDataset(actionTable);
      if (branch.equals("true")) {
        Assert.assertEquals("val1", MockAction.readOutput(actionTableDS, "row1", "key1"));
      } else {
        Assert.assertEquals("val2", MockAction.readOutput(actionTableDS, "row2", "key2"));
      }
    }
  }

  @Test
  public void testNestedCondition() throws Exception {
    testNestedCondition(Engine.MAPREDUCE);
    testNestedCondition(Engine.SPARK);
  }

  private void testNestedCondition(Engine engine) throws Exception {
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    /*
     * action1----->|
     *              |
     * action2----->|
     *              |
     *              V
     * source --> condition1 --> sink1
     *              |
     *              |------->condition2 --> sink2
     *                          |
     *                          |-------> condition3----> sink3----->action3----->condition4---->action4
     *                                                                                |
     *                                                                                |------>action5
     */

    String appName = "NestedCondition-" + engine;
    String source = appName + "Source-" + engine;
    String sink1 = appName + "Sink1-" + engine;
    String sink2 = appName + "Sink2-" + engine;
    String sink3 = appName + "Sink3-" + engine;
    String actionTable = "actionTable" + appName + "-" + engine;

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(source, schema)))
      .addStage(new ETLStage("sink1", MockSink.getPlugin(sink1)))
      .addStage(new ETLStage("sink2", MockSink.getPlugin(sink2)))
      .addStage(new ETLStage("sink3", MockSink.getPlugin(sink3)))
      .addStage(new ETLStage("action1", MockAction.getPlugin(actionTable, "row1", "key1", "val1")))
      .addStage(new ETLStage("action2", MockAction.getPlugin(actionTable, "row2", "key2", "val2")))
      .addStage(new ETLStage("action3", MockAction.getPlugin(actionTable, "row3", "key3", "val3")))
      .addStage(new ETLStage("action4", MockAction.getPlugin(actionTable, "row4", "key4", "val4")))
      .addStage(new ETLStage("action5", MockAction.getPlugin(actionTable, "row5", "key5", "val5")))
      .addStage(new ETLStage("condition1", MockCondition.getPlugin("condition1")))
      .addStage(new ETLStage("condition2", MockCondition.getPlugin("condition2")))
      .addStage(new ETLStage("condition3", MockCondition.getPlugin("condition3")))
      .addStage(new ETLStage("condition4", MockCondition.getPlugin("condition4")))
      .addConnection("action1", "condition1")
      .addConnection("action2", "condition1")
      .addConnection("source", "condition1")
      .addConnection("condition1", "sink1", true)
      .addConnection("condition1", "condition2", false)
      .addConnection("condition2", "sink2", true)
      .addConnection("condition2", "condition3", false)
      .addConnection("condition3", "sink3", true)
      .addConnection("sink3", "action3")
      .addConnection("action3", "condition4")
      .addConnection("condition4", "action4", true)
      .addConnection("condition4", "action5", false)
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);

    ApplicationManager appManager = deployApplication(appId, appRequest);

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();

    // write records to source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(source));
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(ImmutableMap.of("condition3.branch.to.execute", "true",
                                          "condition4.branch.to.execute", "true"));
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // check sink
    DataSetManager<Table> sinkManager = getDataset(sink3);
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    // other sinks should not have any data in them
    sinkManager = getDataset(sink1);
    Assert.assertTrue(MockSink.readOutput(sinkManager).isEmpty());

    sinkManager = getDataset(sink2);
    Assert.assertTrue(MockSink.readOutput(sinkManager).isEmpty());

    // check actions
    DataSetManager<Table> actionTableDS = getDataset(actionTable);
    // action1 is executed
    Assert.assertEquals("val1", MockAction.readOutput(actionTableDS, "row1", "key1"));
    // action2 is executed
    Assert.assertEquals("val2", MockAction.readOutput(actionTableDS, "row2", "key2"));
    // action3 is executed
    Assert.assertEquals("val3", MockAction.readOutput(actionTableDS, "row3", "key3"));
    // action4 is executed
    Assert.assertEquals("val4", MockAction.readOutput(actionTableDS, "row4", "key4"));
    // action5 should not get executed.
    Assert.assertNull(MockAction.readOutput(actionTableDS, "row5", "key5"));
  }

  @Test
  public void testSimpleControlOnlyDag() throws Exception {
    //
    //  condition-->action1
    //     |
    //     |------->action2
    //

    String appName = "SimpleControlOnlyDag";
    String trueActionTable = "trueActionTable" + appName;
    String falseActionTable = "falseActionTable" + appName;

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("condition", MockCondition.getPlugin("condition")))
      .addStage(new ETLStage("action1", MockAction.getPlugin(trueActionTable, "row1", "key1", "val1")))
      .addStage(new ETLStage("action2", MockAction.getPlugin(falseActionTable, "row2", "key2", "val2")))
      .addConnection("condition", "action1", true)
      .addConnection("condition", "action2", false)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    for (String branch : Arrays.asList("true", "false")) {
      String table = branch.equals("true") ? trueActionTable : falseActionTable;
      WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
      workflowManager.start(ImmutableMap.of("condition.branch.to.execute", branch));

      if (branch.equals("true")) {
        workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
      } else {
        workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 5, TimeUnit.MINUTES);
      }

      DataSetManager<Table> actionTableDS = getDataset(table);
      if (branch.equals("true")) {
        Assert.assertEquals("val1", MockAction.readOutput(actionTableDS, "row1", "key1"));
      } else {
        Assert.assertEquals("val2", MockAction.readOutput(actionTableDS, "row2", "key2"));
      }
    }
  }

  @Test
  public void testMultiConditionControlOnlyDag() throws Exception {
    //
    //
    //   action1---
    //            |---condition1---action3
    //   action2---      |
    //                  action4-----condition2-----condition3----action5
    //

    String appName = "MultiConditionControlOnlyDag";
    String actionTable = "actionTable" + appName;

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("condition1", MockCondition.getPlugin("condition1")))
      .addStage(new ETLStage("condition2", MockCondition.getPlugin("condition2")))
      .addStage(new ETLStage("condition3", MockCondition.getPlugin("condition3")))
      .addStage(new ETLStage("action1", MockAction.getPlugin(actionTable, "row1", "key1", "val1")))
      .addStage(new ETLStage("action2", MockAction.getPlugin(actionTable, "row2", "key2", "val2")))
      .addStage(new ETLStage("action3", MockAction.getPlugin(actionTable, "row3", "key3", "val3")))
      .addStage(new ETLStage("action4", MockAction.getPlugin(actionTable, "row4", "key4", "val4")))
      .addStage(new ETLStage("action5", MockAction.getPlugin(actionTable, "row5", "key5", "val5")))
      .addConnection("action1", "condition1")
      .addConnection("action2", "condition1")
      .addConnection("condition1", "action3", true)
      .addConnection("condition1", "action4", false)
      .addConnection("action4", "condition2")
      .addConnection("condition2", "condition3", true)
      .addConnection("condition3", "action5", true)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(ImmutableMap.of("condition2.branch.to.execute", "true",
                                          "condition3.branch.to.execute", "true"));

    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    DataSetManager<Table> actionTableDS = getDataset(actionTable);
    Assert.assertEquals("val1", MockAction.readOutput(actionTableDS, "row1", "key1"));
    Assert.assertEquals("val2", MockAction.readOutput(actionTableDS, "row2", "key2"));
    Assert.assertEquals("val4", MockAction.readOutput(actionTableDS, "row4", "key4"));
    Assert.assertEquals("val5", MockAction.readOutput(actionTableDS, "row5", "key5"));

    Assert.assertNull(MockAction.readOutput(actionTableDS, "row3", "key3"));
  }

  @Test
  public void testNoConnectorsForSourceCondition() throws Exception {
    //
    //  condition1-->condition2-->source-->sink
    //

    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin("simpleNoConnectorConditionSource", schema)))
      .addStage(new ETLStage("trueSink", MockSink.getPlugin("trueOutput")))
      .addStage(new ETLStage("condition1", MockCondition.getPlugin("condition1")))
      .addStage(new ETLStage("condition2", MockCondition.getPlugin("condition2")))
      .addConnection("condition1", "condition2", true)
      .addConnection("condition2", "source", true)
      .addConnection("source", "trueSink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("NoConnectorForSourceConditionApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();

    // write records to source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset("simpleNoConnectorConditionSource"));
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob));
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(ImmutableMap.of("condition1.branch.to.execute", "true",
                                          "condition2.branch.to.execute", "true"));

    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // check sink
    DataSetManager<Table> sinkManager = getDataset("trueOutput");
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFailureToStartIncapableProgram() throws Exception {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", IncapableSource.getPlugin()))
      .addStage(new ETLStage("sink", IncapableSink.getPlugin()))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("IncapableApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    // starting the workflow should throw incapable exception as the pipeline contains incapable plugins
    workflowManager.start();
    // the program should fail as it has incapable plugins
    workflowManager.waitForRun(ProgramRunStatus.FAILED, 5, TimeUnit.MINUTES);
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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin("singleInput", schema)))
      .addStage(new ETLStage("sink", MockSink.getPlugin("singleOutput")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

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
     *           |--> sleep --> sink
     * source2 --|
     */
    String source1Name = String.format("simpleMSInput1-%s", engine);
    String source2Name = String.format("simpleMSInput2-%s", engine);
    String sinkName = String.format("simpleMSOutput-%s", engine);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source1", MockSource.getPlugin(source1Name)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(source2Name)))
      .addStage(new ETLStage("sleep", SleepTransform.getPlugin(2L)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addConnection("source1", "sleep")
      .addConnection("source2", "sleep")
      .addConnection("sleep", "sink")
      .setEngine(engine)
      .build();


    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SimpleMultiSourceApp-" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // there should be only two programs - one workflow and one mapreduce/spark
    Assert.assertEquals(2, appManager.getInfo().getPrograms().size());
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );
    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    StructuredRecord recordVincent = StructuredRecord.builder(schema).set("name", "vincent").build();

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(source1Name));
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordVincent));
    inputManager = getDataset(NamespaceId.DEFAULT.dataset(source2Name));
    MockSource.writeInput(inputManager, ImmutableList.of(recordBob));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // check sink
    DataSetManager<Table> sinkManager = getDataset(sinkName);
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob, recordVincent);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    validateMetric(2, appId, "source1.records.out");
    validateMetric(1, appId, "source2.records.out");
    validateMetric(3, appId, "sleep.records.in");
    validateMetric(3, appId, "sleep.records.out");
    validateMetric(3, appId, "sink.records.in");
    Assert.assertTrue(getMetric(appId, "sleep." + io.cdap.cdap.etl.common.Constants.Metrics.TOTAL_TIME) > 0L);

    try (CloseableIterator<Message> messages =
      getMessagingContext().getMessageFetcher().fetch(appId.getNamespace(), "sleepTopic", 10, null)) {
      Assert.assertTrue(messages.hasNext());
      Assert.assertEquals("2", messages.next().getPayloadAsString());
      Assert.assertFalse(messages.hasNext());
    }

    getMessagingAdmin(appId.getNamespace()).deleteTopic("sleepTopic");
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
    File outputDir = TMP_FOLDER.newFolder();
    String output1 = new File(outputDir, "sink1").getAbsolutePath();
    String output2 = new File(outputDir, "sink2").getAbsolutePath();
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );
    String source1Name = String.format("msInput1-%s", engine);
    String source2Name = String.format("msInput2-%s", engine);
    String source3Name = String.format("msInput3-%s", engine);
    String sink1Name = String.format("msOutput1-%s", engine);
    String sink2Name = String.format("msOutput2-%s", engine);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source1", MockSource.getPlugin(source1Name, schema)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(source2Name, schema)))
      .addStage(new ETLStage("source3", MockSource.getPlugin(source3Name, schema)))
      .addStage(new ETLStage("transform1", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("transform2", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("sink1", MockExternalSink.getPlugin(sink1Name, sink1Name, output1)))
      .addStage(new ETLStage("sink2", MockExternalSink.getPlugin(sink2Name, sink2Name, output2)))
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
    ApplicationManager appManager = deployApplication(appId, appRequest);

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
    Map<String, String> args = Collections.singletonMap(io.cdap.cdap.etl.common.Constants.CONSOLIDATE_STAGES, "true");
    workflowManager.startAndWaitForGoodRun(args, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // sink1 should get records from source1 and source2
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
    Set<StructuredRecord> actual = Sets.newHashSet(MockExternalSink.readOutput(output1, schema));
    Assert.assertEquals(expected, actual);

    // sink2 should get all records
    expected = ImmutableSet.of(recordSamuel, recordBob, recordJane);
    actual = Sets.newHashSet(MockExternalSink.readOutput(output2, schema));
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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
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
    ApplicationManager appManager = deployApplication(appId, appRequest);

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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
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
    ApplicationManager appManager = deployApplication(appId, appRequest);

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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin("actionInput")))
      .addStage(new ETLStage("sink", MockSink.getPlugin("actionOutput")))
      .addPostAction(new ETLStage("tokenWriter", NodeStatesAction.getPlugin("tokenTable")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ActionApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
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
    ApplicationManager appManager = deployApplication(appId, appRequest);


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
    DataSetManager<FileSet> fileSetManager = getDataset(NaiveBayesTrainer.TEXTS_TO_CLASSIFY);
    FileSet fileSet = fileSetManager.get();
    try (PrintStream out = new PrintStream(fileSet.getLocation("inputTexts").getOutputStream(), true, "UTF-8")) {
      out.println("how are you doing today");
      out.println("free money money");
      out.println("what are you doing today");
      out.println("genuine report");
    }

    // manually trigger the pipeline
    Map<String, String> runtimeArgs = new HashMap<>();
    FileSetArguments.setInputPath(runtimeArgs, "inputTexts");
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(runtimeArgs);
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

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source",
                             MockSource.getPlugin(NaiveBayesTrainer.TEXTS_TO_CLASSIFY_SOURCE, SpamMessage.SCHEMA)))
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
    ApplicationManager appManager = deployApplication(appId, appRequest);


    // write some some messages to be classified
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
    messagesToWrite.add(new SpamMessage("how are you doing today").toStructuredRecord());
    messagesToWrite.add(new SpamMessage("free money money").toStructuredRecord());
    messagesToWrite.add(new SpamMessage("what are you doing today").toStructuredRecord());
    messagesToWrite.add(new SpamMessage("genuine report").toStructuredRecord());

    DataSetManager<Table> inputManager =
      getDataset(NamespaceId.DEFAULT.dataset(NaiveBayesTrainer.TEXTS_TO_CLASSIFY_SOURCE));
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

  private void testInnerJoinWithMultiOutput(Engine engine) throws Exception {
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
    File outputDir = TMP_FOLDER.newFolder();
    String output1 = new File(outputDir, "output1").getAbsolutePath();
    String output2 = new File(outputDir, "output2").getAbsolutePath();
    String joinerName = "innerJoiner-" + engine;
    String sinkName = "innerJoinSink-" + engine;
    String sinkName2 = "innerJoinSink-2" + engine;
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source1", MockSource.getPlugin(input1Name, inputSchema1)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(input2Name, inputSchema2)))
      .addStage(new ETLStage("source3", MockSource.getPlugin(input3Name, inputSchema3)))
      .addStage(new ETLStage("t1", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t2", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t3", IdentityTransform.getPlugin()))
      .addStage(new ETLStage(joinerName, MockJoiner.getPlugin("t1.customer_id=t2.cust_id=t3.c_id&" +
                                                                  "t1.customer_name=t2.cust_name=t3.c_name",
                                                                "t1,t2,t3", "")))
      .addStage(new ETLStage(sinkName, MockExternalSink.getPlugin(UUID.randomUUID().toString(), "s1", output1)))
      .addStage(new ETLStage(sinkName2, MockExternalSink.getPlugin(UUID.randomUUID().toString(), "s2", output2)))
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
    ApplicationManager appManager = deployApplication(appId, appRequest);

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

    Map<String, String> args = Collections.singletonMap(io.cdap.cdap.etl.common.Constants.CONSOLIDATE_STAGES, "true");
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForGoodRun(args, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    StructuredRecord joinRecordSamuel = StructuredRecord.builder(outSchema)
      .set("customer_id", "1").set("customer_name", "samuel")
      .set("item_id", "11").set("item_price", 10000L).set("cust_id", "1").set("cust_name", "samuel")
      .set("t_id", "1").set("c_id", "1").set("c_name", "samuel").build();

    StructuredRecord joinRecordJane = StructuredRecord.builder(outSchema)
      .set("customer_id", "3").set("customer_name", "jane")
      .set("item_id", "22").set("item_price", 100L).set("cust_id", "3").set("cust_name", "jane")
      .set("t_id", "2").set("c_id", "3").set("c_name", "jane").build();

    Set<StructuredRecord> expected = ImmutableSet.of(joinRecordSamuel, joinRecordJane);
    Set<StructuredRecord> actual = Sets.newHashSet(MockExternalSink.readOutput(output1, outSchema));
    Assert.assertEquals(expected, actual);

    actual = Sets.newHashSet(MockExternalSink.readOutput(output2, outSchema));
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

  private void testOuterJoin(Engine engine) throws Exception {
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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
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
    ApplicationManager appManager = deployApplication(appId, appRequest);

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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
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
    ApplicationManager appManager = deployApplication(appId, appRequest);

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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockRuntimeDatasetSource.getPlugin("input", "${secure(" + prefix + "source)}")))
      .addStage(new ETLStage("sink", MockRuntimeDatasetSink.getPlugin("output", "${secure(" + prefix + "sink)}")))
      .addConnection("source", "sink")
      .setEngine(engine)
      .build();

    // place dataset names into secure storage
    getSecureStoreManager().put("default", prefix + "source", prefix + "MockSecureSourceDataset",
                                "secure source dataset name", new HashMap<>());
    getSecureStoreManager().put("default", prefix + "sink", prefix + "MockSecureSinkDataset",
                                "secure dataset name", new HashMap<>());

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("App-" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

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

    String name = backwardsCompatible ? null : expectedExternalDatasetOutput;
    ETLBatchConfig.Builder builder = ETLBatchConfig.builder();
    ETLBatchConfig etlConfig = builder
      .setEngine(engine)
        // TODO: test multiple inputs CDAP-5654
      .addStage(new ETLStage("source", MockExternalSource.getPlugin(expectedExternalDatasetInput,
                                                                    inputDir.getAbsolutePath())))
      .addStage(new ETLStage("sink", MockExternalSink.getPlugin(name, "dir1", outputSubDir1.getAbsolutePath(),
                                                                name, "dir2", outputSubDir2.getAbsolutePath())))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ExternalDatasetApp-" + suffix);
    ApplicationManager appManager = deployApplication(appId, appRequest);

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

    Map<String, String> args = Collections.singletonMap(io.cdap.cdap.etl.common.Constants.CONSOLIDATE_STAGES, "true");
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForGoodRun(args, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    List<RunRecord> history = workflowManager.getHistory();
    // there should be only one completed run
    Assert.assertEquals(1, history.size());
    Assert.assertEquals(ProgramRunStatus.COMPLETED, history.get(0).getStatus());

    // Assert output
    Assert.assertEquals(allInput, MockExternalSink.readOutput(outputSubDir1.getAbsolutePath(), schema));
    Assert.assertEquals(allInput, MockExternalSink.readOutput(outputSubDir2.getAbsolutePath(), schema));

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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockRuntimeDatasetSource.getPlugin("mrinput", "${runtime${source}}")))
      .addStage(new ETLStage("sink", MockRuntimeDatasetSink.getPlugin("mroutput", "${runtime}${sink}")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MRApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setEngine(Engine.SPARK)
      .addStage(new ETLStage("source", MockRuntimeDatasetSource.getPlugin("sparkinput", "${runtime${source}}")))
      .addStage(new ETLStage("sink", MockRuntimeDatasetSink.getPlugin("sparkoutput", "${runtime}${sink}")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SparkApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockRuntimeDatasetSource.getPlugin("mrinput", "configTimeMockSourceDataset")))
      .addStage(new ETLStage("sink", MockRuntimeDatasetSink.getPlugin("mroutput", "configTimeMockSinkDataset")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MRApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

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

  @Test
  public void testKVTableLookup() throws Exception {
    addDatasetInstance(KeyValueTable.class.getName(), "ageTable");
    DataSetManager<KeyValueTable> lookupTable = getDataset("ageTable");
    lookupTable.get().write("samuel".getBytes(Charsets.UTF_8), "12".getBytes(Charsets.UTF_8));
    lookupTable.get().write("bob".getBytes(Charsets.UTF_8), "36".getBytes(Charsets.UTF_8));
    lookupTable.get().write("jane".getBytes(Charsets.UTF_8), "25".getBytes(Charsets.UTF_8));
    lookupTable.flush();

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin("inputTable")))
      .addStage(new ETLStage("transform", LookupTransform.getPlugin("person", "age", "ageTable")))
      .addStage(new ETLStage("sink", MockSink.getPlugin("outputTable")))
      .addConnection("source", "transform")
      .addConnection("transform", "sink")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("testKVTableLookup");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // set up input data
    Schema inputSchema = Schema.recordOf(
      "person",
      Schema.Field.of("person", Schema.of(Schema.Type.STRING))
    );
    StructuredRecord recordSamuel = StructuredRecord.builder(inputSchema).set("person", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(inputSchema).set("person", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(inputSchema).set("person", "jane").build();
    DataSetManager<Table> inputTable = getDataset("inputTable");
    MockSource.writeInput(inputTable, ImmutableList.of(recordSamuel, recordBob, recordJane));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME).start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    Schema schema = Schema.recordOf(
      "person",
      Schema.Field.of("person", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("age", Schema.of(Schema.Type.STRING))
    );
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(schema).set("person", "samuel").set("age", "12").build());
    expected.add(StructuredRecord.builder(schema).set("person", "bob").set("age", "36").build());
    expected.add(StructuredRecord.builder(schema).set("person", "jane").set("age", "25").build());
    DataSetManager<Table> outputTable = getDataset("outputTable");
    Set<StructuredRecord> actual = new HashSet<>(MockSink.readOutput(outputTable));
    Assert.assertEquals(expected, actual);
    validateMetric(3, appId, "source.records.out");
    validateMetric(3, appId, "sink.records.in");

    deleteDatasetInstance(NamespaceId.DEFAULT.dataset("inputTable"));
    deleteDatasetInstance(NamespaceId.DEFAULT.dataset("outputTable"));
  }

  @Test
  public void testMetadata() throws Exception {
    ImmutableSet<String> inputTagsToAdd = ImmutableSet.of("tOne", "tTwo");
    ImmutableMap<String, String> inputPropToAdd = ImmutableMap.of("kOne", "vOne", "kTwo", "vTwo");
    MetadataOperation op =
      new MetadataOperation.Put(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(), "singleInput"),
                                inputPropToAdd, inputTagsToAdd);
    Set<MetadataOperation> operations = new HashSet<>(Collections.singletonList(op));

    // run pipeline with the metadata operations which need to be performed
    MetadataAdmin metadataAdmin = getMetadataAdmin();
    runPipelineForMetadata(metadataAdmin, operations);
    waitForMetadataProcessing(metadataAdmin, 2);

    // verify metadata written by the pipeline
    Metadata actual =
      metadataAdmin.getMetadata(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(), "singleInput"),
                                MetadataScope.USER);

    Assert.assertNotNull(actual);
    Assert.assertTrue(!actual.isEmpty());
    // verify the user properties
    Assert.assertEquals(inputPropToAdd, actual.getProperties(MetadataScope.USER));
    Assert.assertEquals(inputTagsToAdd, actual.getTags(MetadataScope.USER));

    // delete some properties and tag
    op = new MetadataOperation.Delete(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(), "singleInput"),
                               ImmutableSet.of("kOne"), ImmutableSet.of("tOne"));
    operations = new HashSet<>(Collections.singleton(op));

    runPipelineForMetadata(metadataAdmin, operations);

    waitForMetadataProcessing(metadataAdmin, 1);

    actual = metadataAdmin.getMetadata(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(), "singleInput"),
                                       MetadataScope.USER);

    Assert.assertNotNull(actual);
    Assert.assertTrue(!actual.isEmpty());
    // verify the user properties
    Assert.assertEquals(Collections.singletonMap("kTwo", "vTwo"), actual.getProperties(MetadataScope.USER));
    Assert.assertEquals(Collections.singleton("tTwo"), actual.getTags(MetadataScope.USER));
  }

  private void waitForMetadataProcessing(MetadataAdmin metadataAdmin, int expectedTagSize)
    throws TimeoutException, InterruptedException, java.util.concurrent.ExecutionException {
    Tasks.waitFor(expectedTagSize, () ->
                    metadataAdmin.getTags(MetadataScope.USER, MetadataEntity.ofDataset(
                      NamespaceId.DEFAULT.getNamespace(), "singleInput")).size(),
                  10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  private void runPipelineForMetadata(MetadataAdmin metadataAdmin,
                                      Set<MetadataOperation> operations) throws Exception {
    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );
    /*
     * source --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin("singleInput", schema, operations)))
      .addStage(new ETLStage("sink", MockSink.getPlugin("singleOutput")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT_RANGE, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MetadataTestApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // wait for the system metadata for the app and the dataset to show up - the pipeline validates them
    Tasks.waitFor(false, () -> metadataAdmin
                    .getProperties(MetadataScope.SYSTEM, appId.toMetadataEntity()).isEmpty(),
                  10, TimeUnit.SECONDS);
    Tasks.waitFor(false, () -> metadataAdmin
                    .getProperties(MetadataScope.SYSTEM,
                                   NamespaceId.DEFAULT.dataset("singleInput").toMetadataEntity()).isEmpty(),
                  10, TimeUnit.SECONDS);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    int numRuns = workflowManager.getHistory().size();
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, numRuns + 1, 5, TimeUnit.MINUTES);
  }

  @Test
  public void testRuntimeArguments() throws Exception {
    testRuntimeArgs(Engine.MAPREDUCE);
    testRuntimeArgs(Engine.SPARK);
  }

  private void testRuntimeArgs(Engine engine) throws Exception {
    String sourceName = "runtimeArgInput-" + engine;
    String sinkName = "runtimeArgOutput-" + engine;
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      // 'filter' stage is configured to remove samuel, but action will set an argument that will make it filter dwayne
      .addStage(new ETLStage("action", MockAction.getPlugin("dumy", "val", "ue", "dwayne")))
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName)))
      .addStage(new ETLStage("filter", StringValueFilterTransform.getPlugin("name", "samuel")))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addConnection("action", "source")
      .addConnection("source", "filter")
      .addConnection("filter", "sink")
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("RuntimeArgApp-" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // there should be only two programs - one workflow and one mapreduce/spark
    Schema schema = Schema.recordOf("testRecord", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordDwayne = StructuredRecord.builder(schema).set("name", "dwayne").build();

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(sourceName);
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordDwayne));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // check sink
    DataSetManager<Table> sinkManager = getDataset(sinkName);
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testTableLookup() throws Exception {
    addDatasetInstance(Table.class.getName(), "personTable");
    DataSetManager<Table> lookupTableManager = getDataset("personTable");
    Table lookupTable = lookupTableManager.get();
    lookupTable.put("samuel".getBytes(Charsets.UTF_8), "age".getBytes(Charsets.UTF_8), "12".getBytes(Charsets.UTF_8));
    lookupTable.put("samuel".getBytes(Charsets.UTF_8), "gender".getBytes(Charsets.UTF_8), "m".getBytes(Charsets.UTF_8));
    lookupTable.put("bob".getBytes(Charsets.UTF_8), "age".getBytes(Charsets.UTF_8), "36".getBytes(Charsets.UTF_8));
    lookupTable.put("bob".getBytes(Charsets.UTF_8), "gender".getBytes(Charsets.UTF_8), "m".getBytes(Charsets.UTF_8));
    lookupTable.put("jane".getBytes(Charsets.UTF_8), "age".getBytes(Charsets.UTF_8), "25".getBytes(Charsets.UTF_8));
    lookupTable.put("jane".getBytes(Charsets.UTF_8), "gender".getBytes(Charsets.UTF_8), "f".getBytes(Charsets.UTF_8));
    lookupTableManager.flush();

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin("inputTable")))
      .addStage(new ETLStage("transform", LookupTransform.getPlugin("person", "age", "personTable")))
      .addStage(new ETLStage("sink", MockSink.getPlugin("outputTable")))
      .addConnection("source", "transform")
      .addConnection("transform", "sink")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("testTableLookup");
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // set up input data
    Schema inputSchema = Schema.recordOf(
      "person",
      Schema.Field.of("person", Schema.of(Schema.Type.STRING))
    );
    StructuredRecord recordSamuel = StructuredRecord.builder(inputSchema).set("person", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(inputSchema).set("person", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(inputSchema).set("person", "jane").build();
    DataSetManager<Table> inputTable = getDataset("inputTable");
    MockSource.writeInput(inputTable, ImmutableList.of(recordSamuel, recordBob, recordJane));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME).start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    Schema schema = Schema.recordOf(
      "person",
      Schema.Field.of("person", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("age", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("gender", Schema.of(Schema.Type.STRING))
    );
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(schema).set("person", "samuel").set("age", "12").set("gender", "m").build());
    expected.add(StructuredRecord.builder(schema).set("person", "bob").set("age", "36").set("gender", "m").build());
    expected.add(StructuredRecord.builder(schema).set("person", "jane").set("age", "25").set("gender", "f").build());
    DataSetManager<Table> outputTable = getDataset("outputTable");
    Set<StructuredRecord> actual = new HashSet<>(MockSink.readOutput(outputTable));
    Assert.assertEquals(expected, actual);
    validateMetric(3, appId, "source.records.out");
    validateMetric(3, appId, "sink.records.in");

    deleteDatasetInstance(NamespaceId.DEFAULT.dataset("inputTable"));
    deleteDatasetInstance(NamespaceId.DEFAULT.dataset("outputTable"));
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

  @Test
  public void testServiceUrlMR() throws Exception {
    testServiceUrl(Engine.MAPREDUCE);
  }

  @Test
  public void testServiceUrlSpark() throws Exception {
    testServiceUrl(Engine.SPARK);
  }

  private void testServiceUrl(Engine engine) throws Exception {

    // Deploy the ServiceApp application
    ApplicationManager appManager = deployApplication(ServiceApp.class);

    // Start Greeting service and use it
    ServiceManager serviceManager = appManager.getServiceManager(ServiceApp.Name.SERVICE_NAME).start();

    // Wait service startup
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    URL url = new URL(serviceManager.getServiceURL(), "name");
    HttpRequest httpRequest = HttpRequest.post(url).withBody("bob").build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest, new DefaultHttpRequestConfig(false));
    Assert.assertEquals(HttpURLConnection.HTTP_OK, httpResponse.getResponseCode());

    url = new URL(serviceManager.getServiceURL(), "name/bob");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    String response;
    try {
      response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
    } finally {
      connection.disconnect();
    }
    Assert.assertEquals("bob", response);

    String sourceName = "ServiceUrlInput-" + engine.name();
    String sinkName = "ServiceUrlOutput-" + engine.name();
    /*
     * source --> filter --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName)))
      .addStage(new ETLStage("filter", FilterTransform.getPlugin("name")))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addConnection("source", "filter")
      .addConnection("filter", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ServiceUrl-" + engine);
    appManager = deployApplication(appId, appRequest);

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
    Set<StructuredRecord> expected = ImmutableSet.of(recordBob);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    serviceManager.stop(null);
    serviceManager.waitForRun(ProgramRunStatus.KILLED, 180, TimeUnit.SECONDS);
  }

  @Test
  public void testSplitterToConnector() throws Exception {
    testSplitterToConnector(Engine.MAPREDUCE);
    testSplitterToConnector(Engine.SPARK);
  }

  private void testSplitterToConnector(Engine engine) throws Exception {
    Schema schema = Schema.recordOf("user",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("email", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    StructuredRecord user0 = StructuredRecord.builder(schema).set("id", 0L).build();
    StructuredRecord user1 = StructuredRecord.builder(schema).set("id", 1L).set("email", "one@example.com").build();
    StructuredRecord user2 = StructuredRecord.builder(schema).set("id", 2L).set("name", "two").build();
    StructuredRecord user3 = StructuredRecord.builder(schema)
      .set("id", 3L).set("name", "three").set("email", "three@example.com").build();

    String sourceName = "splitconSource" + engine.name();
    String sink1Name = "splitconSink1" + engine.name();
    String sink2Name = "splitconSink2" + engine.name();

    /*
     *
     *                                                             |null --> sink1
     *                       |null--> identity-agg --> splitter2 --|
     * source --> splitter1--|                                     |non-null --|
     *                       |                                                 |--> sink2
     *                       |non-null-----------------------------------------|
     */
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName)))
      .addStage(new ETLStage("splitter1", NullFieldSplitterTransform.getPlugin("name")))
      .addStage(new ETLStage("splitter2", NullFieldSplitterTransform.getPlugin("email")))
      .addStage(new ETLStage("identity", IdentityAggregator.getPlugin()))
      .addStage(new ETLStage("sink1", MockSink.getPlugin(sink1Name)))
      .addStage(new ETLStage("sink2", MockSink.getPlugin(sink2Name)))
      .addConnection("source", "splitter1")
      .addConnection("splitter1", "identity", "null")
      .addConnection("splitter1", "sink2", "non-null")
      .addConnection("identity", "splitter2")
      .addConnection("splitter2", "sink1", "null")
      .addConnection("splitter2", "sink2", "non-null")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("SplitConTest-" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input data
    DataSetManager<Table> inputManager = getDataset(sourceName);
    MockSource.writeInput(inputManager, ImmutableList.of(user0, user1, user2, user3));

    // run pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // check output
    // sink1 should only have records where both name and email are null (user0)
    DataSetManager<Table> sinkManager = getDataset(sink1Name);
    Set<StructuredRecord> expected = ImmutableSet.of(user0);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    // sink2 should have anything with a non-null name or non-null email
    sinkManager = getDataset(sink2Name);
    expected = ImmutableSet.of(user1, user2, user3);
    actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    validateMetric(4, appId, "source.records.out");
    validateMetric(1, appId, "sink1.records.in");
    validateMetric(3, appId, "sink2.records.in");
  }

  @Test
  public void testSplitterToJoiner() throws Exception {
    testSplitterToJoiner(Engine.MAPREDUCE);
  }

  private void testSplitterToJoiner(Engine engine) throws Exception {
    Schema schema = Schema.recordOf("user",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Schema infoSchema = Schema.recordOf("userInfo",
                                        Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                        Schema.Field.of("fname", Schema.of(Schema.Type.STRING)));
    Schema joinedSchema = Schema.recordOf("join.output",
                                          Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                          Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                          Schema.Field.of("fname", Schema.of(Schema.Type.STRING)));

    StructuredRecord user0 = StructuredRecord.builder(schema).set("id", 0L).build();
    StructuredRecord user1 = StructuredRecord.builder(schema).set("id", 1L).set("name", "one").build();
    StructuredRecord user0Info = StructuredRecord.builder(infoSchema).set("id", 0L).set("fname", "zero").build();
    StructuredRecord user0Joined = StructuredRecord.builder(joinedSchema).set("id", 0L).set("fname", "zero").build();

    String signupsName = "splitjoinSignups" + engine.name();
    String userInfoName = "splitjoinUserInfo" + engine.name();
    String sink1Name = "splitjoinSink1" + engine.name();
    String sink2Name = "splitjoinSink2" + engine.name();

    /*
     * userInfo --------------------------|
     *                                    |--> joiner --> sink1
     *                            |null --|
     * signups --> namesplitter --|
     *                            |non-null --> sink2
     */
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(new ETLStage("signups", MockSource.getPlugin(signupsName, schema)))
      .addStage(new ETLStage("userInfo", MockSource.getPlugin(userInfoName, infoSchema)))
      .addStage(new ETLStage("namesplitter", NullFieldSplitterTransform.getPlugin("name")))
      .addStage(new ETLStage("joiner", MockJoiner.getPlugin("namesplitter.id=userInfo.id",
                                                            "namesplitter,userInfo", "")))
      .addStage(new ETLStage("sink1", MockSink.getPlugin(sink1Name)))
      .addStage(new ETLStage("sink2", MockSink.getPlugin(sink2Name)))
      .addConnection("signups", "namesplitter")
      .addConnection("namesplitter", "sink2", "non-null")
      .addConnection("namesplitter", "joiner", "null")
      .addConnection("userInfo", "joiner")
      .addConnection("joiner", "sink1")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("SplitJoinTest-" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write signups data
    DataSetManager<Table> signupsManager = getDataset(signupsName);
    MockSource.writeInput(signupsManager, ImmutableList.of(user0, user1));

    // write to userInfo the name for user0 to join against
    DataSetManager<Table> userInfoManager = getDataset(userInfoName);
    MockSource.writeInput(userInfoManager, ImmutableList.of(user0Info));

    // run pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // check output
    DataSetManager<Table> sinkManager = getDataset(sink2Name);
    Set<StructuredRecord> expected = ImmutableSet.of(user1);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    sinkManager = getDataset(sink1Name);
    expected = ImmutableSet.of(user0Joined);
    actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    validateMetric(2, appId, "signups.records.out");
    validateMetric(1, appId, "userInfo.records.out");
    validateMetric(2, appId, "namesplitter.records.in");
    validateMetric(1, appId, "namesplitter.records.out.null");
    validateMetric(1, appId, "namesplitter.records.out.non-null");
    validateMetric(2, appId, "joiner.records.in");
    validateMetric(1, appId, "joiner.records.out");
    validateMetric(1, appId, "sink1.records.in");
    validateMetric(1, appId, "sink2.records.in");
  }

  @Test
  public void testStageConsolidation() throws Exception {
    /*
                                   |non-null port --> sink1
        items1 --> null splitter --|
                                   |null port--> sink2
                                                   ^
                 |--> filter out id == 0  ---------|
        items2 --|    |
                 |    |--> error collector --> sink3
                 |
                 |                 |--> filter out id == 1 --> sink4
                 |--> aggregator --|
                                   |--> filter out id == 2 --> sink5
     */
    Schema schema = Schema.recordOf("item", Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))));
    String input1Name = UUID.randomUUID().toString();
    String input2Name = UUID.randomUUID().toString();
    File outputDir = TMP_FOLDER.newFolder();
    String output1 = new File(outputDir, "output1").getAbsolutePath();
    String output2 = new File(outputDir, "output2").getAbsolutePath();
    String output3 = new File(outputDir, "output3").getAbsolutePath();
    String output4 = new File(outputDir, "output4").getAbsolutePath();
    String output5 = new File(outputDir, "output5").getAbsolutePath();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setEngine(Engine.SPARK)
      .addStage(new ETLStage("items1", MockSource.getPlugin(input1Name, schema)))
      .addStage(new ETLStage("items2", MockSource.getPlugin(input2Name, schema)))
      .addStage(new ETLStage("nullSplitter", NullFieldSplitterTransform.getPlugin("id")))
      .addStage(new ETLStage("filter0", IntValueFilterTransform.getPlugin("id", 0)))
      .addStage(new ETLStage("collector", FlattenErrorTransform.getPlugin()))
      .addStage(new ETLStage("filter1", IntValueFilterTransform.getPlugin("id", 1)))
      .addStage(new ETLStage("filter2", IntValueFilterTransform.getPlugin("id", 2)))
      .addStage(new ETLStage("identityAggregator", IdentityAggregator.getPlugin()))
      .addStage(new ETLStage("sink1", MockExternalSink.getPlugin("s1", "s1", output1)))
      .addStage(new ETLStage("sink2", MockExternalSink.getPlugin("s2", "s2", output2)))
      .addStage(new ETLStage("sink3", MockExternalSink.getPlugin("s3", "s3", output3)))
      .addStage(new ETLStage("sink4", MockExternalSink.getPlugin("s4", "s4", output4)))
      .addStage(new ETLStage("sink5", MockExternalSink.getPlugin("s5", "s5", output5)))
      .addConnection("items1", "nullSplitter")
      .addConnection("nullSplitter", "sink1", "non-null")
      .addConnection("nullSplitter", "sink2", "null")
      .addConnection("items2", "filter0")
      .addConnection("items2", "identityAggregator")
      .addConnection("filter0", "sink2")
      .addConnection("filter0", "collector")
      .addConnection("identityAggregator", "filter1")
      .addConnection("identityAggregator", "filter2")
      .addConnection("collector", "sink3")
      .addConnection("filter1", "sink4")
      .addConnection("filter2", "sink5")
      .setProperties(Collections.singletonMap(io.cdap.cdap.etl.common.Constants.SPARK_PIPELINE_AUTOCACHE_ENABLE_FLAG,
                                              "false"))
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("StageConsolidationTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input data
    StructuredRecord itemNull = StructuredRecord.builder(schema).build();
    StructuredRecord item0 = StructuredRecord.builder(schema).set("id", 0).build();
    StructuredRecord item1 = StructuredRecord.builder(schema).set("id", 1).build();
    StructuredRecord item2 = StructuredRecord.builder(schema).set("id", 2).build();
    StructuredRecord item3 = StructuredRecord.builder(schema).set("id", 3).build();
    DataSetManager<Table> items1Manager = getDataset(input1Name);
    DataSetManager<Table> items2Manager = getDataset(input2Name);
    MockSource.writeInput(items1Manager, Arrays.asList(itemNull, item3));
    MockSource.writeInput(items2Manager, Arrays.asList(item0, item1, item2));

    // run pipeline
    Map<String, String> args = Collections.singletonMap(io.cdap.cdap.etl.common.Constants.CONSOLIDATE_STAGES, "true");
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForGoodRun(args, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);


    Schema errorSchema =
      Schema.recordOf("erroritem",
                      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                      Schema.Field.of("errMsg", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("errCode", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                      Schema.Field.of("errStage", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    StructuredRecord expectedError = StructuredRecord.builder(errorSchema)
      .set("id", 0)
      .set("errMsg", IntValueFilterTransform.ERROR_MESSAGE)
      .set("errCode", IntValueFilterTransform.ERROR_CODE)
      .set("errStage", "filter0")
      .build();

    Set<StructuredRecord> sink1Expected = Collections.singleton(item3);
    Set<StructuredRecord> sink2Expected = new HashSet<>(Arrays.asList(itemNull, item1, item2));
    Set<StructuredRecord> sink3Expected = Collections.singleton(expectedError);
    Set<StructuredRecord> sink4Expected = new HashSet<>(Arrays.asList(item0, item2));
    Set<StructuredRecord> sink5Expected = new HashSet<>(Arrays.asList(item0, item1));

    Assert.assertEquals(sink1Expected, new HashSet<>(MockExternalSink.readOutput(output1, schema)));
    Assert.assertEquals(sink2Expected, new HashSet<>(MockExternalSink.readOutput(output2, schema)));
    Assert.assertEquals(sink3Expected, new HashSet<>(MockExternalSink.readOutput(output3, errorSchema)));
    Assert.assertEquals(sink4Expected, new HashSet<>(MockExternalSink.readOutput(output4, schema)));
    Assert.assertEquals(sink5Expected, new HashSet<>(MockExternalSink.readOutput(output5, schema)));

    // check output
    validateMetric(2, appId, "nullSplitter.records.in");
    validateMetric(1, appId, "nullSplitter.records.out.null");
    validateMetric(1, appId, "nullSplitter.records.out.non-null");
    validateMetric(2, appId, "filter0.records.out");
    validateMetric(1, appId, "filter0.records.error");
    validateMetric(3, appId, "identityAggregator.records.in");
    validateMetric(3, appId, "identityAggregator.records.out");
  }

  private long getMetric(ApplicationId appId, String metric) {
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName(),
                                               Constants.Metrics.Tag.WORKFLOW, SmartWorkflow.NAME);
    return getMetricsManager().getTotalMetric(tags, "user." + metric);
  }

  /* Tests upgrade for a deployed application.
     1. Deploy an application with older application artifact (1.0.0) and older filter plugin version (1.0.0).
     2. Add new versions of application artifacts (0.0.9, 1.1.0, 1.2.0) and filter plugin artifacts (1.0.5, 1.1.0) in
        SYSTEM scope (in test class setup).
     3. Upgrade the older deployed application.
     4. Verify that after upgrading, application artifact and filter plugin artifact is upgraded to use latest version
        in its config.
   */
  @Test
  public void testSimpleUpgradePipelines() throws Exception {
    ArtifactSelectorConfig currentArtifactSelector =
      new ArtifactSelectorConfig(ArtifactScope.USER.name(), "test-plugins", "1.0.0");

    Engine engine = Engine.MAPREDUCE;
    String sourceName = "testSource" + engine.name();
    String sinkName = "testSink" + engine.name();
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName)))
      .addStage(new ETLStage("filter", PluggableFilterTransform.getPlugin(
        ValueFilter.NAME, ValueFilter.getProperties("${field}", "${value}"), currentArtifactSelector)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addConnection("source", "filter")
      .addConnection("filter", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("sparkProgramTest");
    // Deploy app with artifact version 1.0.0.
    ApplicationManager appManager = deployApplication(appId, appRequest);
    ApplicationDetail oldAppDetail = getAppDetail(appId);
    ETLBatchConfig oldBatchConfig = GSON.fromJson(oldAppDetail.getConfiguration(), ETLBatchConfig.class);
    Map<String, ETLStage> oldStageMap = oldBatchConfig.getStages().stream().collect(
      Collectors.toMap(ETLStage::getName, e -> e));

    // Upgrade application.
    appManager.upgrade();

    ApplicationDetail upgradedAppDetail = getAppDetail(appId);
    ETLBatchConfig newBatchConfig = GSON.fromJson(upgradedAppDetail.getConfiguration(), ETLBatchConfig.class);
    Map<String, ETLStage> newStageMap = newBatchConfig.getStages().stream().collect(
      Collectors.toMap(ETLStage::getName, e -> e));

    // Compare stages that should be same after upgrade.
    Assert.assertEquals(oldStageMap.get("source"), newStageMap.get("source"));
    Assert.assertEquals(oldStageMap.get("sink"), newStageMap.get("sink"));

    // Verify that after upgrade, application upgrades artifact version to latest version available.
    Assert.assertEquals(UPGRADE_APP_ARTIFACT_ID_2.getVersion(), upgradedAppDetail.getArtifact().getVersion());
    // Check if the filter stage, for which version should be upgraded to desired version in SYSTEM scope.
    ETLPlugin upgradedPlugin = newStageMap.get("filter").getPlugin();
    Assert.assertEquals(upgradedPlugin.getArtifactConfig().getVersion(), "1.1.0");
    Assert.assertEquals(ArtifactScope.valueOf(upgradedPlugin.getArtifactConfig().getScope().toUpperCase()),
                        ArtifactScope.SYSTEM);
  }

  /* Tests upgrade for a deployed application. Also tests that SNAPSHOT artifacts are being considered for upgrade.
     1. Deploy an application with older application artifact (1.0.0) and older filter plugin version (1.0.0).
     2. Add new versions of application artifact (0.0.9, 1.1.0, 1.2.0) and filter plugin artifacts (1.0.5, 1.1.0).
     3. Also deploy a snapshot version of app artifact 1.3.0-SNAPSHOT and plugin artifact 1.1.1-SNAPSHOT bind to it.
     3. Upgrade the older deployed application.
     4. Verify that after upgrading, application artifact and filter plugin artifact is upgraded to use latest version
        in its config and it uses snapshot versions for both.
   */
  @Test
  public void testSimpleUpgradePipelinesWithSnapshotArtifact() throws Exception {
    ArtifactSelectorConfig currentArtifactSelector =
      new ArtifactSelectorConfig(ArtifactScope.USER.name(), "test-plugins", "1.0.0");

    Engine engine = Engine.MAPREDUCE;
    String sourceName = "testSource" + engine.name();
    String sinkName = "testSink" + engine.name();
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName)))
      .addStage(new ETLStage("filter", PluggableFilterTransform.getPlugin(
        ValueFilter.NAME, ValueFilter.getProperties("${field}", "${value}"), currentArtifactSelector)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addConnection("source", "filter")
      .addConnection("filter", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("sparkProgramTest");
    // Deploy app with artifact version 1.0.0.
    ApplicationManager appManager = deployApplication(appId, appRequest);
    ApplicationDetail oldAppDetail = getAppDetail(appId);
    ETLBatchConfig oldBatchConfig = GSON.fromJson(oldAppDetail.getConfiguration(), ETLBatchConfig.class);
    Map<String, ETLStage> oldStageMap = oldBatchConfig.getStages().stream().collect(
      Collectors.toMap(ETLStage::getName, e -> e));

    // Upgrade application with allowSnapshot set to true.
    appManager.upgrade(Collections.emptySet(), true);

    ApplicationDetail upgradedAppDetail = getAppDetail(appId);
    ETLBatchConfig newBatchConfig = GSON.fromJson(upgradedAppDetail.getConfiguration(), ETLBatchConfig.class);
    Map<String, ETLStage> newStageMap = newBatchConfig.getStages().stream().collect(
      Collectors.toMap(ETLStage::getName, e -> e));

    // Compare stages that should be same after upgrade.
    Assert.assertEquals(oldStageMap.get("source"), newStageMap.get("source"));
    Assert.assertEquals(oldStageMap.get("sink"), newStageMap.get("sink"));

    // Verify that after upgrade, application upgrades artifact version to latest version available.
    Assert.assertEquals(UPGRADE_APP_ARTIFACT_ID_3_SNAPSHOT.getVersion(), upgradedAppDetail.getArtifact().getVersion());
    // Check if the filter stage, for which version should be upgraded to desired version in SYSTEM scope.
    ETLPlugin upgradedPlugin = newStageMap.get("filter").getPlugin();
    Assert.assertEquals(upgradedPlugin.getArtifactConfig().getVersion(), "1.1.1-SNAPSHOT");
    Assert.assertEquals(ArtifactScope.valueOf(upgradedPlugin.getArtifactConfig().getScope().toUpperCase()),
                        ArtifactScope.USER);
  }

    /* Tests upgrade for a deployed application. Also tests artifact scope parameter for only considering artifacts in
       a given scope.
     1. Deploy an application with older application artifact (1.0.0) and older filter plugin version (1.0.0).
     2. Add new versions of application artifacts (0.0.9, 1.1.0, 1.2.0) and filter plugin artifacts (1.0.5, 1.1.0) in
        SYSTEM scope (in test class setup).
     3. Also deploy a snapshot version of plugin artifact 1.0.8 in USER scope.
     3. Upgrade the older deployed application with artifact scope set to USER for upgrade.
     4. Verify that after upgrading, application artifact and filter plugin artifact is upgraded to use latest version
        in its config and it uses snapshot plugin version with 1.0.8 from USER scope.
   */
  @Test
  public void testSimpleUpgradePipelinesWithArtifactScope() throws Exception {
    ArtifactSelectorConfig currentArtifactSelector =
      new ArtifactSelectorConfig(ArtifactScope.USER.name(), "test-plugins", "1.0.0");

    Engine engine = Engine.MAPREDUCE;
    String sourceName = "testSource" + engine.name();
    String sinkName = "testSink" + engine.name();
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName)))
      .addStage(new ETLStage("filter", PluggableFilterTransform.getPlugin(
        ValueFilter.NAME, ValueFilter.getProperties("${field}", "${value}"), currentArtifactSelector)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addConnection("source", "filter")
      .addConnection("filter", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("sparkProgramTest");
    // Deploy app with artifact version 1.0.0.
    ApplicationManager appManager = deployApplication(appId, appRequest);
    ApplicationDetail oldAppDetail = getAppDetail(appId);
    ETLBatchConfig oldBatchConfig = GSON.fromJson(oldAppDetail.getConfiguration(), ETLBatchConfig.class);
    Map<String, ETLStage> oldStageMap = oldBatchConfig.getStages().stream().collect(
      Collectors.toMap(ETLStage::getName, e -> e));

    // Upgrade application with artifact scope as USER.
    appManager.upgrade(Collections.singleton(ArtifactScope.USER.toString()), false);

    ApplicationDetail upgradedAppDetail = getAppDetail(appId);
    ETLBatchConfig newBatchConfig = GSON.fromJson(upgradedAppDetail.getConfiguration(), ETLBatchConfig.class);
    Map<String, ETLStage> newStageMap = newBatchConfig.getStages().stream().collect(
      Collectors.toMap(ETLStage::getName, e -> e));

    // Compare stages that should be same after upgrade.
    Assert.assertEquals(oldStageMap.get("source"), newStageMap.get("source"));
    Assert.assertEquals(oldStageMap.get("sink"), newStageMap.get("sink"));

    // Verify that after upgrade, application upgrades artifact version to latest version available.
    Assert.assertEquals(UPGRADE_APP_ARTIFACT_ID_2.getVersion(), upgradedAppDetail.getArtifact().getVersion());
    // Check if the filter stage, for which version should be upgraded to desired version in SYSTEM scope.
    ETLPlugin upgradedPlugin = newStageMap.get("filter").getPlugin();
    Assert.assertEquals("1.0.8", upgradedPlugin.getArtifactConfig().getVersion());
    Assert.assertEquals(ArtifactScope.valueOf(upgradedPlugin.getArtifactConfig().getScope().toUpperCase()),
                        ArtifactScope.USER);
  }

  /* Tests upgrade for a deployed application with a plugin using plugin range.
   1. Deploy an application with older application artifact (1.0.0) and older filter plugin version with range
      [1.0.0-1.0.5).
   2. Add new versions of application artifacts (0.0.9, 1.1.0, 1.2.0) and filter plugin artifacts (1.0.5, 1.1.0) in
      SYSTEM scope (in test class setup).
   3. Upgrade the older deployed application.
   4. Verify that after upgrading, application artifact and filter plugin artifact is upgraded to use latest version
      in its config. Also verify that plugin version range for filter stage is changed to use newest version of plugin.
 */
  @Test
  public void testUpgradePipelinesWithPluginRange() throws Exception {
    ArtifactSelectorConfig currentArtifactSelector =
      new ArtifactSelectorConfig(ArtifactScope.USER.name(), "test-plugins", "[1.0.0,1.0.5)");

    Engine engine = Engine.MAPREDUCE;
    String sourceName = "testSource" + engine.name();
    String sinkName = "testSink" + engine.name();
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName)))
      .addStage(new ETLStage("filter", PluggableFilterTransform.getPlugin(
        ValueFilter.NAME, ValueFilter.getProperties("${field}", "${value}"), currentArtifactSelector)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addConnection("source", "filter")
      .addConnection("filter", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("sparkProgramTest");
    // Deploy app with artifact version 1.0.0.
    ApplicationManager appManager = deployApplication(appId, appRequest);
    ApplicationDetail oldAppDetail = getAppDetail(appId);
    ETLBatchConfig oldBatchConfig = GSON.fromJson(oldAppDetail.getConfiguration(), ETLBatchConfig.class);
    Map<String, ETLStage> oldStageMap = oldBatchConfig.getStages().stream().collect(
      Collectors.toMap(ETLStage::getName, e -> e));

    // Upgrade application.
    appManager.upgrade();

    ApplicationDetail upgradedAppDetail = getAppDetail(appId);
    ETLBatchConfig newBatchConfig = GSON.fromJson(upgradedAppDetail.getConfiguration(), ETLBatchConfig.class);
    Map<String, ETLStage> newStageMap = newBatchConfig.getStages().stream().collect(
      Collectors.toMap(ETLStage::getName, e -> e));

    // Compare stages that should be same after upgrade.
    Assert.assertEquals(oldStageMap.get("source"), newStageMap.get("source"));
    Assert.assertEquals(oldStageMap.get("sink"), newStageMap.get("sink"));

    // Verify that after upgrade, application upgrades artifact version to latest version available.
    Assert.assertEquals(upgradedAppDetail.getArtifact().getVersion(), UPGRADE_APP_ARTIFACT_ID_2.getVersion());
    // Check if the filter stage, for which version range should be upgraded to include latest plugin version in SYSTEM
    // scope.
    ETLPlugin upgradedPlugin = newStageMap.get("filter").getPlugin();
    Assert.assertEquals(upgradedPlugin.getArtifactConfig().getVersion(), "[1.0.0,1.1.0]");
    Assert.assertEquals(ArtifactScope.valueOf(upgradedPlugin.getArtifactConfig().getScope().toUpperCase()),
                        ArtifactScope.SYSTEM);
  }

  /* Tests upgrade for a deployed application with a plugin using plugin range.
   1. Deploy an application with older application artifact (1.0.0) and filter plugin version with range
      [1.0.0-2.0.0) to make sure latest version of plugin should be included in it.
   2. Add new versions of application artifacts (0.0.9, 1.1.0, 1.2.0) and filter plugin artifacts (1.0.5, 1.1.0) in
      SYSTEM scope (in test class setup).
   3. Upgrade the older deployed application.
   4. Verify that after upgrading, application artifact uses latest version in its config.
      But plugin range is not updated as latest version of plugin is still included in the range.
  */
  @Test
  public void testUpgradePipelinesWithNoChangeInPluginRange() throws Exception {
    ArtifactSelectorConfig currentArtifactSelector =
      new ArtifactSelectorConfig(ArtifactScope.USER.name(), "test-plugins", "[1.0.0,2.0.0)");

    Engine engine = Engine.MAPREDUCE;
    String sourceName = "testSource" + engine.name();
    String sinkName = "testSink" + engine.name();
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName)))
      .addStage(new ETLStage("filter", PluggableFilterTransform.getPlugin(
        ValueFilter.NAME, ValueFilter.getProperties("${field}", "${value}"), currentArtifactSelector)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addConnection("source", "filter")
      .addConnection("filter", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("sparkProgramTest");
    // Deploy app with artifact version 1.0.0.
    ApplicationManager appManager = deployApplication(appId, appRequest);
    ApplicationDetail oldAppDetail = getAppDetail(appId);
    ETLBatchConfig oldBatchConfig = GSON.fromJson(oldAppDetail.getConfiguration(), ETLBatchConfig.class);
    Map<String, ETLStage> oldStageMap = oldBatchConfig.getStages().stream().collect(
      Collectors.toMap(ETLStage::getName, e -> e));

    // Upgrade application with consider snapshot enabled to consider highest version of plugin available which is
    // 1.1.1-SNAPSHOT.
    appManager.upgrade(Collections.emptySet(), true);

    ApplicationDetail upgradedAppDetail = getAppDetail(appId);
    ETLBatchConfig newBatchConfig = GSON.fromJson(upgradedAppDetail.getConfiguration(), ETLBatchConfig.class);
    Map<String, ETLStage> newStageMap = newBatchConfig.getStages().stream().collect(
      Collectors.toMap(ETLStage::getName, e -> e));

    // Compare stages that should be same after upgrade.
    Assert.assertEquals(oldStageMap.get("source"), newStageMap.get("source"));
    Assert.assertEquals(oldStageMap.get("sink"), newStageMap.get("sink"));
    // The filter stage is not changed as latest plugin is in the range.
    Assert.assertEquals(oldStageMap.get("filter"), newStageMap.get("filter"));

    // Verify that after upgrade, application upgrades artifact version to latest version available.
    Assert.assertEquals(upgradedAppDetail.getArtifact().getVersion(), UPGRADE_APP_ARTIFACT_ID_3_SNAPSHOT.getVersion());
  }
}
