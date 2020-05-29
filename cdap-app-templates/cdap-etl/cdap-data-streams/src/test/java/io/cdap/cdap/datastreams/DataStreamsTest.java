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

package io.cdap.cdap.datastreams;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.metadata.lineage.Lineage;
import io.cdap.cdap.data2.metadata.lineage.Relation;
import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.mock.alert.NullAlertTransform;
import io.cdap.cdap.etl.mock.alert.TMSAlertPublisher;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.aggregator.FieldCountAggregator;
import io.cdap.cdap.etl.mock.batch.aggregator.FieldCountReducibleAggregator;
import io.cdap.cdap.etl.mock.batch.aggregator.GroupFilterAggregator;
import io.cdap.cdap.etl.mock.batch.joiner.DupeFlagger;
import io.cdap.cdap.etl.mock.batch.joiner.MockAutoJoiner;
import io.cdap.cdap.etl.mock.batch.joiner.MockJoiner;
import io.cdap.cdap.etl.mock.spark.Window;
import io.cdap.cdap.etl.mock.spark.compute.StringValueFilterCompute;
import io.cdap.cdap.etl.mock.spark.streaming.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.mock.transform.FilterErrorTransform;
import io.cdap.cdap.etl.mock.transform.FlattenErrorTransform;
import io.cdap.cdap.etl.mock.transform.IdentityTransform;
import io.cdap.cdap.etl.mock.transform.NullFieldSplitterTransform;
import io.cdap.cdap.etl.mock.transform.SleepTransform;
import io.cdap.cdap.etl.mock.transform.StringValueFilterTransform;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.metadata.DatasetFieldLineageSummary;
import io.cdap.cdap.metadata.FieldLineageAdmin;
import io.cdap.cdap.metadata.FieldRelation;
import io.cdap.cdap.metadata.LineageAdmin;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.MetricsManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestConfiguration;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class DataStreamsTest extends HydratorTestBase {
  private static final Gson GSON = new Gson();
  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");
  private static String checkpointDir;
  private static int startCount = 0;
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
    checkpointDir = "file://" + TMP_FOLDER.getRoot().toPath().toString();
  }

  @Test
  public void testLineageWithMacros() throws Exception {
    Schema schema = Schema.recordOf(
      "test",
      Schema.Field.of("key", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("value", Schema.of(Schema.Type.STRING))
    );

    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(schema).set("key", "key1").set("value", "value1").build(),
      StructuredRecord.builder(schema).set("key", "key2").set("value", "value2").build());

    String srcName = "lineageSource";
    String sinkName1 = "lineageOutput1";
    String sinkName2 = "lineageOutput2";

    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(schema, input, 0L, srcName)))
      .addStage(new ETLStage("sink", MockSink.getPlugin("${output}")))
      .addStage(new ETLStage("identity", IdentityTransform.getPlugin()))
      .addConnection("source", "identity")
      .addConnection("identity", "sink")
      .setCheckpointDir(checkpointDir)
      .setBatchInterval("1s")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("lineageApp");
    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    ProgramId spark = appId.spark(DataStreamsSparkLauncher.NAME);

    RunId runId = testLineageWithMacro(appManager, new HashSet<>(input), sinkName1);

    FieldLineageAdmin fieldAdmin = getFieldLineageAdmin();
    LineageAdmin lineageAdmin = getLineageAdmin();

    // wait for the lineage get populated
    Tasks.waitFor(true, () -> {
      Lineage dsLineage = lineageAdmin.computeLineage(NamespaceId.DEFAULT.dataset(srcName),
                                                      0, System.currentTimeMillis(), 1, "workflow");
      DatasetFieldLineageSummary fll =
        fieldAdmin.getDatasetFieldLineage(Constants.FieldLineage.Direction.BOTH, EndPoint.of("default", srcName),
                                          0, System.currentTimeMillis());
      return dsLineage.getRelations().size() == 2 && !fll.getOutgoing().isEmpty();
    }, 10, TimeUnit.SECONDS);

    Lineage lineage = lineageAdmin.computeLineage(NamespaceId.DEFAULT.dataset(srcName),
                                                  0, System.currentTimeMillis(), 1, "workflow");

    Set<Relation> expectedLineage =
      ImmutableSet.of(new Relation(NamespaceId.DEFAULT.dataset(srcName), spark, AccessType.READ, runId),
                      new Relation(NamespaceId.DEFAULT.dataset(sinkName1), spark, AccessType.WRITE, runId));
    Assert.assertEquals(expectedLineage, lineage.getRelations());

    DatasetFieldLineageSummary summary =
      fieldAdmin.getDatasetFieldLineage(Constants.FieldLineage.Direction.BOTH, EndPoint.of("default", srcName),
                                        0, System.currentTimeMillis());
    Assert.assertEquals(NamespaceId.DEFAULT.dataset(srcName), summary.getDatasetId());
    Assert.assertEquals(ImmutableSet.of("key", "value"), summary.getFields());
    Assert.assertTrue(summary.getIncoming().isEmpty());
    Set<DatasetFieldLineageSummary.FieldLineageRelations> outgoing = summary.getOutgoing();
    Assert.assertEquals(1, outgoing.size());

    Set<DatasetFieldLineageSummary.FieldLineageRelations> expectedRelations =
      Collections.singleton(
        new DatasetFieldLineageSummary.FieldLineageRelations(NamespaceId.DEFAULT.dataset(sinkName1), 2,
                                                             ImmutableSet.of(new FieldRelation("key", "key"),
                                                                             new FieldRelation("value", "value"))));
    Assert.assertEquals(expectedRelations, outgoing);

    // here sleep for 1 seconds to start the second run because the dataset lineage is storing based on unit second
    TimeUnit.SECONDS.sleep(1);
    long startTimeMillis = System.currentTimeMillis();
    runId = testLineageWithMacro(appManager, new HashSet<>(input), sinkName2);

    // wait for the lineage get populated
    Tasks.waitFor(true, () -> {
      Lineage dsLineage = lineageAdmin.computeLineage(NamespaceId.DEFAULT.dataset(srcName),
                                                      startTimeMillis, System.currentTimeMillis(), 1, "workflow");
      long end = System.currentTimeMillis();
      DatasetFieldLineageSummary fll =
        fieldAdmin.getDatasetFieldLineage(Constants.FieldLineage.Direction.BOTH, EndPoint.of("default", srcName),
                                          startTimeMillis, end);
      return dsLineage.getRelations().size() == 2 && !fll.getOutgoing().isEmpty();
    }, 10, TimeUnit.SECONDS);

    lineage = lineageAdmin.computeLineage(NamespaceId.DEFAULT.dataset(srcName),
                                          startTimeMillis, System.currentTimeMillis(), 1, "workflow");

    expectedLineage =
      ImmutableSet.of(new Relation(NamespaceId.DEFAULT.dataset(srcName), spark, AccessType.READ, runId),
                      new Relation(NamespaceId.DEFAULT.dataset(sinkName2), spark, AccessType.WRITE, runId));
    Assert.assertEquals(expectedLineage, lineage.getRelations());

    summary =
      fieldAdmin.getDatasetFieldLineage(Constants.FieldLineage.Direction.BOTH, EndPoint.of("default", srcName),
                                        startTimeMillis, System.currentTimeMillis());
    Assert.assertEquals(NamespaceId.DEFAULT.dataset(srcName), summary.getDatasetId());
    Assert.assertEquals(ImmutableSet.of("key", "value"), summary.getFields());
    Assert.assertTrue(summary.getIncoming().isEmpty());
    outgoing = summary.getOutgoing();
    Assert.assertEquals(1, outgoing.size());

    expectedRelations =
      Collections.singleton(
        new DatasetFieldLineageSummary.FieldLineageRelations(NamespaceId.DEFAULT.dataset(sinkName2), 2, 
                                                             ImmutableSet.of(new FieldRelation("key", "key"),
                                                                             new FieldRelation("value", "value"))));
    Assert.assertEquals(expectedRelations, outgoing);
  }

  private RunId testLineageWithMacro(ApplicationManager appManager,
                                    Set<StructuredRecord> expected, String outputName) throws Exception {
    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start(Collections.singletonMap("output", outputName));
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    // since dataset name is a macro, the dataset isn't created until it is needed. Wait for it to exist
    Tasks.waitFor(true, () -> getDataset(outputName).get() != null, 1, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputName);
    Tasks.waitFor(
      true,
      () -> {
        outputManager.flush();
        Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(outputManager));
        return expected.equals(outputRecords);
      },
      1,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStopped(10, TimeUnit.SECONDS);
    return RunIds.fromString(sparkManager.getHistory().iterator().next().getPid());
  }

  @Test
  public void testTransformComputeWithMacros() throws Exception {
    Schema schema = Schema.recordOf(
      "test",
      Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );
    List<StructuredRecord> input = new ArrayList<>();
    StructuredRecord samuelRecord = StructuredRecord.builder(schema).set("id", "123").set("name", "samuel").build();
    StructuredRecord jacksonRecord = StructuredRecord.builder(schema).set("id", "456").set("name", "jackson").build();
    StructuredRecord dwayneRecord = StructuredRecord.builder(schema).set("id", "789").set("name", "dwayne").build();
    StructuredRecord johnsonRecord = StructuredRecord.builder(schema).set("id", "0").set("name", "johnson").build();
    input.add(samuelRecord);
    input.add(jacksonRecord);
    input.add(dwayneRecord);
    input.add(johnsonRecord);

    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(schema, input)))
      .addStage(new ETLStage("sink", MockSink.getPlugin("${output}")))
      .addStage(new ETLStage("filter1", StringValueFilterTransform.getPlugin("${field}", "${val1}")))
      .addStage(new ETLStage("filter2", StringValueFilterCompute.getPlugin("${field}", "${val2}")))
      .addStage(new ETLStage("sleep", SleepTransform.getPlugin(2L)))
      .addConnection("source", "sleep")
      .addConnection("sleep", "filter1")
      .addConnection("filter1", "filter2")
      .addConnection("filter2", "sink")
      .setBatchInterval("1s")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("simpleApp");
    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(samuelRecord);
    expected.add(jacksonRecord);

    testTransformComputeRun(appManager, expected, "dwayne", "johnson", "macroOutput1");
    validateMetric(appId, "source.records.out", 4);
    validateMetric(appId, "sleep.records.in", 4);
    validateMetric(appId, "sleep.records.out", 4);
    validateMetric(appId, "filter1.records.in", 4);
    validateMetric(appId, "filter1.records.out", 3);
    validateMetric(appId, "filter2.records.in", 3);
    validateMetric(appId, "filter2.records.out", 2);
    validateMetric(appId, "sink.records.in", 2);
    Assert.assertTrue(getMetric(appId, "sleep." + io.cdap.cdap.etl.common.Constants.Metrics.TOTAL_TIME) > 0L);

    expected.clear();
    expected.add(dwayneRecord);
    expected.add(johnsonRecord);
    testTransformComputeRun(appManager, expected, "samuel", "jackson", "macroOutput2");
  }

  private void testTransformComputeRun(ApplicationManager appManager, final Set<StructuredRecord> expected,
                                       String val1, String val2, final String outputName) throws Exception {
    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start(ImmutableMap.of("field", "name", "val1", val1, "val2", val2, "output", outputName));
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    // since dataset name is a macro, the dataset isn't created until it is needed. Wait for it to exist
    Tasks.waitFor(true, () -> getDataset(outputName).get() != null, 1, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputName);
    Tasks.waitFor(
      true,
      () -> {
        outputManager.flush();
        Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(outputManager));
        return expected.equals(outputRecords);
      },
      1,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStopped(10, TimeUnit.SECONDS);
  }

  @Test
  public void testAggregatorJoinerMacrosWithCheckpoints() throws Exception {
    testAggregatorJoinerMacrosWithCheckpoints(false);
    testAggregatorJoinerMacrosWithCheckpoints(true);
  }

  private void testAggregatorJoinerMacrosWithCheckpoints(boolean isReducibleAggregator) throws Exception {
    /*
                 |--> aggregator --> sink1
        users1 --|
                 |----|
                      |--> dupeFlagger --> sink2
        users2 -------|
     */
    Schema userSchema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));


    List<StructuredRecord> users1 = ImmutableList.of(
      StructuredRecord.builder(userSchema).set("id", 1L).set("name", "Samuel").build(),
      StructuredRecord.builder(userSchema).set("id", 2L).set("name", "Dwayne").build(),
      StructuredRecord.builder(userSchema).set("id", 3L).set("name", "Terry").build());

    List<StructuredRecord> users2 = ImmutableList.of(
      StructuredRecord.builder(userSchema).set("id", 1L).set("name", "Samuel").build(),
      StructuredRecord.builder(userSchema).set("id", 2L).set("name", "Dwayne").build(),
      StructuredRecord.builder(userSchema).set("id", 4L).set("name", "Terry").build(),
      StructuredRecord.builder(userSchema).set("id", 5L).set("name", "Christopher").build());

    DataStreamsConfig pipelineConfig = DataStreamsConfig.builder()
      .setBatchInterval("5s")
      .addStage(new ETLStage("users1", MockSource.getPlugin(userSchema, users1)))
      .addStage(new ETLStage("users2", MockSource.getPlugin(userSchema, users2)))
      .addStage(new ETLStage("sink1", MockSink.getPlugin("sink1")))
      .addStage(new ETLStage("sink2", MockSink.getPlugin("sink2")))
      .addStage(new ETLStage("aggregator", isReducibleAggregator ?
        FieldCountReducibleAggregator.getPlugin("${aggfield}", "${aggType}") :
        FieldCountAggregator.getPlugin("${aggfield}", "${aggType}")))
      .addStage(new ETLStage("dupeFlagger", DupeFlagger.getPlugin("users1", "${flagField}")))
      .addConnection("users1", "aggregator")
      .addConnection("aggregator", "sink1")
      .addConnection("users1", "dupeFlagger")
      .addConnection("users2", "dupeFlagger")
      .addConnection("dupeFlagger", "sink2")
      .setCheckpointDir(checkpointDir)
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, pipelineConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ParallelAggJoinApp" + isReducibleAggregator);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // run it once with this set of macros
    Map<String, String> arguments = new HashMap<>();
    arguments.put("aggfield", "id");
    arguments.put("aggType", "long");
    arguments.put("flagField", "isDupe");

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start(arguments);
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    DataSetManager<Table> sink1 = getDataset("sink1");
    DataSetManager<Table> sink2 = getDataset("sink2");

    Schema aggSchema = Schema.recordOf(
      "id.count",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("ct", Schema.of(Schema.Type.LONG)));
    Set<StructuredRecord> expectedAggregates = ImmutableSet.of(
      StructuredRecord.builder(aggSchema).set("id", 0L).set("ct", 3L).build(),
      StructuredRecord.builder(aggSchema).set("id", 1L).set("ct", 1L).build(),
      StructuredRecord.builder(aggSchema).set("id", 2L).set("ct", 1L).build(),
      StructuredRecord.builder(aggSchema).set("id", 3L).set("ct", 1L).build());

    Schema outputSchema = Schema.recordOf(
      "user.flagged",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("isDupe", Schema.of(Schema.Type.BOOLEAN)));
    Set<StructuredRecord> expectedJoined = ImmutableSet.of(
      StructuredRecord.builder(outputSchema).set("id", 1L).set("name", "Samuel").set("isDupe", true).build(),
      StructuredRecord.builder(outputSchema).set("id", 2L).set("name", "Dwayne").set("isDupe", true).build(),
      StructuredRecord.builder(outputSchema).set("id", 3L).set("name", "Terry").set("isDupe", false).build());

    Tasks.waitFor(
      true,
      () -> {
        sink1.flush();
        sink2.flush();
        Set<StructuredRecord> actualAggs = new HashSet<>(MockSink.readOutput(sink1));
        Set<StructuredRecord> actualJoined = new HashSet<>(MockSink.readOutput(sink2));
        return expectedAggregates.equals(actualAggs) && expectedJoined.equals(actualJoined);
      },
      1,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStopped(30, TimeUnit.SECONDS);

    MockSink.clear(sink1);
    MockSink.clear(sink2);

    // run it again with different macros to make sure they are re-evaluated and not stored in the checkpoint
    arguments = new HashMap<>();
    arguments.put("aggfield", "name");
    arguments.put("aggType", "string");
    arguments.put("flagField", "dupe");

    sparkManager.start(arguments);
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    aggSchema = Schema.recordOf(
      "name.count",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("ct", Schema.of(Schema.Type.LONG)));
    Set<StructuredRecord> expectedAggregates2 = ImmutableSet.of(
      StructuredRecord.builder(aggSchema).set("name", "all").set("ct", 3L).build(),
      StructuredRecord.builder(aggSchema).set("name", "Samuel").set("ct", 1L).build(),
      StructuredRecord.builder(aggSchema).set("name", "Dwayne").set("ct", 1L).build(),
      StructuredRecord.builder(aggSchema).set("name", "Terry").set("ct", 1L).build());

    outputSchema = Schema.recordOf(
      "user.flagged",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("dupe", Schema.of(Schema.Type.BOOLEAN)));
    Set<StructuredRecord> expectedJoined2 = ImmutableSet.of(
      StructuredRecord.builder(outputSchema).set("id", 1L).set("name", "Samuel").set("dupe", true).build(),
      StructuredRecord.builder(outputSchema).set("id", 2L).set("name", "Dwayne").set("dupe", true).build(),
      StructuredRecord.builder(outputSchema).set("id", 3L).set("name", "Terry").set("dupe", false).build());

    Tasks.waitFor(
      true,
      () -> {
        sink1.flush();
        sink2.flush();
        Set<StructuredRecord> actualAggs = new HashSet<>(MockSink.readOutput(sink1));
        Set<StructuredRecord> actualJoined = new HashSet<>(MockSink.readOutput(sink2));
        return expectedAggregates2.equals(actualAggs) && expectedJoined2.equals(actualJoined);
      },
      1,
      TimeUnit.MINUTES);

    sparkManager.stop();

    MockSink.clear(sink1);
    MockSink.clear(sink2);
  }

  @Test
  public void testParallelAggregators() throws Exception {
    testParallelAggregators(false);
    testParallelAggregators(true);
  }

  private void testParallelAggregators(boolean isReducibleAggregator) throws Exception {
    String sink1Name = "pAggOutput1-" + isReducibleAggregator;
    String sink2Name = "pAggOutput2-" + isReducibleAggregator;

    Schema inputSchema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.LONG))
    );

    List<StructuredRecord> input1 = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("user", "samuel").set("item", 1L).build(),
      StructuredRecord.builder(inputSchema).set("user", "samuel").set("item", 2L).build());

    List<StructuredRecord> input2 = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("user", "samuel").set("item", 3L).build(),
      StructuredRecord.builder(inputSchema).set("user", "john").set("item", 4L).build(),
      StructuredRecord.builder(inputSchema).set("user", "john").set("item", 3L).build());

    /*
       source1 --|--> agg1 --> sink1
                 |
       source2 --|--> agg2 --> sink2
     */
    DataStreamsConfig pipelineConfig = DataStreamsConfig.builder()
      .setBatchInterval("5s")
      .addStage(new ETLStage("source1", MockSource.getPlugin(inputSchema, input1)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(inputSchema, input2)))
      .addStage(new ETLStage("sink1", MockSink.getPlugin(sink1Name)))
      .addStage(new ETLStage("sink2", MockSink.getPlugin(sink2Name)))
      .addStage(new ETLStage("agg1", isReducibleAggregator ?
        FieldCountReducibleAggregator.getPlugin("user", "string") : FieldCountAggregator.getPlugin("user", "string")))
      .addStage(new ETLStage("agg2", isReducibleAggregator ?
        FieldCountReducibleAggregator.getPlugin("item", "long") : FieldCountAggregator.getPlugin("item", "long")))
      .addConnection("source1", "agg1")
      .addConnection("source1", "agg2")
      .addConnection("source2", "agg1")
      .addConnection("source2", "agg2")
      .addConnection("agg1", "sink1")
      .addConnection("agg2", "sink2")
      .setCheckpointDir(checkpointDir)
      .disableCheckpoints()
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, pipelineConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ParallelAggApp" + isReducibleAggregator);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

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
    DataSetManager<Table> sinkManager1 = getDataset(sink1Name);
    Set<StructuredRecord> expected1 = ImmutableSet.of(
      StructuredRecord.builder(outputSchema1).set("user", "all").set("ct", 5L).build(),
      StructuredRecord.builder(outputSchema1).set("user", "samuel").set("ct", 3L).build(),
      StructuredRecord.builder(outputSchema1).set("user", "john").set("ct", 2L).build());

    Tasks.waitFor(
      true,
      () -> {
        sinkManager1.flush();
        Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(sinkManager1));
        return expected1.equals(outputRecords);
      },
      1,
      TimeUnit.MINUTES);

    DataSetManager<Table> sinkManager2 = getDataset(sink2Name);
    Set<StructuredRecord> expected2 = ImmutableSet.of(
      StructuredRecord.builder(outputSchema2).set("item", 0L).set("ct", 5L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 1L).set("ct", 1L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 2L).set("ct", 1L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 3L).set("ct", 2L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 4L).set("ct", 1L).build());

    Tasks.waitFor(
      true,
      () -> {
        sinkManager2.flush();
        Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(sinkManager2));
        return expected2.equals(outputRecords);
      },
      1,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStopped(10, TimeUnit.SECONDS);

    validateMetric(appId, "source1.records.out", 2);
    validateMetric(appId, "source2.records.out", 3);
    validateMetric(appId, "agg1.records.in", 5);
    validateMetric(appId, "agg1.records.out", 3);
    validateMetric(appId, "agg2.records.in", 5);
    validateMetric(appId, "agg2.records.out", 5);
    validateMetric(appId, "sink1.records.in", 3);
    validateMetric(appId, "sink2.records.in", 5);
  }

  @Test
  public void testWindower() throws Exception {
    /*
     * source --> window(width=10,interval=1) --> aggregator --> filter --> sink
     */
    Schema schema = Schema.recordOf("data", Schema.Field.of("x", Schema.of(Schema.Type.STRING)));
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(schema).set("x", "abc").build(),
      StructuredRecord.builder(schema).set("x", "abc").build(),
      StructuredRecord.builder(schema).set("x", "abc").build());

    String sinkName = "windowOut";
    // source sleeps 1 second between outputs
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(schema, input, 1000L)))
      .addStage(new ETLStage("window", Window.getPlugin(30, 1)))
      .addStage(new ETLStage("agg", FieldCountAggregator.getPlugin("x", "string")))
      .addStage(new ETLStage("filter", StringValueFilterTransform.getPlugin("x", "all")))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addConnection("source", "window")
      .addConnection("window", "agg")
      .addConnection("agg", "filter")
      .addConnection("filter", "sink")
      .setBatchInterval("1s")
      .setCheckpointDir(checkpointDir)
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("WindowerApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    // the sink should contain at least one record with count of 3, and no records with more than 3.
    // less than 3 if the window doesn't contain all 3 records yet, but there should eventually be a window
    // that contains all 3.
    final DataSetManager<Table> outputManager = getDataset(sinkName);
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          outputManager.flush();
          boolean sawThree = false;
          for (StructuredRecord record : MockSink.readOutput(outputManager)) {
            long count = record.get("ct");
            if (count == 3L) {
              sawThree = true;
            }
            Assert.assertTrue(count <= 3L);
          }
          return sawThree;
        }
      },
      2,
      TimeUnit.MINUTES);

    sparkManager.stop();
  }

  @Test
  public void testJoin() throws Exception {
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

    List<StructuredRecord> input1 = ImmutableList.of(recordSamuel, recordBob, recordJane);
    List<StructuredRecord> input2 = ImmutableList.of(recordCar, recordBike);
    List<StructuredRecord> input3 = ImmutableList.of(recordTrasCar, recordTrasBike, recordTrasPlane);

    String outputName = "multiJoinOutputSink";
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("source1", MockSource.getPlugin(inputSchema1, input1)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(inputSchema2, input2)))
      .addStage(new ETLStage("source3", MockSource.getPlugin(inputSchema3, input3)))
      .addStage(new ETLStage("t1", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t2", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t3", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t4", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("innerjoin", MockJoiner.getPlugin("t1.customer_id=t2.cust_id",
                                                               "t1,t2", "")))
      .addStage(new ETLStage("outerjoin", MockJoiner.getPlugin("t4.item_id=t3.i_id",
                                                                 "", "")))
      .addStage(new ETLStage("multijoinSink", MockSink.getPlugin(outputName)))
      .addConnection("source1", "t1")
      .addConnection("source2", "t2")
      .addConnection("source3", "t3")
      .addConnection("t1", "innerjoin")
      .addConnection("t2", "innerjoin")
      .addConnection("innerjoin", "t4")
      .addConnection("t3", "outerjoin")
      .addConnection("t4", "outerjoin")
      .addConnection("outerjoin", "multijoinSink")
      .setBatchInterval("5s")
      .setCheckpointDir(checkpointDir)
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("JoinerApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

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
    Set<StructuredRecord> expected = ImmutableSet.of(joinRecordSamuel, joinRecordJane, joinRecordPlane);

    DataSetManager<Table> outputManager = getDataset(outputName);
    Tasks.waitFor(
      true,
      () -> {
        outputManager.flush();
        Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(outputManager));
        return expected.equals(outputRecords);
      },
      4,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStopped(10, TimeUnit.SECONDS);

    validateMetric(appId, "source1.records.out", 3);
    validateMetric(appId, "source2.records.out", 2);
    validateMetric(appId, "source3.records.out", 3);
    validateMetric(appId, "t1.records.in", 3);
    validateMetric(appId, "t1.records.out", 3);
    validateMetric(appId, "t2.records.in", 2);
    validateMetric(appId, "t2.records.out", 2);
    validateMetric(appId, "t3.records.in", 3);
    validateMetric(appId, "t3.records.out", 3);
    validateMetric(appId, "t4.records.in", 2);
    validateMetric(appId, "t4.records.out", 2);
    validateMetric(appId, "innerjoin.records.in", 5);
    validateMetric(appId, "innerjoin.records.out", 2);
    validateMetric(appId, "outerjoin.records.in", 5);
    validateMetric(appId, "outerjoin.records.out", 3);
    validateMetric(appId, "multijoinSink.records.in", 3);
  }

  @Test
  public void testAutoJoin() throws Exception {
    /*
     * customers ----------|
     *                     |
     *                     |---> join ---> sink
     *                     |
     * transactions -------|
     */

    Schema inputSchema1 = Schema.recordOf(
      "customer",
      Schema.Field.of("customer_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("customer_name", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema2 = Schema.recordOf(
      "transaction",
      Schema.Field.of("t_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("customer_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item_id", Schema.of(Schema.Type.STRING))
    );

    Schema outSchema = Schema.recordOf(
      "customers.transactions",
      Schema.Field.of("customers_customer_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("customers_customer_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("transactions_t_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("transactions_customer_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("transactions_item_id", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord recordSamuel = StructuredRecord.builder(inputSchema1)
      .set("customer_id", "1")
      .set("customer_name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(inputSchema1)
      .set("customer_id", "2")
      .set("customer_name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(inputSchema1)
      .set("customer_id", "3")
      .set("customer_name", "jane").build();

    StructuredRecord tx1 = StructuredRecord.builder(inputSchema2)
      .set("t_id", "1")
      .set("customer_id", "1")
      .set("item_id", "11").build();
    StructuredRecord tx2 = StructuredRecord.builder(inputSchema2)
      .set("t_id", "2")
      .set("customer_id", "3")
      .set("item_id", "22").build();
    StructuredRecord tx3 = StructuredRecord.builder(inputSchema2)
      .set("t_id", "3")
      .set("customer_id", "4")
      .set("item_id", "33").build();

    List<StructuredRecord> input1 = ImmutableList.of(recordSamuel, recordBob, recordJane);
    List<StructuredRecord> input2 = ImmutableList.of(tx1, tx2, tx3);

    String outputName = UUID.randomUUID().toString();
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("customers", MockSource.getPlugin(inputSchema1, input1)))
      .addStage(new ETLStage("transactions", MockSource.getPlugin(inputSchema2, input2)))
      .addStage(new ETLStage("join", MockAutoJoiner.getPlugin(Arrays.asList("customers", "transactions"),
                                                              Collections.singletonList("customer_id"),
                                                              Collections.singletonList("transactions"))))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputName)))
      .addConnection("customers", "join")
      .addConnection("transactions", "join")
      .addConnection("join", "sink")
      .setBatchInterval("5s")
      .setCheckpointDir(checkpointDir)
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("AutoJoinerApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    StructuredRecord join1 = StructuredRecord.builder(outSchema)
      .set("customers_customer_id", "1").set("customers_customer_name", "samuel")
      .set("transactions_t_id", "1").set("transactions_customer_id", "1").set("transactions_item_id", "11").build();

    StructuredRecord join2 = StructuredRecord.builder(outSchema)
      .set("customers_customer_id", "3").set("customers_customer_name", "jane")
      .set("transactions_t_id", "2").set("transactions_customer_id", "3").set("transactions_item_id", "22").build();

    StructuredRecord join3 = StructuredRecord.builder(outSchema)
      .set("transactions_t_id", "3").set("transactions_customer_id", "4").set("transactions_item_id", "33").build();
    Set<StructuredRecord> expected = ImmutableSet.of(join1, join2, join3);

    DataSetManager<Table> outputManager = getDataset(outputName);
    Tasks.waitFor(
      true,
      () -> {
        outputManager.flush();
        Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(outputManager));
        return expected.equals(outputRecords);
      },
      4,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStopped(10, TimeUnit.SECONDS);
  }

  @Test
  public void testAutoJoinNullEquality() throws Exception {
    testAutoJoinNullEquality(true);
    testAutoJoinNullEquality(false);
  }

  private void testAutoJoinNullEquality(boolean nullSafe) throws Exception {
    /*
     * customers ----------|
     *                     |
     *                     |---> join ---> sink
     *                     |
     * transactions -------|
     */

    Schema inputSchema1 = Schema.recordOf(
      "customer",
      Schema.Field.of("customer_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("customer_name", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    Schema inputSchema2 = Schema.recordOf(
      "transaction",
      Schema.Field.of("t_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("customer_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("item_id", Schema.of(Schema.Type.STRING))
    );

    Schema outSchema = Schema.recordOf(
      "customers.transactions",
      Schema.Field.of("customers_customer_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("customers_customer_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("transactions_t_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("transactions_customer_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("transactions_item_id", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord recordSamuel = StructuredRecord.builder(inputSchema1)
      .set("customer_id", "1")
      .set("customer_name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(inputSchema1)
      .set("customer_name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(inputSchema1)
      .set("customer_id", "3")
      .set("customer_name", "jane").build();

    StructuredRecord trans1 = StructuredRecord.builder(inputSchema2)
      .set("t_id", "1")
      .set("customer_id", "1")
      .set("item_id", "11").build();
    StructuredRecord trans2 = StructuredRecord.builder(inputSchema2)
      .set("t_id", "2")
      .set("customer_id", "3")
      .set("item_id", "22").build();
    StructuredRecord trans3 = StructuredRecord.builder(inputSchema2)
      .set("t_id", "3")
      .set("item_id", "33").build();

    List<StructuredRecord> input1 = ImmutableList.of(recordSamuel, recordBob, recordJane);
    List<StructuredRecord> input2 = ImmutableList.of(trans1, trans2, trans3);

    String outputName = UUID.randomUUID().toString();
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("customers", MockSource.getPlugin(inputSchema1, input1)))
      .addStage(new ETLStage("transactions", MockSource.getPlugin(inputSchema2, input2)))
      .addStage(new ETLStage("join", MockAutoJoiner.getPlugin(Arrays.asList("customers", "transactions"),
                                                              Collections.singletonList("customer_id"),
                                                              Collections.singletonList("transactions"),
                                                              nullSafe)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputName)))
      .addConnection("customers", "join")
      .addConnection("transactions", "join")
      .addConnection("join", "sink")
      .setBatchInterval("5s")
      .setCheckpointDir(checkpointDir)
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    StructuredRecord join1 = StructuredRecord.builder(outSchema)
      .set("customers_customer_id", "1").set("customers_customer_name", "samuel")
      .set("transactions_t_id", "1").set("transactions_customer_id", "1").set("transactions_item_id", "11").build();

    StructuredRecord join2 = StructuredRecord.builder(outSchema)
      .set("customers_customer_id", "3").set("customers_customer_name", "jane")
      .set("transactions_t_id", "2").set("transactions_customer_id", "3").set("transactions_item_id", "22").build();

    StructuredRecord join3;
    if (nullSafe) {
      // this transaction has a null customer id, which should match with the null id from customers
      join3 = StructuredRecord.builder(outSchema)
        .set("transactions_t_id", "3").set("transactions_item_id", "33").set("customers_customer_name", "bob").build();
    } else {
      // this transaction has a null customer id, which should not match with the null id from customers
      join3 = StructuredRecord.builder(outSchema)
        .set("transactions_t_id", "3").set("transactions_item_id", "33").build();
    }
    Set<StructuredRecord> expected = ImmutableSet.of(join1, join2, join3);

    DataSetManager<Table> outputManager = getDataset(outputName);
    Tasks.
      waitFor(
      true,
      () -> {
        outputManager.flush();
        Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(outputManager));
        return expected.equals(outputRecords);
      },
      4,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStopped(10, TimeUnit.SECONDS);
  }

  @Test
  public void testErrorTransform() throws Exception {
    String sink1TableName = "errTestOut1";
    String sink2TableName = "errTestOut2";

    Schema inputSchema = Schema.recordOf("user", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("name", "Leo").build(),
      StructuredRecord.builder(inputSchema).set("name", "Ralph").build(),
      StructuredRecord.builder(inputSchema).set("name", "Don").build(),
      StructuredRecord.builder(inputSchema).set("name", "Mike").build(),
      StructuredRecord.builder(inputSchema).set("name", "April").build());
    /*
     *
     * source--> filter1 --> filter2 --> agg1 --> agg2
     *              |           |         |        |
     *              |-----------|---------|--------|--------|--> flatten errors --> sink1
     *                                                      |
     *                                                      |--> filter errors --> sink2
     * arrows coming out the right represent output records
     * arrows coming out the bottom represent error records
     * this will test multiple stages from multiple phases emitting errors to the same stage
     * as well as errors from one stage going to multiple stages
     */
    DataStreamsConfig config = DataStreamsConfig.builder()
      .setBatchInterval("5s")
      .addStage(new ETLStage("source", MockSource.getPlugin(inputSchema, input)))
      .addStage(new ETLStage("filter1", StringValueFilterTransform.getPlugin("name", "Leo")))
      .addStage(new ETLStage("filter2", StringValueFilterTransform.getPlugin("name", "Ralph")))
      .addStage(new ETLStage("agg1", GroupFilterAggregator.getPlugin("name", "Don")))
      .addStage(new ETLStage("agg2", GroupFilterAggregator.getPlugin("name", "Mike")))
      .addStage(new ETLStage("errorflatten", FlattenErrorTransform.getPlugin()))
      .addStage(new ETLStage("errorfilter", FilterErrorTransform.getPlugin(3)))
      .addStage(new ETLStage("sink1", MockSink.getPlugin(sink1TableName)))
      .addStage(new ETLStage("sink2", MockSink.getPlugin(sink2TableName)))
      .addConnection("source", "filter1")
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
      .addConnection("errorflatten", "sink1")
      .addConnection("errorfilter", "sink2")
      .setCheckpointDir(checkpointDir)
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("ErrTransformTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    Schema flattenSchema =
      Schema.recordOf("erroruser",
                      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                      Schema.Field.of("errMsg", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("errCode", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                      Schema.Field.of("errStage", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Set<StructuredRecord> expected = ImmutableSet.of(
      StructuredRecord.builder(flattenSchema)
        .set("name", "Leo").set("errMsg", "bad string value").set("errCode", 1).set("errStage", "filter1").build(),
      StructuredRecord.builder(flattenSchema)
        .set("name", "Ralph").set("errMsg", "bad string value").set("errCode", 1).set("errStage", "filter2").build(),
      StructuredRecord.builder(flattenSchema)
        .set("name", "Don").set("errMsg", "bad val").set("errCode", 3).set("errStage", "agg1").build(),
      StructuredRecord.builder(flattenSchema)
        .set("name", "Mike").set("errMsg", "bad val").set("errCode", 3).set("errStage", "agg2").build());
    DataSetManager<Table> sink1Table = getDataset(sink1TableName);
    Tasks.waitFor(
      true,
      () -> {
        sink1Table.flush();
        Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(sink1Table));
        return expected.equals(outputRecords);
      },
      4,
      TimeUnit.MINUTES);

    Set<StructuredRecord> expected2 = ImmutableSet.of(
      StructuredRecord.builder(inputSchema).set("name", "Leo").build(),
      StructuredRecord.builder(inputSchema).set("name", "Ralph").build());
    DataSetManager<Table> sink2Table = getDataset(sink2TableName);
    Tasks.waitFor(
      true,
      () -> {
        sink2Table.flush();
        Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(sink2Table));
        return expected2.equals(outputRecords);
      },
      4,
      TimeUnit.MINUTES);
  }

  @Test
  public void testSplitterTransform() throws Exception {
    Schema schema = Schema.recordOf("user",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("email", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    StructuredRecord user0 = StructuredRecord.builder(schema).set("id", 0L).build();
    StructuredRecord user1 = StructuredRecord.builder(schema).set("id", 1L).set("email", "one@example.com").build();
    StructuredRecord user2 = StructuredRecord.builder(schema).set("id", 2L).set("name", "two").build();
    StructuredRecord user3 = StructuredRecord.builder(schema)
      .set("id", 3L).set("name", "three").set("email", "three@example.com").build();

    String sink1Name = "splitSink1";
    String sink2Name = "splitSink2";

    /*
     *
     *                                            |null --> sink1
     *                       |null--> splitter2 --|
     * source --> splitter1--|                    |non-null --|
     *                       |                                |--> sink2
     *                       |non-null------------------------|
     */
    DataStreamsConfig config = DataStreamsConfig.builder()
      .setBatchInterval("5s")
      .addStage(new ETLStage("source", MockSource.getPlugin(schema, ImmutableList.of(user0, user1, user2, user3))))
      .addStage(new ETLStage("splitter1", NullFieldSplitterTransform.getPlugin("name")))
      .addStage(new ETLStage("splitter2", NullFieldSplitterTransform.getPlugin("email")))
      .addStage(new ETLStage("sink1", MockSink.getPlugin(sink1Name)))
      .addStage(new ETLStage("sink2", MockSink.getPlugin(sink2Name)))
      .addConnection("source", "splitter1")
      .addConnection("splitter1", "splitter2", "null")
      .addConnection("splitter1", "sink2", "non-null")
      .addConnection("splitter2", "sink1", "null")
      .addConnection("splitter2", "sink2", "non-null")
      .setCheckpointDir(checkpointDir)
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("SplitterTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // run pipeline
    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    // check output
    // sink1 should only have records where both name and email are null (user0)
    DataSetManager<Table> sink1Manager = getDataset(sink1Name);
    Set<StructuredRecord> expected1 = ImmutableSet.of(user0);
    Tasks.waitFor(
      true,
      () -> {
        sink1Manager.flush();
        Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(sink1Manager));
        return expected1.equals(outputRecords);
      },
      4,
      TimeUnit.MINUTES);

    // sink2 should have anything with a non-null name or non-null email
    DataSetManager<Table> sink2Manager = getDataset(sink2Name);
    Set<StructuredRecord> expected2 = ImmutableSet.of(user1, user2, user3);

    Tasks.waitFor(
      true,
      () -> {
        sink2Manager.flush();
        Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(sink2Manager));
        return expected2.equals(outputRecords);
      },
      4,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStopped(10, TimeUnit.SECONDS);

    validateMetric(appId, "source.records.out", 4);
    validateMetric(appId, "splitter1.records.in", 4);
    validateMetric(appId, "splitter1.records.out.non-null", 2);
    validateMetric(appId, "splitter1.records.out.null", 2);
    validateMetric(appId, "splitter2.records.in", 2);
    validateMetric(appId, "splitter2.records.out.non-null", 1);
    validateMetric(appId, "splitter2.records.out.null", 1);
    validateMetric(appId, "sink1.records.in", 1);
    validateMetric(appId, "sink2.records.in", 3);
  }

  @Test
  public void testAlertPublisher() throws Exception {
    String sinkName = "alertSink";
    String topic = "alertTopic";

    Schema schema = Schema.recordOf("x", Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
    StructuredRecord record1 = StructuredRecord.builder(schema).set("id", 1L).build();
    StructuredRecord record2 = StructuredRecord.builder(schema).set("id", 2L).build();
    StructuredRecord alertRecord = StructuredRecord.builder(schema).build();

    /*
     * source --> nullAlert --> sink
     *               |
     *               |--> TMS publisher
     */
    DataStreamsConfig config = DataStreamsConfig.builder()
      .setBatchInterval("5s")
      .addStage(new ETLStage("source", MockSource.getPlugin(schema, ImmutableList.of(record1, record2, alertRecord))))
      .addStage(new ETLStage("nullAlert", NullAlertTransform.getPlugin("id")))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addStage(new ETLStage("tms", TMSAlertPublisher.getPlugin(topic, NamespaceId.DEFAULT.getNamespace())))
      .addConnection("source", "nullAlert")
      .addConnection("nullAlert", "sink")
      .addConnection("nullAlert", "tms")
      .setCheckpointDir(checkpointDir)
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("AlertTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    final Set<StructuredRecord> expectedRecords = ImmutableSet.of(record1, record2);
    final Set<Alert> expectedMessages = ImmutableSet.of(new Alert("nullAlert", new HashMap<String, String>()));
    final DataSetManager<Table> sinkTable = getDataset(sinkName);

    Tasks.waitFor(
      true,
      () -> {
        // get alerts from TMS
        try {
          getMessagingAdmin(NamespaceId.DEFAULT.getNamespace()).getTopicProperties(topic);
        } catch (TopicNotFoundException e) {
          return false;
        }
        MessageFetcher messageFetcher = getMessagingContext().getMessageFetcher();
        Set<Alert> actualMessages = new HashSet<>();
        try (CloseableIterator<Message> iter =
               messageFetcher.fetch(NamespaceId.DEFAULT.getNamespace(), topic, 5, 0)) {
          while (iter.hasNext()) {
            Message message = iter.next();
            Alert alert = GSON.fromJson(message.getPayloadAsString(), Alert.class);
            actualMessages.add(alert);
          }
        }

        // get records from sink
        sinkTable.flush();
        Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(sinkTable));

        return expectedRecords.equals(outputRecords) && expectedMessages.equals(actualMessages);
      },
      4,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStopped(10, TimeUnit.SECONDS);

    validateMetric(appId, "source.records.out", 3);
    validateMetric(appId, "nullAlert.records.in", 3);
    validateMetric(appId, "nullAlert.records.out", 2);
    validateMetric(appId, "nullAlert.records.alert", 1);
    validateMetric(appId, "sink.records.in", 2);
    validateMetric(appId, "tms.records.in", 1);
  }

  private void validateMetric(ApplicationId appId, String metric,
                              long expected) throws TimeoutException, InterruptedException {
    MetricsManager metricsManager = getMetricsManager();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName(),
                                               Constants.Metrics.Tag.SPARK, DataStreamsSparkLauncher.NAME);
    String metricName = "user." + metric;
    metricsManager.waitForTotalMetricCount(tags, metricName, expected, 10, TimeUnit.SECONDS);
    metricsManager.waitForTotalMetricCount(tags, metricName, expected, 10, TimeUnit.SECONDS);
    // wait for won't throw an exception if the metric count is greater than expected
    Assert.assertEquals(expected, metricsManager.getTotalMetric(tags, metricName));
  }

  private long getMetric(ApplicationId appId, String metric) {
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName(),
                                               Constants.Metrics.Tag.SPARK, DataStreamsSparkLauncher.NAME);
    return getMetricsManager().getTotalMetric(tags, "user." + metric);
  }
}
