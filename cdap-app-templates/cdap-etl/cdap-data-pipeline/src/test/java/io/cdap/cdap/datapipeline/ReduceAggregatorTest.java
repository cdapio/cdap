/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.batch.reduceaggregator.DistinctReduceAggregator;
import io.cdap.cdap.etl.mock.batch.reduceaggregator.FieldCountReduceAggregator;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests for BatchReduceAggregator plugins.
 */
public class ReduceAggregatorTest extends HydratorTestBase {
  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");

  private static int startCount = 0;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.Security.Store.PROVIDER, "file",
                                                                       Constants.AppFabric.SPARK_COMPAT,
                                                                       Compat.SPARK_COMPAT);

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }
    setupBatchArtifacts(APP_ARTIFACT_ID, DataPipelineApp.class);
  }


  @Test
  public void testSimpleAggregator() throws Exception {
    Schema inputSchema = Schema.recordOf(
      "input",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("used_name", Schema.of(Schema.Type.STRING)));
    String userInput = "inputSource";
    String output = "outputSink";
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("users", MockSource.getPlugin(userInput, inputSchema)))
      .addStage(new ETLStage("aggregator", DistinctReduceAggregator.getPlugin("id,name")))
      .addStage(new ETLStage("sink", MockSink.getPlugin(output)))
      .addConnection("users", "aggregator")
      .addConnection("aggregator", "sink")
      .setEngine(Engine.SPARK)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    List<StructuredRecord> userData = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      StructuredRecord record1 = StructuredRecord.builder(inputSchema)
        .set("id", 1).set("name", "test").set("used_name", "test" + i).build();
      StructuredRecord record2 = StructuredRecord.builder(inputSchema)
        .set("id", 2).set("name", "test2").set("used_name", "test2" + i).build();
      userData.add(record1);
      userData.add(record2);
    }

    // write input data
    DataSetManager<Table> inputManager = getDataset(userInput);
    MockSource.writeInput(inputManager, userData);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Schema expectedSchema = Schema.recordOf(
      "record",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    Set<StructuredRecord> expected = ImmutableSet.of(
      StructuredRecord.builder(expectedSchema).set("id", 1).set("name", "test").build(),
      StructuredRecord.builder(expectedSchema).set("id", 2).set("name", "test2").build()
    );
    Assert.assertEquals(expected, new HashSet<>(outputRecords));
  }

  @Test
  public void testFieldCountAgg() throws Exception {
    Engine engine = Engine.SPARK;
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
      .addStage(new ETLStage("agg1", FieldCountReduceAggregator.getPlugin("user", "string")))
      .addStage(new ETLStage("agg2", FieldCountReduceAggregator.getPlugin("item", "long")))
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
    // 2 users, but FieldCountReduceAggregator always emits an 'all' group
    validateMetric(3, appId, "agg1.aggregator.groups");
    validateMetric(3, appId, "agg1.records.out");
    validateMetric(5, appId, "agg2.records.in");
    // 4 items, but FieldCountReduceAggregator always emits an 'all' group
    validateMetric(5, appId, "agg2.aggregator.groups");
    validateMetric(5, appId, "agg2.records.out");
    validateMetric(3, appId, "sink1.records.in");
    validateMetric(5, appId, "sink2.records.in");
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
