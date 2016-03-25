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

package co.cask.cdap.etl.datapipeline;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.datapipeline.mock.FieldCountAggregator;
import co.cask.cdap.etl.datapipeline.mock.IdentityAggregator;
import co.cask.cdap.etl.datapipeline.mock.IdentityTransform;
import co.cask.cdap.etl.datapipeline.mock.MockSink;
import co.cask.cdap.etl.datapipeline.mock.MockSource;
import co.cask.cdap.etl.datapipeline.mock.StringValueFilterTransform;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DataPipelineTest extends TestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final Id.Artifact APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT, "datapipeline", "3.4.0");
  protected static final ArtifactSummary ARTIFACT = new ArtifactSummary("datapipeline", "3.4.0");

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    addAppArtifact(APP_ARTIFACT_ID, DataPipelineApp.class,
                   BatchSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    // add some test plugins
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "test-plugins", "1.0.0"), APP_ARTIFACT_ID,
                      MockSource.class, MockSink.class,
                      StringValueFilterTransform.class, IdentityTransform.class,
                      FieldCountAggregator.class, IdentityAggregator.class);
  }

  @Test
  public void testSinglePhase() throws Exception {
    /*
     * source --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("singleInput")))
      .addStage(new ETLStage("sink", MockSink.getPlugin("singleOutput")))
      .addConnection("source", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );
    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();

    // write records to source
    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, "singleInput");
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    // check sink
    DataSetManager<Table> sinkManager = getDataset("singleOutput");
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSimpleMultiSource() throws Exception {
    /*
     * source1 --|
     *           |--> sink
     * source2 --|
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source1", MockSource.getPlugin("simpleMSInput1")))
      .addStage(new ETLStage("source2", MockSource.getPlugin("simpleMSInput2")))
      .addStage(new ETLStage("sink", MockSink.getPlugin("simpleMSOutput")))
      .addConnection("source1", "sink")
      .addConnection("source2", "sink")
      .build();


    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "SimpleMultiSourceApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );
    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, "simpleMSInput1");
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel));
    inputManager = getDataset(Id.Namespace.DEFAULT, "simpleMSInput2");
    MockSource.writeInput(inputManager, ImmutableList.of(recordBob));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    // check sink
    DataSetManager<Table> sinkManager = getDataset("simpleMSOutput");
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMultiSource() throws Exception {
    /*
     * source1 --|                 |--> sink1
     *           |--> transform1 --|
     * source2 --|                 |
     *                             |--> transform2 --> sink2
     * source3 --------------------|
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source1", MockSource.getPlugin("msInput1")))
      .addStage(new ETLStage("source2", MockSource.getPlugin("msInput2")))
      .addStage(new ETLStage("source3", MockSource.getPlugin("msInput3")))
      .addStage(new ETLStage("transform1", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("transform2", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("sink1", MockSink.getPlugin("msOutput1")))
      .addStage(new ETLStage("sink2", MockSink.getPlugin("msOutput2")))
      .addConnection("source1", "transform1")
      .addConnection("source2", "transform1")
      .addConnection("transform1", "sink1")
      .addConnection("transform1", "transform2")
      .addConnection("transform2", "sink2")
      .addConnection("source3", "transform2")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "MultiSourceApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(schema).set("name", "jane").build();

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, "msInput1");
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel));
    inputManager = getDataset(Id.Namespace.DEFAULT, "msInput2");
    MockSource.writeInput(inputManager, ImmutableList.of(recordBob));
    inputManager = getDataset(Id.Namespace.DEFAULT, "msInput3");
    MockSource.writeInput(inputManager, ImmutableList.of(recordJane));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    // sink1 should get records from source1 and source2
    DataSetManager<Table> sinkManager = getDataset("msOutput1");
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel, recordBob);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    // sink2 should get all records
    sinkManager = getDataset("msOutput2");
    expected = ImmutableSet.of(recordSamuel, recordBob, recordJane);
    actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testLinearAggregators() throws Exception {
    /*
     * source --> filter1 --> aggregator1 --> aggregator2 --> filter2 --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("linearAggInput")))
      .addStage(new ETLStage("sink", MockSink.getPlugin("linearAggOutput")))
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

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "LinearAggApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    Schema schema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord recordSamuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(schema).set("name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(schema).set("name", "jane").build();

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, "linearAggInput");
    MockSource.writeInput(inputManager, ImmutableList.of(recordSamuel, recordBob, recordJane));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    // check output
    DataSetManager<Table> sinkManager = getDataset("linearAggOutput");
    Set<StructuredRecord> expected = ImmutableSet.of(recordSamuel);
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testParallelAggregators() throws Exception {
    /*
                 |--> agg1 --> sink1
        source --|
                 |--> agg2 --> sink2
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("pAggInput")))
      .addStage(new ETLStage("sink1", MockSink.getPlugin("pAggOutput1")))
      .addStage(new ETLStage("sink2", MockSink.getPlugin("pAggOutput2")))
      .addStage(new ETLStage("agg1", FieldCountAggregator.getPlugin("user", "string")))
      .addStage(new ETLStage("agg2", FieldCountAggregator.getPlugin("item", "long")))
      .addConnection("source", "agg1")
      .addConnection("source", "agg2")
      .addConnection("agg1", "sink1")
      .addConnection("agg2", "sink2")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "ParallelAggApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    Schema inputSchema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.LONG))
    );

    // write one record to each source
    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, "pAggInput");
    MockSource.writeInput(inputManager, ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("user", "samuel").set("item", 1L).build(),
      StructuredRecord.builder(inputSchema).set("user", "samuel").set("item", 2L).build(),
      StructuredRecord.builder(inputSchema).set("user", "samuel").set("item", 3L).build(),
      StructuredRecord.builder(inputSchema).set("user", "john").set("item", 4L).build(),
      StructuredRecord.builder(inputSchema).set("user", "john").set("item", 3L).build()
    ));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

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
    DataSetManager<Table> sinkManager = getDataset("pAggOutput1");
    Set<StructuredRecord> expected = ImmutableSet.of(
      StructuredRecord.builder(outputSchema1).set("user", "all").set("ct", 5L).build(),
      StructuredRecord.builder(outputSchema1).set("user", "samuel").set("ct", 3L).build(),
      StructuredRecord.builder(outputSchema1).set("user", "john").set("ct", 2L).build());
    Set<StructuredRecord> actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);

    sinkManager = getDataset("pAggOutput2");
    expected = ImmutableSet.of(
      StructuredRecord.builder(outputSchema2).set("item", 0L).set("ct", 5L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 1L).set("ct", 1L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 2L).set("ct", 1L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 3L).set("ct", 2L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 4L).set("ct", 1L).build());
    actual = Sets.newHashSet(MockSink.readOutput(sinkManager));
    Assert.assertEquals(expected, actual);
  }
}
