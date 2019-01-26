/*
 * Copyright © 2019 Cask Data, Inc.
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

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.datapipeline.plugin.PluggableFilterTransform;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.batch.aggregator.IdentityAggregator;
import co.cask.cdap.etl.mock.batch.joiner.MockJoiner;
import co.cask.cdap.etl.mock.condition.TrueCondition;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.mock.transform.IdentityTransform;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.spark.Compat;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests for runtime arguments in data pipelines
 */
public class DataPipelineSchemaTest extends HydratorTestBase {

  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.Security.Store.PROVIDER, "file",
                                                                       Constants.AppFabric.SPARK_COMPAT,
                                                                       Compat.SPARK_COMPAT);

  @BeforeClass
  public static void setupTest() throws Exception {
    setupBatchArtifacts(APP_ARTIFACT_ID, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact("test-plugins", "1.0.0"), APP_ARTIFACT_ID,
                      PluggableFilterTransform.class);
  }

  @Test
  public void testRuntimeSchemaPropagation() throws Exception {
    testRuntimeSchemaPropagation(Engine.MAPREDUCE);
    testRuntimeSchemaPropagation(Engine.SPARK);
  }

  private void testRuntimeSchemaPropagation(Engine engine) throws Exception {
    String sourceName = UUID.randomUUID().toString();
    String sinkName = UUID.randomUUID().toString();

    Schema schema = Schema.recordOf("x", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    /*
         source -> identity1 -> identity2 -> argument setting transform -> sink

         The identity1 and identity2 aggregators are used to ensure that the source is in a different mapreduce
         program than the sink, to test schema propagation across phases.

         The source is configured with schema ${schema}
         The sink is configured to write to ${output} and validate its schema
         The argument setting transform is configured to set argument "output" = <sinkName>

         If runtime configuration executes properly, the ${schema} property will be macro evaluated and
         propagated down to the sink where it will be verified. The 'output'= <sinkName> property will also
         be available when the sink runs, where it will create the dataset, validate the schema, and write to it.
     */
    Map<String, String> sourceProperties = new HashMap<>();
    sourceProperties.put("tableName", sourceName);
    sourceProperties.put("schema", "${schema}");
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(new ETLStage("source", new ETLPlugin("Mock", BatchSource.PLUGIN_TYPE, sourceProperties)))
      .addStage(new ETLStage("identity1", IdentityAggregator.getPlugin()))
      .addStage(new ETLStage("identity2", IdentityAggregator.getPlugin()))
      .addStage(new ETLStage("argSetter", IdentityTransform.getPlugin("output", sinkName)))
      .addStage(new ETLStage("sink", MockSink.getPlugin("${output}", schema)))
      .addConnection("source", "identity1")
      .addConnection("identity1", "identity2")
      .addConnection("identity2", "argSetter")
      .addConnection("argSetter", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    StructuredRecord samuel = StructuredRecord.builder(schema).set("name", "samuel").build();
    StructuredRecord dwayne = StructuredRecord.builder(schema).set("name", "dwayne").build();
    Set<StructuredRecord> inputRecords = new HashSet<>();
    inputRecords.add(samuel);
    inputRecords.add(dwayne);

    DataSetManager<Table> sourceTable = getDataset(sourceName);
    MockSource.writeInput(sourceTable, inputRecords);

    Map<String, String> runtimeArgs = new HashMap<>();
    runtimeArgs.put("schema", schema.toString());

    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.startAndWaitForRun(runtimeArgs, ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    DataSetManager<Table> sinkTable = getDataset(sinkName);
    Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(sinkTable));
    Assert.assertEquals(inputRecords, outputRecords);
  }

  @Test
  public void testJoinerSchemaPropagation() throws Exception {
    /*
        users --> i1 ---|
                        |--> joiner --> sink
        emails --> i2 --|

        Both the users and emails sources have schema set as a macro
        The identity1 and identity2 aggregators are used to ensure that the joiner is in a different phase
        than the sources, to make sure schema propagates correctly to the joiner
     */
    String usersTable = UUID.randomUUID().toString();
    Map<String, String> properties = new HashMap<>();
    properties.put("tableName", usersTable);
    properties.put("schema", "${users.schema}");
    ETLStage users = new ETLStage("users", new ETLPlugin("Mock", BatchSource.PLUGIN_TYPE, properties));

    String emailsTable = UUID.randomUUID().toString();
    properties = new HashMap<>();
    properties.put("tableName", emailsTable);
    properties.put("schema", "${emails.schema}");
    ETLStage emails = new ETLStage("emails", new ETLPlugin("Mock", BatchSource.PLUGIN_TYPE, properties));

    Schema userSchema = Schema.recordOf("user",
                                        Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                        Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    Schema emailSchema = Schema.recordOf("email",
                                         Schema.Field.of("from", Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of("to", Schema.of(Schema.Type.STRING)));
    Schema expectedSchema = Schema.recordOf("join.output",
                                            Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                            Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                            Schema.Field.of("from", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                            Schema.Field.of("to", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    String sinkTable = UUID.randomUUID().toString();

    ETLBatchConfig config = ETLBatchConfig.builder()
      .setEngine(Engine.MAPREDUCE)
      .addStage(users)
      .addStage(emails)
      .addStage(new ETLStage("i1", IdentityAggregator.getPlugin()))
      .addStage(new ETLStage("i2", IdentityAggregator.getPlugin()))
      .addStage(new ETLStage("joiner", MockJoiner.getPlugin("i1.id=i2.from", "i1,i2", "")))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkTable, expectedSchema)))
      .addConnection("users", "i1")
      .addConnection("emails", "i2")
      .addConnection("i1", "joiner")
      .addConnection("i2", "joiner")
      .addConnection("joiner", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> tableManager = getDataset(usersTable);
    StructuredRecord user = StructuredRecord.builder(userSchema).set("id", "abc").set("name", "xyz").build();
    MockSource.writeInput(tableManager, Collections.singleton(user));

    tableManager = getDataset(emailsTable);
    StructuredRecord email = StructuredRecord.builder(emailSchema).set("from", "abc").set("to", "efg").build();
    MockSource.writeInput(tableManager, Collections.singleton(email));

    Map<String, String> runtimeArgs = new HashMap<>();
    runtimeArgs.put("users.schema", userSchema.toString());
    runtimeArgs.put("emails.schema", emailSchema.toString());

    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.startAndWaitForRun(runtimeArgs, ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    tableManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(tableManager);
    StructuredRecord expected = StructuredRecord.builder(expectedSchema)
      .set("id", "abc").set("name", "xyz").set("from", "abc").set("to", "efg").build();
    Assert.assertEquals(Collections.singletonList(expected), outputRecords);
  }

  @Test
  public void testConditionSchemaPropagation() throws Exception {
    testConditionSchemaPropagation(Engine.MAPREDUCE);
    testConditionSchemaPropagation(Engine.SPARK);
  }

  private void testConditionSchemaPropagation(Engine engine) throws Exception {
    /*
        source --> c0 --> c1 --> sink

        The source has schema set as a macro.
        The schema should be propagated at runtime through the conditions into the sink.
     */

    String sourceTable = UUID.randomUUID().toString();
    Map<String, String> sourceProperties = new HashMap<>();
    sourceProperties.put("tableName", sourceTable);
    sourceProperties.put("schema", "${schema}");
    ETLStage source = new ETLStage("source", new ETLPlugin("Mock", BatchSource.PLUGIN_TYPE, sourceProperties));

    Schema schema = Schema.recordOf("x", Schema.Field.of("x", Schema.of(Schema.Type.STRING)));
    String sinkTable = UUID.randomUUID().toString();

    ETLBatchConfig config = ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(source)
      .addStage(new ETLStage("c0", TrueCondition.getPlugin()))
      .addStage(new ETLStage("c1", TrueCondition.getPlugin()))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkTable, schema)))
      .addConnection(source.getName(), "c0")
      .addConnection("c0", "c1", true)
      .addConnection("c1", "sink", true)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    StructuredRecord input = StructuredRecord.builder(schema).set("x", "abc").build();
    DataSetManager<Table> tableManager = getDataset(sourceTable);
    MockSource.writeInput(tableManager, Collections.singleton(input));

    Map<String, String> runtimeArgs = new HashMap<>();
    runtimeArgs.put("schema", schema.toString());

    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.startAndWaitForRun(runtimeArgs, ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    tableManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(tableManager);
    Assert.assertEquals(Collections.singletonList(input), outputRecords);
  }
}
