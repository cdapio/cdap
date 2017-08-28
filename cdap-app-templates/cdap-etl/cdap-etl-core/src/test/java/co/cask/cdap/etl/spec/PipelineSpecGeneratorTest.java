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

package co.cask.cdap.etl.spec;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.MultiOutputPipelineConfigurable;
import co.cask.cdap.etl.api.MultiOutputPipelineConfigurer;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.SplitterTransform;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.condition.Condition;
import co.cask.cdap.etl.api.condition.ConditionContext;
import co.cask.cdap.etl.batch.BatchPipelineSpec;
import co.cask.cdap.etl.batch.BatchPipelineSpecGenerator;
import co.cask.cdap.etl.common.MockPluginConfigurer;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Tests for converting a {@link ETLConfig} into a {@link PipelineSpec}.
 */
public class PipelineSpecGeneratorTest {
  private static final Schema SCHEMA_A = Schema.recordOf("a", Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
  private static final Schema SCHEMA_B = Schema.recordOf("b", Schema.Field.of("b", Schema.of(Schema.Type.STRING)));
  private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
  private static final ETLPlugin MOCK_SOURCE = new ETLPlugin("mocksource", BatchSource.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_TRANSFORM_A = new ETLPlugin("mockA", Transform.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_TRANSFORM_B = new ETLPlugin("mockB", Transform.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_SINK = new ETLPlugin("mocksink", BatchSink.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_JOINER = new ETLPlugin("mockjoiner", BatchJoiner.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_ERROR = new ETLPlugin("mockerror", ErrorTransform.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_ACTION = new ETLPlugin("mockaction", Action.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_CONDITION = new ETLPlugin("mockcondition", Condition.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_SPLITTER = new ETLPlugin("mocksplit", SplitterTransform.PLUGIN_TYPE, EMPTY_MAP);
  private static final ArtifactId ARTIFACT_ID =
    new ArtifactId("plugins", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
  private static BatchPipelineSpecGenerator specGenerator;

  @BeforeClass
  public static void setupTests() {
    // populate some mock plugins.
    MockPluginConfigurer pluginConfigurer = new MockPluginConfigurer();
    Set<ArtifactId> artifactIds = ImmutableSet.of(ARTIFACT_ID);
    pluginConfigurer.addMockPlugin(BatchSource.PLUGIN_TYPE, "mocksource",
                                   MockPlugin.builder().setOutputSchema(SCHEMA_A).build(), artifactIds);
    pluginConfigurer.addMockPlugin(Transform.PLUGIN_TYPE, "mockA",
                                   MockPlugin.builder().setOutputSchema(SCHEMA_A).setErrorSchema(SCHEMA_B).build(),
                                   artifactIds);
    pluginConfigurer.addMockPlugin(Transform.PLUGIN_TYPE, "mockB",
                                   MockPlugin.builder().setOutputSchema(SCHEMA_B).build(), artifactIds);
    pluginConfigurer.addMockPlugin(BatchSink.PLUGIN_TYPE, "mocksink", MockPlugin.builder().build(), artifactIds);
    pluginConfigurer.addMockPlugin(Action.PLUGIN_TYPE, "mockaction", MockPlugin.builder().build(), artifactIds);
    pluginConfigurer.addMockPlugin(Condition.PLUGIN_TYPE, "mockcondition", MockPlugin.builder().build(), artifactIds);
    pluginConfigurer.addMockPlugin(BatchJoiner.PLUGIN_TYPE, "mockjoiner", MockPlugin.builder().build(), artifactIds);
    pluginConfigurer.addMockPlugin(ErrorTransform.PLUGIN_TYPE, "mockerror", MockPlugin.builder().build(), artifactIds);
    pluginConfigurer.addMockPlugin(SplitterTransform.PLUGIN_TYPE, "mocksplit",
                                   new MockSplitter(ImmutableMap.of("portA", SCHEMA_A, "portB", SCHEMA_B)),
                                   artifactIds);


    specGenerator = new BatchPipelineSpecGenerator(pluginConfigurer,
                                                   ImmutableSet.of(BatchSource.PLUGIN_TYPE),
                                                   ImmutableSet.of(BatchSink.PLUGIN_TYPE),
                                                   FileSet.class, DatasetProperties.EMPTY,
                                                   Engine.MAPREDUCE);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testUniqueStageNames() {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_B))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("source", "sink")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConnectionWithMissingStage() {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("source", "sink")
      .addConnection("source", "stage2")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConnectionIntoSource() {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addStage(new ETLStage("transform", MOCK_TRANSFORM_A))
      .addConnection("source", "sink")
      .addConnection("transform", "source")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConnectionOutOfSink() {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addStage(new ETLStage("transform", MOCK_TRANSFORM_A))
      .addConnection("source", "sink")
      .addConnection("sink", "transform")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnreachableStage() {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addStage(new ETLStage("transform", MOCK_TRANSFORM_A))
      .addConnection("source", "sink")
      .addConnection("transform", "sink")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDeadEndStage() {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addStage(new ETLStage("transform", MOCK_TRANSFORM_A))
      .addConnection("source", "sink")
      .addConnection("source", "transform")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalStateException.class)
  public void testCycle() {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t2", MOCK_TRANSFORM_A))
      .addConnection("source", "t1")
      .addConnection("t1", "t2")
      .addConnection("t2", "t1")
      .addConnection("t2", "sink")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test
  public void testGenerateSpec() {
    /*
     *           ---- t1 ------------
     *           |            |      |
     * source ---             |      |--- t3 --- sink1
     *           |            |      |
     *           ------------ t2 --------------- sink2
     *           |                        |
     *           |                        |
     *           -------------------------
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink1", MOCK_SINK))
      .addStage(new ETLStage("sink2", MOCK_SINK))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t2", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t3", MOCK_TRANSFORM_B))
      .addConnection("source", "t1")
      .addConnection("source", "t2")
      .addConnection("source", "sink2")
      .addConnection("t1", "t2")
      .addConnection("t1", "t3")
      .addConnection("t1", "sink2")
      .addConnection("t2", "sink2")
      .addConnection("t2", "t3")
      .addConnection("t3", "sink1")
      .build();
    // test the spec generated is correct, with the right input and output schemas and artifact information.
    BatchPipelineSpec actual = specGenerator.generateSpec(etlConfig);
    Map<String, String> emptyMap = ImmutableMap.of();

    PipelineSpec expected = BatchPipelineSpec.builder()
      .addStage(
        StageSpec.builder("source", new PluginSpec(BatchSource.PLUGIN_TYPE, "mocksource", emptyMap, ARTIFACT_ID))
          .addOutputSchema(SCHEMA_A, "t1", "t2", "sink2")
          .build())
      .addStage(
        StageSpec.builder("sink1", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", emptyMap, ARTIFACT_ID))
          .addInputSchema("t3", SCHEMA_B)
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("sink2", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", emptyMap, ARTIFACT_ID))
          .addInputSchemas(ImmutableMap.of("t1", SCHEMA_A, "t2", SCHEMA_A, "source", SCHEMA_A))
          .setErrorSchema(SCHEMA_A)
          .build())
      .addStage(
        StageSpec.builder("t1", new PluginSpec(Transform.PLUGIN_TYPE, "mockA", emptyMap, ARTIFACT_ID))
          .addInputSchema("source", SCHEMA_A)
          .addOutputSchema(SCHEMA_A, "t2", "t3", "sink2")
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("t2", new PluginSpec(Transform.PLUGIN_TYPE, "mockA", emptyMap, ARTIFACT_ID))
          .addInputSchemas(ImmutableMap.of("source", SCHEMA_A, "t1", SCHEMA_A))
          .addOutputSchema(SCHEMA_A, "t3", "sink2")
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("t3", new PluginSpec(Transform.PLUGIN_TYPE, "mockB", emptyMap, ARTIFACT_ID))
          .addInputSchemas(ImmutableMap.of("t1", SCHEMA_A, "t2", SCHEMA_A))
          .addOutputSchema(SCHEMA_B, "sink1")
          .setErrorSchema(SCHEMA_A)
          .build())
      .addConnections(etlConfig.getConnections())
      .setResources(etlConfig.getResources())
      .setDriverResources(new Resources(1024, 1))
      .setClientResources(new Resources(1024, 1))
      .setStageLoggingEnabled(etlConfig.isStageLoggingEnabled())
      .build();
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDifferentInputSchemasForAction() {
    /*
     *           ---- transformA ---- sinkA ----
     *           |                             |
     * source ---                              |--- action
     *           |                             |
     *           ---- transformB ---- sinkB ----
     */
    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("tA", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("tB", MOCK_TRANSFORM_B))
      .addStage(new ETLStage("sinkA", MOCK_SINK))
      .addStage(new ETLStage("sinkB", MOCK_SINK))
      .addStage(new ETLStage("action", MOCK_ACTION))
      .addConnection("source", "tA")
      .addConnection("source", "tB")
      .addConnection("tA", "sinkA")
      .addConnection("tB", "sinkB")
      .addConnection("sinkA", "action")
      .addConnection("sinkB", "action")
      .build();
    PipelineSpec actual = specGenerator.generateSpec(config);

    Map<String, String> emptyMap = ImmutableMap.of();
    PipelineSpec expected = BatchPipelineSpec.builder()
      .addStage(
        StageSpec.builder("source", new PluginSpec(BatchSource.PLUGIN_TYPE, "mocksource", emptyMap, ARTIFACT_ID))
          .addOutputSchema(SCHEMA_A, "tA", "tB")
          .build())
      .addStage(
        StageSpec.builder("sinkA", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", emptyMap, ARTIFACT_ID))
          .addInputSchema("tA", SCHEMA_A)
          .addOutputSchema(null, "action")
          .setErrorSchema(SCHEMA_A)
          .build())
      .addStage(
        StageSpec.builder("sinkB", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", emptyMap, ARTIFACT_ID))
          .addInputSchema("tB", SCHEMA_B)
          .addOutputSchema(null, "action")
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("tA", new PluginSpec(Transform.PLUGIN_TYPE, "mockA", emptyMap, ARTIFACT_ID))
          .addInputSchema("source", SCHEMA_A)
          .addOutputSchema(SCHEMA_A, "sinkA")
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("tB", new PluginSpec(Transform.PLUGIN_TYPE, "mockB", emptyMap, ARTIFACT_ID))
          .addInputSchema("source", SCHEMA_A)
          .addOutputSchema(SCHEMA_B, "sinkB")
          .setErrorSchema(SCHEMA_A)
          .build())
      .addStage(
        StageSpec.builder("action", new PluginSpec(Action.PLUGIN_TYPE, "mockaction", emptyMap, ARTIFACT_ID))
          .addInputSchema("sinkA", null)
          .addInputSchema("sinkB", null)
          .build())
      .addConnections(config.getConnections())
      .setResources(config.getResources())
      .setDriverResources(config.getDriverResources())
      .setClientResources(config.getClientResources())
      .setStageLoggingEnabled(config.isStageLoggingEnabled())
      .build();

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSingleAction() {
    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("action", MOCK_ACTION))
      .build();
    PipelineSpec actual = specGenerator.generateSpec(config);

    Map<String, String> emptyMap = ImmutableMap.of();
    PipelineSpec expected = BatchPipelineSpec.builder()
      .addStage(
        StageSpec.builder("action", new PluginSpec(Action.PLUGIN_TYPE, "mockaction", emptyMap, ARTIFACT_ID)).build())
      .setResources(config.getResources())
      .setDriverResources(config.getDriverResources())
      .setClientResources(config.getClientResources())
      .setStageLoggingEnabled(config.isStageLoggingEnabled())
      .build();

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testOutputPorts() {
    /*
     *
     *                    |portA --> sinkA
     *                    |
     * source --> split --|portB --> sinkB
     *                    |
     *                    |portC --> sinkC
     *
     * portA has output schemaA, portB has output schemaB, portC has null output schema
     */
    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("split", MOCK_SPLITTER))
      .addStage(new ETLStage("sinkA", MOCK_SINK))
      .addStage(new ETLStage("sinkB", MOCK_SINK))
      .addStage(new ETLStage("sinkC", MOCK_SINK))
      .addConnection("source", "split")
      .addConnection("source", "split")
      .addConnection("split", "sinkA", "portA")
      .addConnection("split", "sinkB", "portB")
      .addConnection("split", "sinkC", "portC")
      .build();

    PipelineSpec expected = BatchPipelineSpec.builder()
      .addStage(
        StageSpec.builder("source", new PluginSpec(BatchSource.PLUGIN_TYPE, "mocksource", EMPTY_MAP, ARTIFACT_ID))
          .addOutputSchema(SCHEMA_A, "split")
          .build())
      .addStage(
        StageSpec.builder("split", new PluginSpec(SplitterTransform.PLUGIN_TYPE, "mocksplit", EMPTY_MAP, ARTIFACT_ID))
          .addInputSchema("source", SCHEMA_A)
          .addOutputPortSchema("sinkA", "portA", SCHEMA_A)
          .addOutputPortSchema("sinkB", "portB", SCHEMA_B)
          .addOutputPortSchema("sinkC", "portC", null)
          .setErrorSchema(SCHEMA_A)
          .build())
      .addStage(
        StageSpec.builder("sinkA", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", EMPTY_MAP, ARTIFACT_ID))
          .addInputSchema("split", SCHEMA_A)
          .setErrorSchema(SCHEMA_A)
          .build())
      .addStage(
        StageSpec.builder("sinkB", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", EMPTY_MAP, ARTIFACT_ID))
          .addInputSchema("split", SCHEMA_B)
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("sinkC", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", EMPTY_MAP, ARTIFACT_ID))
          .addInputSchema("split", null)
          .build())
      .addConnections(config.getConnections())
      .setResources(config.getResources())
      .setDriverResources(config.getDriverResources())
      .setClientResources(config.getClientResources())
      .setStageLoggingEnabled(config.isStageLoggingEnabled())
      .build();

    PipelineSpec actual = specGenerator.generateSpec(config);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testConditionSchemaPropagation() {
    /*
     * source --> condition --> sink
     */
    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("cond", MOCK_CONDITION))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("source", "cond")
      .addConnection("cond", "sink", true)
      .build();

    PipelineSpec expected = BatchPipelineSpec.builder()
      .addStage(
        StageSpec.builder("source", new PluginSpec(BatchSource.PLUGIN_TYPE, "mocksource", EMPTY_MAP, ARTIFACT_ID))
          .addOutputSchema(SCHEMA_A, "cond")
          .build())
      .addStage(
        StageSpec.builder("cond", new PluginSpec(Condition.PLUGIN_TYPE, "mockcondition", EMPTY_MAP, ARTIFACT_ID))
          .addInputSchema("source", SCHEMA_A)
          .addOutputPortSchema("sink", null, SCHEMA_A)
          .setErrorSchema(SCHEMA_A)
          .build())
      .addStage(
        StageSpec.builder("sink", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", EMPTY_MAP, ARTIFACT_ID))
          .addInputSchema("cond", SCHEMA_A)
          .setErrorSchema(SCHEMA_A)
          .build())
      .addConnections(config.getConnections())
      .setResources(config.getResources())
      .setDriverResources(config.getDriverResources())
      .setClientResources(config.getClientResources())
      .setStageLoggingEnabled(config.isStageLoggingEnabled())
      .build();

    PipelineSpec actual = specGenerator.generateSpec(config);
    Assert.assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConflictingInputSchemasCondition() {
    /*
     *           ---- transformA ----
     *           |                  |
     * source ---                   |--- condition -- sink
     *           |                  |
     *           ---- transformB ----
     *
     * sink gets schema A and schema B as input, should fail
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addStage(new ETLStage("tA", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("tB", MOCK_TRANSFORM_B))
      .addStage(new ETLStage("cond", MOCK_CONDITION))
      .addConnection("source", "tA")
      .addConnection("source", "tB")
      .addConnection("tA", "cond")
      .addConnection("tB", "cond")
      .addConnection("cond", "sink", true)
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConflictingInputSchemas() {
    /*
     *           ---- transformA ----
     *           |                  |
     * source ---                   |--- sink
     *           |                  |
     *           ---- transformB ----
     *
     * sink gets schema A and schema B as input, should fail
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addStage(new ETLStage("tA", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("tB", MOCK_TRANSFORM_B))
      .addConnection("source", "tA")
      .addConnection("source", "tB")
      .addConnection("tA", "sink")
      .addConnection("tB", "sink")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConflictingInputErrorSchemas() {
    /*
     *           ---- transformA
     *           |        |
     * source ---|        |------- error -- sink
     *           |        |
     *           ---- transformB
     *
     * error gets schema B from transformA and schema A from transformB, should fail
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addStage(new ETLStage("tA", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("tB", MOCK_TRANSFORM_B))
      .addStage(new ETLStage("error", MOCK_ERROR))
      .addConnection("source", "tA")
      .addConnection("source", "tB")
      .addConnection("tA", "error")
      .addConnection("tB", "error")
      .addConnection("error", "sink")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadErrorTransformInput() {
    /*
     * source --> joiner --> error --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("joiner", MOCK_JOINER))
      .addStage(new ETLStage("error", MOCK_ERROR))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("source", "joiner")
      .addConnection("joiner", "error")
      .addConnection("error", "sink")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConflictingPipelineProperties() {
    // populate some mock plugins.
    MockPluginConfigurer pluginConfigurer = new MockPluginConfigurer();
    Set<ArtifactId> artifactIds = ImmutableSet.of(ARTIFACT_ID);
    pluginConfigurer.addMockPlugin(Action.PLUGIN_TYPE, "action1",
                                   MockPlugin.builder().putPipelineProperty("prop1", "val1").build(), artifactIds);
    pluginConfigurer.addMockPlugin(Action.PLUGIN_TYPE, "action2",
                                   MockPlugin.builder().putPipelineProperty("prop1", "val2").build(), artifactIds);

    BatchPipelineSpecGenerator specGenerator = new BatchPipelineSpecGenerator(pluginConfigurer,
                                                                              ImmutableSet.of(BatchSource.PLUGIN_TYPE),
                                                                              ImmutableSet.of(BatchSink.PLUGIN_TYPE),
                                                                              FileSet.class, DatasetProperties.EMPTY,
                                                                              Engine.MAPREDUCE);

    Map<String, String> empty = ImmutableMap.of();
    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("a1", new ETLPlugin("action1", Action.PLUGIN_TYPE, empty)))
      .addStage(new ETLStage("a2", new ETLPlugin("action2", Action.PLUGIN_TYPE, empty)))
      .addConnection("a1", "a2")
      .setEngine(Engine.MAPREDUCE)
      .build();
    specGenerator.generateSpec(config);
  }

  @Test
  public void testPipelineProperties() {
    // populate some mock plugins.
    MockPluginConfigurer pluginConfigurer = new MockPluginConfigurer();
    Set<ArtifactId> artifactIds = ImmutableSet.of(ARTIFACT_ID);
    pluginConfigurer.addMockPlugin(Action.PLUGIN_TYPE, "action1",
                                   MockPlugin.builder()
                                     .putPipelineProperty("prop1", "val1")
                                     .putPipelineProperty("prop2", "val2").build(), artifactIds);
    pluginConfigurer.addMockPlugin(Action.PLUGIN_TYPE, "action2",
                                   MockPlugin.builder().putPipelineProperty("prop2", "val2").build(), artifactIds);

    BatchPipelineSpecGenerator specGenerator = new BatchPipelineSpecGenerator(pluginConfigurer,
                                                                              ImmutableSet.of(BatchSource.PLUGIN_TYPE),
                                                                              ImmutableSet.of(BatchSink.PLUGIN_TYPE),
                                                                              FileSet.class, DatasetProperties.EMPTY,
                                                                              Engine.MAPREDUCE);

    Map<String, String> empty = ImmutableMap.of();
    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .setProperties(ImmutableMap.of("system.spark.spark.test", "abc", "system.mapreduce.prop3", "val3"))
      .addStage(new ETLStage("a1", new ETLPlugin("action1", Action.PLUGIN_TYPE, empty)))
      .addStage(new ETLStage("a2", new ETLPlugin("action2", Action.PLUGIN_TYPE, empty)))
      .addConnection("a1", "a2")
      .setEngine(Engine.MAPREDUCE)
      .build();
    PipelineSpec actual = specGenerator.generateSpec(config);
    PipelineSpec expected = BatchPipelineSpec.builder()
      .addConnection("a1", "a2")
      // properties should not include the spark property, but should include the one from the config
      // plus the ones from the plugins
      .setProperties(ImmutableMap.of("prop1", "val1", "prop2", "val2", "prop3", "val3"))
      .addStage(StageSpec.builder("a1", new PluginSpec(Action.PLUGIN_TYPE, "action1", empty, ARTIFACT_ID))
                  .addOutputSchema(null, "a2")
                  .build())
      .addStage(StageSpec.builder("a2", new PluginSpec(Action.PLUGIN_TYPE, "action2", empty, ARTIFACT_ID))
                  .addInputSchema("a1", null)
                  .build())
      .setResources(new Resources(1024))
      .setDriverResources(new Resources(1024))
      .setClientResources(new Resources(1024))
      .build();
    Assert.assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSimpleConditionConnectionWithNoBranchInfo() {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("condition", MOCK_CONDITION))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("source", "condition")
      .addConnection("condition", "t1")
      .addConnection("t1", "sink")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSimpleConditionConnectionWithMultipleTrueBranches() {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("condition", MOCK_CONDITION))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t2", MOCK_TRANSFORM_B))
      .addStage(new ETLStage("sink1", MOCK_SINK))
      .addStage(new ETLStage("sink2", MOCK_SINK))
      .addConnection("source", "condition")
      .addConnection("condition", "t1", true)
      .addConnection("condition", "t2", true)
      .addConnection("t1", "sink1")
      .addConnection("t2", "sink2")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNestedConditionConnectionWithNoBranchInfo() {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("condition1", MOCK_CONDITION))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("condition2", MOCK_CONDITION))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("source", "condition1")
      .addConnection("condition1", "t1", true)
      .addConnection("t1", "condition2")
      .addConnection("condition2", "sink")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNestedConditionConnectionWithMultipleTrueBranches() {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("condition1", MOCK_CONDITION))
      .addStage(new ETLStage("condition2", MOCK_CONDITION))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t11", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t12", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t2", MOCK_TRANSFORM_B))
      .addStage(new ETLStage("sink1", MOCK_SINK))
      .addStage(new ETLStage("sink2", MOCK_SINK))
      .addConnection("source", "condition1")
      .addConnection("condition1", "t1", true)
      .addConnection("t1", "condition2")
      .addConnection("condition2", "t11", false)
      .addConnection("condition2", "t12", false)
      .addConnection("condition1", "t2", false)
      .addConnection("t11", "sink1")
      .addConnection("t12", "sink1")
      .addConnection("t2", "sink2")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSimpleConditionWithCrossConnection() {

    //  source1--condition-----t1-----sink
    //                                  |
    //                    source2-------

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source1", MOCK_SOURCE))
      .addStage(new ETLStage("source2", MOCK_SOURCE))
      .addStage(new ETLStage("condition", MOCK_CONDITION))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("source1", "condition")
      .addConnection("condition", "t1", true)
      .addConnection("t1", "sink")
      .addConnection("source2", "sink")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNestedConditionWithCrossConnection() {
    //
    //                                anothersource-------------
    //                                                          |
    //  source--condition1-----t1-----condition2------t11------sink1
    //             |                      |                     |
    //             |                      |-----------t12--------
    //             t2---------sink2

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("anothersource", MOCK_SOURCE))
      .addStage(new ETLStage("condition1", MOCK_CONDITION))
      .addStage(new ETLStage("condition2", MOCK_CONDITION))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t11", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t12", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t2", MOCK_TRANSFORM_B))
      .addStage(new ETLStage("sink1", MOCK_SINK))
      .addStage(new ETLStage("sink2", MOCK_SINK))
      .addConnection("source", "condition1")
      .addConnection("condition1", "t1", true)
      .addConnection("t1", "condition2")
      .addConnection("condition2", "t11", false)
      .addConnection("condition2", "t12", true)
      .addConnection("condition1", "t2", false)
      .addConnection("t11", "sink1")
      .addConnection("t12", "sink1")
      .addConnection("anothersource", "sink1")
      .addConnection("t2", "sink2")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test
  public void testSimpleValidCondition() {

    //  source--condition-----t1-----sink

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("condition", MOCK_CONDITION))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("source", "condition")
      .addConnection("condition", "t1", true)
      .addConnection("t1", "sink")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test
  public void testNestedValidCondition() {

    //  source--condition1-----t1-----condition2------t11------sink1
    //             |                      |                     |
    //             |                      |-----------t12--------
    //             t2---------sink2

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("condition1", MOCK_CONDITION))
      .addStage(new ETLStage("condition2", MOCK_CONDITION))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t11", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t12", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t2", MOCK_TRANSFORM_B))
      .addStage(new ETLStage("sink1", MOCK_SINK))
      .addStage(new ETLStage("sink2", MOCK_SINK))
      .addConnection("source", "condition1")
      .addConnection("condition1", "t1", true)
      .addConnection("t1", "condition2")
      .addConnection("condition2", "t11", false)
      .addConnection("condition2", "t12", true)
      .addConnection("condition1", "t2", false)
      .addConnection("t11", "sink1")
      .addConnection("t12", "sink1")
      .addConnection("t2", "sink2")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTwoConditionsGoingToSameStage() {

    //  source--condition1-----t1-----condition2------t11------sink1
    //             |                      |                     |
    //             |                      |-----------t12--------
    //             t2---------sink2                   |
    //                                                |
    //                                                |
    //  anotherSource--------->condition3----------------------sink3

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("anotherSource", MOCK_SOURCE))
      .addStage(new ETLStage("condition1", MOCK_CONDITION))
      .addStage(new ETLStage("condition2", MOCK_CONDITION))
      .addStage(new ETLStage("condition3", MOCK_CONDITION))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t11", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t12", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t2", MOCK_TRANSFORM_B))
      .addStage(new ETLStage("sink1", MOCK_SINK))
      .addStage(new ETLStage("sink2", MOCK_SINK))
      .addStage(new ETLStage("sink3", MOCK_SINK))
      .addConnection("source", "condition1")
      .addConnection("condition1", "t1", true)
      .addConnection("t1", "condition2")
      .addConnection("condition2", "t11", false)
      .addConnection("condition2", "t12", true)
      .addConnection("condition1", "t2", false)
      .addConnection("t11", "sink1")
      .addConnection("t12", "sink1")
      .addConnection("t2", "sink2")
      .addConnection("anotherSource", "condition3")
      .addConnection("condition3", "sink3", false)
      .addConnection("condition3", "t12", true)
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  private static class MockSplitter implements MultiOutputPipelineConfigurable {
    private final Map<String, Schema> outputSchemas;

    public MockSplitter(Map<String, Schema> outputSchemas) {
      this.outputSchemas = outputSchemas;
    }

    @Override
    public void configurePipeline(MultiOutputPipelineConfigurer multiOutputPipelineConfigurer) {
      multiOutputPipelineConfigurer.getMultiOutputStageConfigurer().setOutputSchemas(outputSchemas);
    }
  }

  private static class MockPlugin implements PipelineConfigurable {
    private final Schema outputSchema;
    private final Schema errorSchema;
    private final Map<String, String> pipelineProperties;

    private MockPlugin(@Nullable Schema outputSchema, @Nullable Schema errorSchema,
                       Map<String, String> pipelineProperties) {
      this.outputSchema = outputSchema;
      this.errorSchema = errorSchema;
      this.pipelineProperties = ImmutableMap.copyOf(pipelineProperties);
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
      if (outputSchema != null) {
        pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
      }
      if (errorSchema != null) {
        pipelineConfigurer.getStageConfigurer().setErrorSchema(errorSchema);
      }
      pipelineConfigurer.setPipelineProperties(pipelineProperties);
    }

    private static Builder builder() {
      return new Builder();
    }

    private static class Builder {
      private Schema outputSchema;
      private Schema errorSchema;
      private Map<String, String> pipelineProperties = new HashMap<>();

      public Builder setOutputSchema(Schema schema) {
        outputSchema = schema;
        return this;
      }

      public Builder setErrorSchema(Schema schema) {
        errorSchema = schema;
        return this;
      }

      public Builder putPipelineProperty(String name, String val) {
        pipelineProperties.put(name, val);
        return this;
      }

      public MockPlugin build() {
        return new MockPlugin(outputSchema, errorSchema, pipelineProperties);
      }
    }
  }
}
