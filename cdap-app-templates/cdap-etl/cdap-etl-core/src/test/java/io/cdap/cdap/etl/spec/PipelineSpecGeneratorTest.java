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

package io.cdap.cdap.etl.spec;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.ErrorTransform;
import io.cdap.cdap.etl.api.MultiOutputPipelineConfigurable;
import io.cdap.cdap.etl.api.MultiOutputPipelineConfigurer;
import io.cdap.cdap.etl.api.PipelineConfigurable;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.condition.Condition;
import io.cdap.cdap.etl.api.engine.sql.BatchSQLEngine;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPullDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPushDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinDefinition;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.batch.BatchPipelineSpec;
import io.cdap.cdap.etl.batch.BatchPipelineSpecGenerator;
import io.cdap.cdap.etl.common.MockPluginConfigurer;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.proto.v2.ETLTransformationPushdown;
import io.cdap.cdap.etl.proto.v2.spec.PipelineSpec;
import io.cdap.cdap.etl.proto.v2.spec.PluginSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Tests for converting a {@link ETLConfig} into a {@link PipelineSpec}.
 */
public class PipelineSpecGeneratorTest {
  private static final Schema SCHEMA_A = Schema.recordOf("a", Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
  private static final Schema SCHEMA_A2 = Schema.recordOf("a2", Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
  private static final Schema SCHEMA_B = Schema.recordOf("b", Schema.Field.of("b", Schema.of(Schema.Type.STRING)));
  private static final Schema SCHEMA_ABC = Schema.recordOf("abc",
                                                           Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                           Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                           Schema.Field.of("c", Schema.of(Schema.Type.INT)));
  private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
  private static final ETLPlugin MOCK_SOURCE = new ETLPlugin("mocksource", BatchSource.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_SOURCE2 = new ETLPlugin("mocksource2", BatchSource.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_TRANSFORM_A = new ETLPlugin("mockA", Transform.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_TRANSFORM_B = new ETLPlugin("mockB", Transform.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_TRANSFORM_ABC = new ETLPlugin("mockABC", Transform.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_SINK = new ETLPlugin("mocksink", BatchSink.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_JOINER = new ETLPlugin("mockjoiner", BatchJoiner.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_AUTO_JOINER = new ETLPlugin("mockautojoiner", BatchJoiner.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_ERROR = new ETLPlugin("mockerror", ErrorTransform.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_ACTION = new ETLPlugin("mockaction", Action.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_CONDITION = new ETLPlugin("mockcondition", Condition.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_SPLITTER = new ETLPlugin("mocksplit", SplitterTransform.PLUGIN_TYPE, EMPTY_MAP);
  private static final ETLPlugin MOCK_SQL_ENGINE = new ETLPlugin("mocksqlengine", BatchSQLEngine.PLUGIN_TYPE,
                                                                 EMPTY_MAP);
  private static final ArtifactId ARTIFACT_ID =
    new ArtifactId("plugins", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
  private static JoinDefinition joinDefinition;
  private static BatchPipelineSpecGenerator specGenerator;

  @BeforeClass
  public static void setupTests() {
    // populate some mock plugins.
    MockPluginConfigurer pluginConfigurer = new MockPluginConfigurer();
    Set<ArtifactId> artifactIds = ImmutableSet.of(ARTIFACT_ID);
    pluginConfigurer.addMockPlugin(BatchSource.PLUGIN_TYPE, "mocksource",
                                   MockPlugin.builder().setOutputSchema(SCHEMA_A).build(), artifactIds);
    pluginConfigurer.addMockPlugin(BatchSource.PLUGIN_TYPE, "mocksource2",
                                   MockPlugin.builder().setOutputSchema(SCHEMA_A2).build(), artifactIds);
    pluginConfigurer.addMockPlugin(Transform.PLUGIN_TYPE, "mockA",
                                   MockPlugin.builder().setOutputSchema(SCHEMA_A).setErrorSchema(SCHEMA_B).build(),
                                   artifactIds);
    pluginConfigurer.addMockPlugin(Transform.PLUGIN_TYPE, "mockB",
                                   MockPlugin.builder().setOutputSchema(SCHEMA_B).build(), artifactIds);
    pluginConfigurer.addMockPlugin(Transform.PLUGIN_TYPE, "mockABC",
                                   MockPlugin.builder().setOutputSchema(SCHEMA_ABC).build(), artifactIds);
    pluginConfigurer.addMockPlugin(BatchSink.PLUGIN_TYPE, "mocksink", MockPlugin.builder().build(), artifactIds);
    pluginConfigurer.addMockPlugin(Action.PLUGIN_TYPE, "mockaction", MockPlugin.builder().build(), artifactIds);
    pluginConfigurer.addMockPlugin(Condition.PLUGIN_TYPE, "mockcondition", MockPlugin.builder().build(), artifactIds);
    pluginConfigurer.addMockPlugin(BatchJoiner.PLUGIN_TYPE, "mockjoiner", MockPlugin.builder().build(), artifactIds);
    pluginConfigurer.addMockPlugin(ErrorTransform.PLUGIN_TYPE, "mockerror", MockPlugin.builder().build(), artifactIds);
    pluginConfigurer.addMockPlugin(SplitterTransform.PLUGIN_TYPE, "mocksplit",
                                   new MockSplitter(ImmutableMap.of("portA", SCHEMA_A, "portB", SCHEMA_B)),
                                   artifactIds);
    pluginConfigurer.addMockPlugin(BatchJoiner.PLUGIN_TYPE, "mockautojoiner", new MockAutoJoin(), artifactIds);
    pluginConfigurer.addMockPlugin(BatchSQLEngine.PLUGIN_TYPE, "mocksqlengine", new MockSQLEngine(), artifactIds);

    specGenerator = new BatchPipelineSpecGenerator(NamespaceId.DEFAULT.getNamespace(), pluginConfigurer,
                                                   null, ImmutableSet.of(BatchSource.PLUGIN_TYPE),
                                                   ImmutableSet.of(BatchSink.PLUGIN_TYPE),
                                                   Engine.MAPREDUCE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUniqueStageNames() throws ValidationException {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_B))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("source", "sink")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConnectionWithMissingStage() throws ValidationException {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("source", "sink")
      .addConnection("source", "stage2")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConnectionIntoSource() throws ValidationException {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addStage(new ETLStage("transform", MOCK_TRANSFORM_A))
      .addConnection("source", "sink")
      .addConnection("transform", "source")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConnectionOutOfSink() throws ValidationException {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addStage(new ETLStage("transform", MOCK_TRANSFORM_A))
      .addConnection("source", "sink")
      .addConnection("sink", "transform")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnreachableStage() throws ValidationException {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addStage(new ETLStage("transform", MOCK_TRANSFORM_A))
      .addConnection("source", "sink")
      .addConnection("transform", "sink")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDeadEndStage() throws ValidationException {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addStage(new ETLStage("transform", MOCK_TRANSFORM_A))
      .addConnection("source", "sink")
      .addConnection("source", "transform")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test(expected = IllegalStateException.class)
  public void testCycle() throws ValidationException {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
  public void testGenerateSpec() throws ValidationException {
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
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
      .setNumOfRecordsPreview(100)
      .build();
    // test the spec generated is correct, with the right input and output schemas and artifact information.
    BatchPipelineSpec actual = specGenerator.generateSpec(etlConfig);
    Map<String, String> emptyMap = ImmutableMap.of();

    PipelineSpec expected = BatchPipelineSpec.builder()
      .addStage(
        StageSpec.builder("source", new PluginSpec(BatchSource.PLUGIN_TYPE, "mocksource", emptyMap, ARTIFACT_ID))
          .addOutput(SCHEMA_A, "t1", "t2", "sink2")
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
          .addOutput(SCHEMA_A, "t2", "t3", "sink2")
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("t2", new PluginSpec(Transform.PLUGIN_TYPE, "mockA", emptyMap, ARTIFACT_ID))
          .addInputSchemas(ImmutableMap.of("source", SCHEMA_A, "t1", SCHEMA_A))
          .addOutput(SCHEMA_A, "t3", "sink2")
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("t3", new PluginSpec(Transform.PLUGIN_TYPE, "mockB", emptyMap, ARTIFACT_ID))
          .addInputSchemas(ImmutableMap.of("t1", SCHEMA_A, "t2", SCHEMA_A))
          .addOutput(SCHEMA_B, "sink1")
          .setErrorSchema(SCHEMA_A)
          .build())
      .addConnections(etlConfig.getConnections())
      .setResources(etlConfig.getResources())
      .setDriverResources(new Resources(1024, 1))
      .setClientResources(new Resources(1024, 1))
      .setStageLoggingEnabled(etlConfig.isStageLoggingEnabled())
      .setNumOfRecordsPreview(etlConfig.getNumOfRecordsPreview())
      .build();
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testInputSchemasWithDifferentName() {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(new ETLStage("s1", MOCK_SOURCE))
      .addStage(new ETLStage("s2", MOCK_SOURCE2))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("s1", "sink")
      .addConnection("s2", "sink")
      .setNumOfRecordsPreview(100)
      .build();

    Map<String, String> emptyMap = Collections.emptyMap();
    PipelineSpec expected = BatchPipelineSpec.builder()
      .addStage(
        StageSpec.builder("s1", new PluginSpec(BatchSource.PLUGIN_TYPE, "mocksource", emptyMap, ARTIFACT_ID))
          .addOutput(SCHEMA_A, "sink")
          .build())
      .addStage(
        StageSpec.builder("s2", new PluginSpec(BatchSource.PLUGIN_TYPE, "mocksource2", emptyMap, ARTIFACT_ID))
          .addOutput(SCHEMA_A2, "sink")
          .build())
      .addStage(
        StageSpec.builder("sink", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", emptyMap, ARTIFACT_ID))
          .addInputSchemas(ImmutableMap.of("s1", SCHEMA_A, "s2", SCHEMA_A2))
          .setErrorSchema(SCHEMA_A)
          .build())
      .addConnections(etlConfig.getConnections())
      .setResources(etlConfig.getResources())
      .setDriverResources(new Resources(1024, 1))
      .setClientResources(new Resources(1024, 1))
      .setStageLoggingEnabled(etlConfig.isStageLoggingEnabled())
      .setNumOfRecordsPreview(etlConfig.getNumOfRecordsPreview())
      .build();

    PipelineSpec actual = specGenerator.generateSpec(etlConfig);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testActionInPipelineMiddle() throws ValidationException {
    /*
     * source1 --> sink1 --> action --> source2 --> sink2
     */
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("source1", MOCK_SOURCE))
      .addStage(new ETLStage("source2", MOCK_SOURCE))
      .addStage(new ETLStage("sink1", MOCK_SINK))
      .addStage(new ETLStage("sink2", MOCK_SINK))
      .addStage(new ETLStage("action", MOCK_ACTION))
      .addConnection("source1", "sink1")
      .addConnection("sink1", "action")
      .addConnection("action", "source2")
      .addConnection("source2", "sink2")
      .build();
    try {
      specGenerator.generateSpec(config);
      Assert.fail("Did not fail a pipeline with an action in the middle");
    } catch (IllegalArgumentException e) {
      // expected
    }


    /*
     * action1 --> source --> action2 --> sink --> action3
     */
    config = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addStage(new ETLStage("action1", MOCK_ACTION))
      .addStage(new ETLStage("action2", MOCK_ACTION))
      .addStage(new ETLStage("action3", MOCK_ACTION))
      .addConnection("action1", "source")
      .addConnection("source", "action2")
      .addConnection("action2", "sink")
      .addConnection("sink", "action3")
      .build();
    try {
      specGenerator.generateSpec(config);
      Assert.fail("Did not fail a pipeline with an action in the middle");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testSourceInPipelineMiddle() throws ValidationException {
    /*
     * source1 --> t1 --> source2 --> t2 --> sink
     */
    ETLBatchConfig config = ETLBatchConfig.builder()
            .addStage(new ETLStage("source1", MOCK_SOURCE))
            .addStage(new ETLStage("source2", MOCK_SOURCE))
            .addStage(new ETLStage("sink", MOCK_SINK))
            .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
            .addStage(new ETLStage("t2", MOCK_TRANSFORM_B))
            .addConnection("source1", "t1")
            .addConnection("t1", "source2")
            .addConnection("source2", "t2")
            .addConnection("t2", "sink")
            .build();
    try {
      specGenerator.generateSpec(config);
      Assert.fail("Did not fail a pipeline with a source in the middle");
    } catch (IllegalArgumentException e) {
      // expected
    }

    /*
     * source1 --> condition --> source2 --> sink1
     *                       --> t1 -> sink2
     */
    config = ETLBatchConfig.builder()
            .addStage(new ETLStage("source1", MOCK_SOURCE))
            .addStage(new ETLStage("source2", MOCK_SOURCE))
            .addStage(new ETLStage("sink1", MOCK_SINK))
            .addStage(new ETLStage("sink2", MOCK_SINK))
            .addStage(new ETLStage("condition", MOCK_CONDITION))
            .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
            .addConnection("source1", "condition")
            .addConnection("condition", "source2", true)
            .addConnection("condition", "t1", false)
            .addConnection("source2", "sink1")
            .addConnection("t1", "sink2")
            .build();
    try {
      specGenerator.generateSpec(config);
      Assert.fail("Did not fail a pipeline with a source after a condition that had a source as input");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testDifferentInputSchemasForAction() throws ValidationException {
    /*
     *           ---- transformA ---- sinkA ----
     *           |                             |
     * source ---                              |--- action
     *           |                             |
     *           ---- transformB ---- sinkB ----
     */
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
      .setNumOfRecordsPreview(100)
      .build();
    PipelineSpec actual = specGenerator.generateSpec(config);

    Map<String, String> emptyMap = ImmutableMap.of();
    PipelineSpec expected = BatchPipelineSpec.builder()
      .addStage(
        StageSpec.builder("source", new PluginSpec(BatchSource.PLUGIN_TYPE, "mocksource", emptyMap, ARTIFACT_ID))
          .addOutput(SCHEMA_A, "tA", "tB")
          .build())
      .addStage(
        StageSpec.builder("sinkA", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", emptyMap, ARTIFACT_ID))
          .addInputSchema("tA", SCHEMA_A)
          .addOutput(null, "action")
          .setErrorSchema(SCHEMA_A)
          .build())
      .addStage(
        StageSpec.builder("sinkB", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", emptyMap, ARTIFACT_ID))
          .addInputSchema("tB", SCHEMA_B)
          .addOutput(null, "action")
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("tA", new PluginSpec(Transform.PLUGIN_TYPE, "mockA", emptyMap, ARTIFACT_ID))
          .addInputSchema("source", SCHEMA_A)
          .addOutput(SCHEMA_A, "sinkA")
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("tB", new PluginSpec(Transform.PLUGIN_TYPE, "mockB", emptyMap, ARTIFACT_ID))
          .addInputSchema("source", SCHEMA_A)
          .addOutput(SCHEMA_B, "sinkB")
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
      .setNumOfRecordsPreview(config.getNumOfRecordsPreview())
      .build();

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSingleAction() throws ValidationException {
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
  public void testOutputPorts() throws ValidationException {
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
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
      .setNumOfRecordsPreview(100)
      .build();

    Map<String, String> emptyMap = Collections.emptyMap();
    PipelineSpec expected = BatchPipelineSpec.builder()
      .addStage(
        StageSpec.builder("source", new PluginSpec(BatchSource.PLUGIN_TYPE, "mocksource", EMPTY_MAP, ARTIFACT_ID))
          .addOutput(SCHEMA_A, "split")
          .build())
      .addStage(
        StageSpec.builder("split", new PluginSpec(SplitterTransform.PLUGIN_TYPE, "mocksplit", EMPTY_MAP, ARTIFACT_ID))
          .addInputSchema("source", SCHEMA_A)
          .addOutput("sinkA", "portA", SCHEMA_A)
          .addOutput("sinkB", "portB", SCHEMA_B)
          .addOutput("sinkC", "portC", null)
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
      .setNumOfRecordsPreview(config.getNumOfRecordsPreview())
      .build();

    PipelineSpec actual = specGenerator.generateSpec(config);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testAutoJoin() {
    /*
     *           ---- transformA --------|
     *           |                       |
     * source ---|                       |-- autojoin --- sink
     *           |                       |
     *           ---- transformABC ------|
     */
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("tA", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("tABC", MOCK_TRANSFORM_ABC))
      .addStage(new ETLStage("autojoin", MOCK_AUTO_JOINER))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("source", "tA")
      .addConnection("source", "tABC")
      .addConnection("tA", "autojoin")
      .addConnection("tABC", "autojoin")
      .addConnection("autojoin", "sink")
      .setNumOfRecordsPreview(100)
      .build();

    joinDefinition = JoinDefinition.builder()
      .select(new JoinField("tA", "a"), new JoinField("tABC", "b"), new JoinField("tABC", "c"))
      .from(JoinStage.builder("tA", SCHEMA_A).isRequired().build(),
            JoinStage.builder("tABC", SCHEMA_ABC).isOptional().build())
      .on(JoinCondition.onKeys()
            .addKey(new JoinKey("tA", Collections.singletonList("a")))
            .addKey(new JoinKey("tABC", Collections.singletonList("a")))
            .build())
      .setOutputSchemaName("abc.joined")
      .build();
    Schema joinSchema = Schema.recordOf("abc.joined",
                                        Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                        Schema.Field.of("b", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                        Schema.Field.of("c", Schema.nullableOf(Schema.of(Schema.Type.INT))));

    Map<String, String> emptyMap = new HashMap<>();
    PipelineSpec expected = BatchPipelineSpec.builder()
      .addStage(
        StageSpec.builder("source", new PluginSpec(BatchSource.PLUGIN_TYPE, "mocksource", emptyMap, ARTIFACT_ID))
          .addOutput(SCHEMA_A, "tA", "tABC")
          .build())
      .addStage(
        StageSpec.builder("tA", new PluginSpec(Transform.PLUGIN_TYPE, "mockA", emptyMap, ARTIFACT_ID))
          .addInputSchema("source", SCHEMA_A)
          .addOutput(SCHEMA_A, "autojoin")
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("tABC", new PluginSpec(Transform.PLUGIN_TYPE, "mockABC", emptyMap, ARTIFACT_ID))
          .addInputSchema("source", SCHEMA_A)
          .addOutput(SCHEMA_ABC, "autojoin")
          .setErrorSchema(SCHEMA_A)
          .build())
      .addStage(
        StageSpec.builder("autojoin",
                          new PluginSpec(BatchJoiner.PLUGIN_TYPE, "mockautojoiner", emptyMap, ARTIFACT_ID))
          .addInputSchema("tA", SCHEMA_A)
          .addInputSchema("tABC", SCHEMA_ABC)
          .addOutput(joinSchema, "sink")
          .setErrorSchema(SCHEMA_ABC)
          .build())
      .addStage(
        StageSpec.builder("sink", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", emptyMap, ARTIFACT_ID))
          .addInputSchema("autojoin", joinSchema)
          .setErrorSchema(joinSchema)
          .build())
      .addConnections(config.getConnections())
      .setResources(config.getResources())
      .setDriverResources(config.getDriverResources())
      .setClientResources(config.getClientResources())
      .setStageLoggingEnabled(config.isStageLoggingEnabled())
      .setNumOfRecordsPreview(config.getNumOfRecordsPreview())
      .build();

    PipelineSpec actual = specGenerator.generateSpec(config);

    Assert.assertEquals(expected, actual);
  }

  @Test(expected = ValidationException.class)
  public void testJoinExpressionMapReduceFails() {
    joinDefinition = JoinDefinition.builder()
      .select(new JoinField("s1", "a"), new JoinField("s2", "b"))
      .from(JoinStage.builder("s1", SCHEMA_A).isRequired().build(),
            JoinStage.builder("s2", SCHEMA_ABC).isOptional().build())
      .on(JoinCondition.onExpression().setExpression("s1.a = s2.b").build())
      .build();

    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("s1", MOCK_SOURCE))
      .addStage(new ETLStage("s2", MOCK_SOURCE))
      .addStage(new ETLStage("autojoin", MOCK_AUTO_JOINER))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("s1", "autojoin")
      .addConnection("s2", "autojoin")
      .addConnection("autojoin", "sink")
      .setEngine(Engine.MAPREDUCE)
      .build();

    specGenerator.generateSpec(config);
  }

  @Test
  public void testConditionSchemaPropagation() throws ValidationException {
    /*
     * source --> condition --> sink
     */
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("cond", MOCK_CONDITION))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("source", "cond")
      .addConnection("cond", "sink", true)
      .setNumOfRecordsPreview(100)
      .build();

    PipelineSpec expected = BatchPipelineSpec.builder()
      .addStage(
        StageSpec.builder("source", new PluginSpec(BatchSource.PLUGIN_TYPE, "mocksource", EMPTY_MAP, ARTIFACT_ID))
          .addOutput(SCHEMA_A, "cond")
          .build())
      .addStage(
        StageSpec.builder("cond", new PluginSpec(Condition.PLUGIN_TYPE, "mockcondition", EMPTY_MAP, ARTIFACT_ID))
          .addInputSchema("source", SCHEMA_A)
          .addOutput("sink", null, SCHEMA_A)
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
      .setNumOfRecordsPreview(config.getNumOfRecordsPreview())
      .build();

    PipelineSpec actual = specGenerator.generateSpec(config);
    Assert.assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConflictingInputSchemasCondition() throws ValidationException {
    /*
     *           ---- transformA ----
     *           |                  |
     * source ---                   |--- condition -- sink
     *           |                  |
     *           ---- transformB ----
     *
     * sink gets schema A and schema B as input, should fail
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
  public void testConflictingInputSchemas() throws ValidationException {
    /*
     *           ---- transformA ----
     *           |                  |
     * source ---                   |--- sink
     *           |                  |
     *           ---- transformB ----
     *
     * sink gets schema A and schema B as input, should fail
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
  public void testConflictingInputErrorSchemas() throws ValidationException {
    /*
     *           ---- transformA
     *           |        |
     * source ---|        |------- error -- sink
     *           |        |
     *           ---- transformB
     *
     * error gets schema B from transformA and schema A from transformB, should fail
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
  public void testBadErrorTransformInput() throws ValidationException {
    /*
     * source --> joiner --> error --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
  public void testConflictingPipelineProperties() throws ValidationException {
    // populate some mock plugins.
    MockPluginConfigurer pluginConfigurer = new MockPluginConfigurer();
    Set<ArtifactId> artifactIds = ImmutableSet.of(ARTIFACT_ID);
    pluginConfigurer.addMockPlugin(Action.PLUGIN_TYPE, "action1",
                                   MockPlugin.builder().putPipelineProperty("prop1", "val1").build(), artifactIds);
    pluginConfigurer.addMockPlugin(Action.PLUGIN_TYPE, "action2",
                                   MockPlugin.builder().putPipelineProperty("prop1", "val2").build(), artifactIds);

    Map<String, String> empty = ImmutableMap.of();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
      .addStage(new ETLStage("a1", new ETLPlugin("action1", Action.PLUGIN_TYPE, empty)))
      .addStage(new ETLStage("a2", new ETLPlugin("action2", Action.PLUGIN_TYPE, empty)))
      .addConnection("a1", "a2")
      .setEngine(Engine.MAPREDUCE)
      .build();

    new BatchPipelineSpecGenerator(NamespaceId.DEFAULT.getNamespace(),
                                   pluginConfigurer, null, ImmutableSet.of(BatchSource.PLUGIN_TYPE),
                                   ImmutableSet.of(BatchSink.PLUGIN_TYPE), Engine.MAPREDUCE)
      .generateSpec(config);
  }

  @Test
  public void testPipelineProperties() throws ValidationException {
    // populate some mock plugins.
    MockPluginConfigurer pluginConfigurer = new MockPluginConfigurer();
    Set<ArtifactId> artifactIds = ImmutableSet.of(ARTIFACT_ID);
    pluginConfigurer.addMockPlugin(Action.PLUGIN_TYPE, "action1",
                                   MockPlugin.builder()
                                     .putPipelineProperty("prop1", "val1")
                                     .putPipelineProperty("prop2", "val2").build(), artifactIds);
    pluginConfigurer.addMockPlugin(Action.PLUGIN_TYPE, "action2",
                                   MockPlugin.builder().putPipelineProperty("prop2", "val2").build(), artifactIds);

    Map<String, String> empty = ImmutableMap.of();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setProperties(ImmutableMap.of("system.spark.spark.test", "abc", "system.mapreduce.prop3", "val3"))
      .addStage(new ETLStage("a1", new ETLPlugin("action1", Action.PLUGIN_TYPE, empty)))
      .addStage(new ETLStage("a2", new ETLPlugin("action2", Action.PLUGIN_TYPE, empty)))
      .addConnection("a1", "a2")
      .setEngine(Engine.MAPREDUCE)
      .setNumOfRecordsPreview(100)
      .build();

    PipelineSpec actual = new BatchPipelineSpecGenerator(NamespaceId.DEFAULT.getNamespace(), pluginConfigurer, null,
                                                         ImmutableSet.of(BatchSource.PLUGIN_TYPE),
                                                         ImmutableSet.of(BatchSink.PLUGIN_TYPE), Engine.MAPREDUCE)
      .generateSpec(config);

    PipelineSpec expected = BatchPipelineSpec.builder()
      .addConnection("a1", "a2")
      // properties should not include the spark property, but should include the one from the config
      // plus the ones from the plugins
      .setProperties(ImmutableMap.of("prop1", "val1", "prop2", "val2", "prop3", "val3"))
      .addStage(StageSpec.builder("a1", new PluginSpec(Action.PLUGIN_TYPE, "action1", empty, ARTIFACT_ID))
                  .addOutput(null, "a2")
                  .build())
      .addStage(StageSpec.builder("a2", new PluginSpec(Action.PLUGIN_TYPE, "action2", empty, ARTIFACT_ID))
                  .addInputSchema("a1", null)
                  .build())
      .setResources(new Resources(1024))
      .setDriverResources(new Resources(1024))
      .setClientResources(new Resources(1024))
      .setNumOfRecordsPreview(config.getNumOfRecordsPreview())
      .build();
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSQLEngine() throws ValidationException {
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
      .addStage(new ETLStage("action", MOCK_ACTION))
      .setPushdownEnabled(true)
      .setTransformationPushdown(new ETLTransformationPushdown(MOCK_SQL_ENGINE))
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
      .setSqlEngineStageSpec(
        StageSpec.builder("sqlengine_mocksqlengine",
                          new PluginSpec(BatchSQLEngine.PLUGIN_TYPE, "mocksqlengine", emptyMap, ARTIFACT_ID)).build())
      .build();

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSQLEngineNotEnabled() throws ValidationException {
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
      .addStage(new ETLStage("action", MOCK_ACTION))
      .setPushdownEnabled(false)
      .setTransformationPushdown(new ETLTransformationPushdown(MOCK_SQL_ENGINE))
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
      .setSqlEngineStageSpec(null)
      .build();

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSQLEngineEnabledButNotConfigured() throws ValidationException {
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
      .addStage(new ETLStage("action", MOCK_ACTION))
      .setPushdownEnabled(true)
      .setTransformationPushdown(null)
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
      .setSqlEngineStageSpec(null)
      .build();

    Assert.assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSimpleConditionConnectionWithNoBranchInfo() throws ValidationException {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
  public void testSimpleConditionConnectionWithMultipleTrueBranches() throws ValidationException {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
  public void testNestedConditionConnectionWithNoBranchInfo() throws ValidationException {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
  public void testNestedConditionConnectionWithMultipleTrueBranches() throws ValidationException {
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
  public void testSimpleConditionWithCrossConnection() throws ValidationException {

    //  source1--condition-----t1-----sink
    //                                  |
    //                    source2-------

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
  public void testNestedConditionWithCrossConnection() throws ValidationException {
    //
    //                                anothersource-------------
    //                                                          |
    //  source--condition1-----t1-----condition2------t11------sink1
    //             |                      |                     |
    //             |                      |-----------t12--------
    //             t2---------sink2

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
  public void testSourceConditionWithMultipleInputStepWithinBranch() {
    //
    //  condition1-----source-----condition2------t1-------sink
    //                                 |                    |
    //                                 |----------t2---------
    //

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
      .addStage(new ETLStage("condition1", MOCK_CONDITION))
      .addStage(new ETLStage("source", MOCK_SOURCE))
      .addStage(new ETLStage("condition2", MOCK_CONDITION))
      .addStage(new ETLStage("t1", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("t2", MOCK_TRANSFORM_A))
      .addStage(new ETLStage("sink", MOCK_SINK))
      .addConnection("condition1", "source", true)
      .addConnection("source", "condition2")
      .addConnection("condition2", "t1", true)
      .addConnection("condition2", "t2", false)
      .addConnection("t1", "sink")
      .addConnection("t2", "sink")
      .build();
    specGenerator.generateSpec(etlConfig);
  }

  @Test
  public void testSimpleValidCondition() throws ValidationException {

    //  source--condition-----t1-----sink

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
  public void testNestedValidCondition() throws ValidationException {

    //  source--condition1-----t1-----condition2------t11------sink1
    //             |                      |                     |
    //             |                      |-----------t12--------
    //             t2---------sink2

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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
  public void testTwoConditionsGoingToSameStage() throws ValidationException {

    //  source--condition1-----t1-----condition2------t11------sink1
    //             |                      |                     |
    //             |                      |-----------t12--------
    //             t2---------sink2                   |
    //                                                |
    //                                                |
    //  anotherSource--------->condition3----------------------sink3

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .setTimeSchedule("* * * * *")
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

  private static class MockAutoJoin extends BatchAutoJoiner {

    @Nullable
    @Override
    public JoinDefinition define(AutoJoinerContext context) {
      return joinDefinition;
    }
  }

  private static class MockSQLEngine extends BatchSQLEngine<Object, Object, Object, Object> {
    @Override
    public SQLPushDataset<StructuredRecord, Object, Object> getPushProvider(SQLPushRequest pushRequest)
      throws SQLEngineException {
      return null;
    }

    @Override
    public SQLPullDataset<StructuredRecord, Object, Object> getPullProvider(SQLPullRequest pullRequest)
      throws SQLEngineException {
      return null;
    }

    @Override
    public boolean exists(String datasetName) throws SQLEngineException {
      return false;
    }

    @Override
    public boolean canJoin(SQLJoinDefinition joinRequest) {
      return false;
    }

    @Override
    public SQLDataset join(SQLJoinRequest joinRequest)
      throws SQLEngineException {
      return null;
    }

    @Override
    public void cleanup(String datasetName) throws SQLEngineException {

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
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
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
