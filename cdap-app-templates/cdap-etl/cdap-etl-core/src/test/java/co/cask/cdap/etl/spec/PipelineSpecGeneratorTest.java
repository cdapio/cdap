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
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
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
  private static final ETLPlugin MOCK_SOURCE =
    new ETLPlugin("mocksource", BatchSource.PLUGIN_TYPE, ImmutableMap.<String, String>of(), null);
  private static final ETLPlugin MOCK_TRANSFORM_A =
    new ETLPlugin("mockA", Transform.PLUGIN_TYPE, ImmutableMap.<String, String>of(), null);
  private static final ETLPlugin MOCK_TRANSFORM_B =
    new ETLPlugin("mockB", Transform.PLUGIN_TYPE, ImmutableMap.<String, String>of(), null);
  private static final ETLPlugin MOCK_SINK =
    new ETLPlugin("mocksink", BatchSink.PLUGIN_TYPE, ImmutableMap.<String, String>of(), null);
  private static final ETLPlugin MOCK_JOINER =
    new ETLPlugin("mockjoiner", BatchJoiner.PLUGIN_TYPE, ImmutableMap.<String, String>of(), null);
  private static final ETLPlugin MOCK_ERROR =
    new ETLPlugin("mockerror", ErrorTransform.PLUGIN_TYPE, ImmutableMap.<String, String>of(), null);
  private static final ETLPlugin MOCK_ACTION =
    new ETLPlugin("mockaction", Action.PLUGIN_TYPE, ImmutableMap.<String, String>of(), null);
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
    pluginConfigurer.addMockPlugin(BatchJoiner.PLUGIN_TYPE, "mockjoiner", MockPlugin.builder().build(), artifactIds);
    pluginConfigurer.addMockPlugin(ErrorTransform.PLUGIN_TYPE, "mockerror", MockPlugin.builder().build(), artifactIds);

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
          .setOutputSchema(SCHEMA_A)
          .addOutputs("t1", "t2", "sink2")
          .build())
      .addStage(
        StageSpec.builder("sink1", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", emptyMap, ARTIFACT_ID))
          .addInputSchema("t3", SCHEMA_B)
          .addInputs("t3")
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("sink2", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", emptyMap, ARTIFACT_ID))
          .addInputSchemas(ImmutableMap.of("t1", SCHEMA_A, "t2", SCHEMA_A, "source", SCHEMA_A))
          .addInputs("t1", "t2", "source")
          .setErrorSchema(SCHEMA_A)
          .build())
      .addStage(
        StageSpec.builder("t1", new PluginSpec(Transform.PLUGIN_TYPE, "mockA", emptyMap, ARTIFACT_ID))
          .addInputSchema("source", SCHEMA_A)
          .setOutputSchema(SCHEMA_A)
          .addInputs("source")
          .addOutputs("t2", "t3", "sink2")
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("t2", new PluginSpec(Transform.PLUGIN_TYPE, "mockA", emptyMap, ARTIFACT_ID))
          .addInputSchemas(ImmutableMap.of("source", SCHEMA_A, "t1", SCHEMA_A))
          .setOutputSchema(SCHEMA_A)
          .addInputs("source", "t1")
          .addOutputs("t3", "sink2")
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("t3", new PluginSpec(Transform.PLUGIN_TYPE, "mockB", emptyMap, ARTIFACT_ID))
          .addInputSchemas(ImmutableMap.of("t1", SCHEMA_A, "t2", SCHEMA_A))
          .setOutputSchema(SCHEMA_B)
          .addInputs("t1", "t2")
          .addOutputs("sink1")
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
     *
     * sink gets schema A and schema B as input, should fail
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
          .setOutputSchema(SCHEMA_A)
          .addOutputs("tA", "tB")
          .build())
      .addStage(
        StageSpec.builder("sinkA", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", emptyMap, ARTIFACT_ID))
          .addInputSchema("tA", SCHEMA_A)
          .addInputs("tA")
          .addOutputs("action")
          .setErrorSchema(SCHEMA_A)
          .build())
      .addStage(
        StageSpec.builder("sinkB", new PluginSpec(BatchSink.PLUGIN_TYPE, "mocksink", emptyMap, ARTIFACT_ID))
          .addInputSchema("tB", SCHEMA_B)
          .addInputs("tB")
          .addOutputs("action")
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("tA", new PluginSpec(Transform.PLUGIN_TYPE, "mockA", emptyMap, ARTIFACT_ID))
          .addInputSchema("source", SCHEMA_A)
          .setOutputSchema(SCHEMA_A)
          .addInputs("source")
          .addOutputs("sinkA")
          .setErrorSchema(SCHEMA_B)
          .build())
      .addStage(
        StageSpec.builder("tB", new PluginSpec(Transform.PLUGIN_TYPE, "mockB", emptyMap, ARTIFACT_ID))
          .addInputSchema("source", SCHEMA_A)
          .setOutputSchema(SCHEMA_B)
          .addInputs("source")
          .addOutputs("sinkB")
          .setErrorSchema(SCHEMA_A)
          .build())
      .addStage(
        StageSpec.builder("action", new PluginSpec(Action.PLUGIN_TYPE, "mockaction", emptyMap, ARTIFACT_ID))
          .addInputs("sinkA", "sinkB")
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

    PipelineSpecGenerator specGenerator = new BatchPipelineSpecGenerator(pluginConfigurer,
                                                                         ImmutableSet.of(BatchSource.PLUGIN_TYPE),
                                                                         ImmutableSet.of(BatchSink.PLUGIN_TYPE),
                                                                         FileSet.class, DatasetProperties.EMPTY,
                                                                         Engine.MAPREDUCE);

    Map<String, String> empty = ImmutableMap.of();
    ETLConfig config = ETLBatchConfig.builder("* * * * *")
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

    PipelineSpecGenerator specGenerator = new BatchPipelineSpecGenerator(pluginConfigurer,
                                                                         ImmutableSet.of(BatchSource.PLUGIN_TYPE),
                                                                         ImmutableSet.of(BatchSink.PLUGIN_TYPE),
                                                                         FileSet.class, DatasetProperties.EMPTY,
                                                                         Engine.MAPREDUCE);

    Map<String, String> empty = ImmutableMap.of();
    ETLConfig config = ETLBatchConfig.builder("* * * * *")
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
                  .addOutputs("a2")
                  .build())
      .addStage(StageSpec.builder("a2", new PluginSpec(Action.PLUGIN_TYPE, "action2", empty, ARTIFACT_ID))
                  .addInputs("a1")
                  .build())
      .setResources(new Resources(1024))
      .setDriverResources(new Resources(1024))
      .setClientResources(new Resources(1024))
      .build();
    Assert.assertEquals(expected, actual);
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

      public Builder putPipelineProperties(Map<String, String> properties) {
        pipelineProperties.putAll(properties);
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
