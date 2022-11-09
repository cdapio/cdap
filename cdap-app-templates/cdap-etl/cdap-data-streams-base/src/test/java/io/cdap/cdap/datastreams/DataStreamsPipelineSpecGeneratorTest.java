/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.RuntimeConfigurer;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.etl.api.AlertPublisher;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.Windower;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.mock.spark.streaming.MockSink;
import io.cdap.cdap.etl.mock.spark.streaming.MockSource;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.spark.streaming.StreamingRetrySettings;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Tests for @link{DataStreamsPipelineSpecGenerator}
 */
public class DataStreamsPipelineSpecGeneratorTest {

  private static final ImmutableSet<String> SOURCE_PLUGIN_TYPES = ImmutableSet.of(StreamingSource.PLUGIN_TYPE);
  private static final ImmutableSet<String> SINK_PLUGIN_TYPES = ImmutableSet.of(BatchSink.PLUGIN_TYPE,
                                                                                SparkSink.PLUGIN_TYPE,
                                                                                AlertPublisher.PLUGIN_TYPE);
  private static Schema schema = Schema.recordOf("test", Schema.Field.of("key", Schema.of(Schema.Type.STRING)),
                                                 Schema.Field.of("value", Schema.of(Schema.Type.STRING)));

  @Test
  public void testConfigureAtleastOnceModeSuccess() throws IOException {
    DataStreamsConfig etlConfig = getDataStreamsConfig(MockStateHandlerSource.getPlugin(schema, new ArrayList<>()));
    DataStreamsPipelineSpec.Builder builder = DataStreamsPipelineSpec.builder(System.currentTimeMillis(), "test-id");
    FeatureFlagsProvider featureFlagsProvider = new FeatureFlagsProvider() {

      @Override
      public boolean isFeatureEnabled(String name) {
        return true;
      }
    };
    DataStreamsPipelineSpecGenerator specGenerator = new DataStreamsPipelineSpecGenerator("test", null, null,
                                                                                          SOURCE_PLUGIN_TYPES,
                                                                                          SINK_PLUGIN_TYPES,
                                                                                          featureFlagsProvider);
    specGenerator.configureSourcePlugin("source", new MockStateHandlerSource(new MockSource.Conf()), null, null);
    specGenerator.configureAtleastOnceMode(etlConfig, builder);

    DataStreamsPipelineSpec pipelineSpec = builder.build();
    Assert.assertTrue(pipelineSpec.getStateSpec().getMode() == DataStreamsStateSpec.Mode.STATE_STORE);
  }

  @Test
  public void testConfigureAtleastOnceModeRuntimeArg() throws IOException {
    DataStreamsConfig etlConfig = getDataStreamsConfig(MockStateHandlerSource.getPlugin(schema, new ArrayList<>()));
    DataStreamsPipelineSpec.Builder builder = DataStreamsPipelineSpec.builder(System.currentTimeMillis(), "test-id");
    FeatureFlagsProvider featureFlagsProvider = new FeatureFlagsProvider() {

      @Override
      public boolean isFeatureEnabled(String name) {
        return true;
      }
    };
    RuntimeConfigurer runtimeConfigurer = getTestRuntimeConfigurer(
      Collections.singletonMap("cdap.streaming.atleastonce.enabled", "false"));
    DataStreamsPipelineSpecGenerator specGenerator = new DataStreamsPipelineSpecGenerator("test", null,
                                                                                          runtimeConfigurer,
                                                                                          SOURCE_PLUGIN_TYPES,
                                                                                          SINK_PLUGIN_TYPES,
                                                                                          featureFlagsProvider);
    specGenerator.configureSourcePlugin("source", new MockStateHandlerSource(new MockSource.Conf()), null, null);
    specGenerator.configureAtleastOnceMode(etlConfig, builder);

    DataStreamsPipelineSpec pipelineSpec = builder.build();
    Assert.assertTrue(pipelineSpec.getStateSpec().getMode() == DataStreamsStateSpec.Mode.NONE);
  }

  private RuntimeConfigurer getTestRuntimeConfigurer(Map<String, String> runTimeArgumentsMap) {
    RuntimeConfigurer runtimeConfigurer = new RuntimeConfigurer() {

      @Override
      public Map<String, String> getRuntimeArguments() {
        return runTimeArgumentsMap;
      }

      @Nullable
      @Override
      public ApplicationSpecification getDeployedApplicationSpec() {
        return null;
      }

      @Nullable
      @Override
      public URL getServiceURL(String applicationId, String serviceId) {
        return null;
      }

      @Nullable
      @Override
      public URL getServiceURL(String serviceId) {
        return null;
      }
    };
    return runtimeConfigurer;
  }

  @Test
  public void testConfigureAtleastOnceModeFeatureDisabled() throws IOException {
    DataStreamsConfig etlConfig = getDataStreamsConfig(MockStateHandlerSource.getPlugin(schema, new ArrayList<>()));
    DataStreamsPipelineSpec.Builder builder = DataStreamsPipelineSpec.builder(System.currentTimeMillis(), "test-id");
    FeatureFlagsProvider featureFlagsProvider = new FeatureFlagsProvider() {

      @Override
      public boolean isFeatureEnabled(String name) {
        return false;
      }
    };
    DataStreamsPipelineSpecGenerator specGenerator = new DataStreamsPipelineSpecGenerator("test", null, null,
                                                                                          SOURCE_PLUGIN_TYPES,
                                                                                          SINK_PLUGIN_TYPES,
                                                                                          featureFlagsProvider);
    specGenerator.configureSourcePlugin("source", new MockStateHandlerSource(new MockSource.Conf()), null, null);
    specGenerator.configureAtleastOnceMode(etlConfig, builder);

    DataStreamsPipelineSpec pipelineSpec = builder.build();
    Assert.assertTrue(pipelineSpec.getStateSpec().getMode() == DataStreamsStateSpec.Mode.SPARK_CHECKPOINTING);
  }

  @Test
  public void testConfigureAtleastOnceModeUnsupportedSource() throws IOException {
    DataStreamsConfig etlConfig = getDataStreamsConfig(MockSource.getPlugin(schema, new ArrayList<>()));
    DataStreamsPipelineSpec.Builder builder = DataStreamsPipelineSpec.builder(System.currentTimeMillis(), "test-id");
    FeatureFlagsProvider featureFlagsProvider = new FeatureFlagsProvider() {

      @Override
      public boolean isFeatureEnabled(String name) {
        return true;
      }
    };
    DataStreamsPipelineSpecGenerator specGenerator = new DataStreamsPipelineSpecGenerator("test", null, null,
                                                                                          SOURCE_PLUGIN_TYPES,
                                                                                          SINK_PLUGIN_TYPES,
                                                                                          featureFlagsProvider);
    specGenerator.configureSourcePlugin("source", new MockSource(new MockSource.Conf()), null, null);
    specGenerator.configureAtleastOnceMode(etlConfig, builder);

    DataStreamsPipelineSpec pipelineSpec = builder.build();
    Assert.assertTrue(pipelineSpec.getStateSpec().getMode() == DataStreamsStateSpec.Mode.SPARK_CHECKPOINTING);
  }

  @Test
  public void testConfigureAtleastOnceModeWindower() throws IOException {
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("source", MockStateHandlerSource.getPlugin(schema, new ArrayList<>())))
      .addStage(new ETLStage("sink", MockSink.getPlugin("${tablename}")))
      .addStage(new ETLStage("windower", new ETLPlugin("MockWindower", Windower.PLUGIN_TYPE, new HashMap<>(), null)))
      .addConnection("source", "windower").addConnection("windower", "sink").setCheckpointDir(null)
      .setBatchInterval("1s").build();
    DataStreamsPipelineSpec.Builder builder = DataStreamsPipelineSpec.builder(System.currentTimeMillis(), "test-id");
    FeatureFlagsProvider featureFlagsProvider = new FeatureFlagsProvider() {

      @Override
      public boolean isFeatureEnabled(String name) {
        return true;
      }
    };
    DataStreamsPipelineSpecGenerator specGenerator = new DataStreamsPipelineSpecGenerator("test", null, null,
                                                                                          SOURCE_PLUGIN_TYPES,
                                                                                          SINK_PLUGIN_TYPES,
                                                                                          featureFlagsProvider);
    specGenerator.configureSourcePlugin("source", new MockStateHandlerSource(new MockSource.Conf()), null, null);
    specGenerator.configureAtleastOnceMode(etlConfig, builder);

    DataStreamsPipelineSpec pipelineSpec = builder.build();
    Assert.assertTrue(pipelineSpec.getStateSpec().getMode() == DataStreamsStateSpec.Mode.SPARK_CHECKPOINTING);
  }

  private DataStreamsConfig getDataStreamsConfig(ETLPlugin plugin) throws IOException {
    return DataStreamsConfig.builder().addStage(new ETLStage("source", plugin))
      .addStage(new ETLStage("sink", MockSink.getPlugin("${tablename}"))).addConnection("source", "sink")
      .setCheckpointDir(null).setBatchInterval("1s").build();
  }

  @Test
  public void testConfigureRetriesDefault() throws IOException {
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("source", MockStateHandlerSource.getPlugin(schema, new ArrayList<>())))
      .addStage(new ETLStage("sink", MockSink.getPlugin("${tablename}")))
      .addConnection("source", "sink")
      .setBatchInterval("1s").build();
    DataStreamsPipelineSpec.Builder builder = DataStreamsPipelineSpec.builder(System.currentTimeMillis(), "test-id");
    RuntimeConfigurer mockRuntimeConfigurer = getTestRuntimeConfigurer(Collections.emptyMap());
    DataStreamsPipelineSpecGenerator specGenerator = new DataStreamsPipelineSpecGenerator("test", null,
                                                                                          mockRuntimeConfigurer,
                                                                                          SOURCE_PLUGIN_TYPES,
                                                                                          SINK_PLUGIN_TYPES,
                                                                                          null);
    specGenerator.configureRetries(builder);
    DataStreamsPipelineSpec pipelineSpec = builder.build();
    StreamingRetrySettings streamingRetrySettings = pipelineSpec.getStreamingRetrySettings();
    Assert.assertEquals(360L, streamingRetrySettings.getMaxRetryTimeInMins());
    Assert.assertEquals(1L, streamingRetrySettings.getBaseDelayInSeconds());
    Assert.assertEquals(60L, streamingRetrySettings.getMaxDelayInSeconds());
  }

  @Test
  public void testConfigureRetries() throws IOException {
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("source", MockStateHandlerSource.getPlugin(schema, new ArrayList<>())))
      .addStage(new ETLStage("sink", MockSink.getPlugin("${tablename}")))
      .addConnection("source", "sink")
      .setBatchInterval("1s").build();
    DataStreamsPipelineSpec.Builder builder = DataStreamsPipelineSpec.builder(System.currentTimeMillis(), "test-id");
    Map<String, String> runtimeArgs = new HashMap<>();
    runtimeArgs.put(Constants.CDAP_STREAMING_MAX_RETRY_TIME_IN_MINS, "60");
    runtimeArgs.put(Constants.CDAP_STREAMING_BASE_RETRY_DELAY_IN_SECONDS, "10");
    runtimeArgs.put(Constants.CDAP_STREAMING_MAX_RETRY_DELAY_IN_SECONDS, "100");
    RuntimeConfigurer mockRuntimeConfigurer = getTestRuntimeConfigurer(runtimeArgs);
    DataStreamsPipelineSpecGenerator specGenerator = new DataStreamsPipelineSpecGenerator("test", null,
                                                                                          mockRuntimeConfigurer,
                                                                                          SOURCE_PLUGIN_TYPES,
                                                                                          SINK_PLUGIN_TYPES,
                                                                                          null);
    specGenerator.configureRetries(builder);
    DataStreamsPipelineSpec pipelineSpec = builder.build();
    StreamingRetrySettings streamingRetrySettings = pipelineSpec.getStreamingRetrySettings();
    Assert.assertEquals(60L, streamingRetrySettings.getMaxRetryTimeInMins());
    Assert.assertEquals(10L, streamingRetrySettings.getBaseDelayInSeconds());
    Assert.assertEquals(100L, streamingRetrySettings.getMaxDelayInSeconds());
  }
}
