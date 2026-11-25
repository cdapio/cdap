/*
 * Copyright © 2023 Cask Data, Inc.
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

import io.cdap.cdap.api.app.ApplicationConfigurer;
import io.cdap.cdap.api.app.ApplicationContext;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.api.workflow.Workflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DataPipelineApp}.
 */
public class DataPipelineAppTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testConfigureUsesDefaultDescriptionWhenNull() {
    ETLBatchConfig config = createBatchConfig(null);
    ApplicationConfigurer configurer = createMockConfigurer();
    ApplicationContext<ETLBatchConfig> context = Mockito.mock(ApplicationContext.class);
    Mockito.when(context.getConfig()).thenReturn(config);

    DataPipelineApp app = new DataPipelineApp();
    app.configure(configurer, context);

    Mockito.verify(configurer).setDescription(DataPipelineApp.DEFAULT_DESCRIPTION);
    Mockito.verify(configurer).addWorkflow(Mockito.any(Workflow.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testConfigureUsesCustomDescriptionWhenProvided() {
    String customDescription = "Custom Batch Pipeline Description";
    ETLBatchConfig config = createBatchConfig(customDescription);
    ApplicationConfigurer configurer = createMockConfigurer();
    ApplicationContext<ETLBatchConfig> context = Mockito.mock(ApplicationContext.class);
    Mockito.when(context.getConfig()).thenReturn(config);

    DataPipelineApp app = new DataPipelineApp();
    app.configure(configurer, context);

    Assert.assertTrue(app.isUpdateSupported());
    Mockito.verify(configurer).setDescription(customDescription);
    Mockito.verify(configurer).addWorkflow(Mockito.any(Workflow.class));
  }

  private ETLBatchConfig createBatchConfig(String description) {
    ETLBatchConfig.Builder builder = ETLBatchConfig.builder()
        .addStage(new ETLStage(
            "src", new ETLPlugin("mockSrc", BatchSource.PLUGIN_TYPE, Collections.emptyMap())))
        .addStage(new ETLStage(
            "sink", new ETLPlugin("mockSink", BatchSink.PLUGIN_TYPE, Collections.emptyMap())))
        .addConnection("src", "sink");
    if (description != null) {
      builder.setDescription(description);
    }
    return builder.build();
  }

  private ApplicationConfigurer createMockConfigurer() {
    ApplicationConfigurer configurer = Mockito.mock(ApplicationConfigurer.class);
    Mockito.when(configurer.getDeployedNamespace()).thenReturn("default");
    Mockito.doReturn(new BatchSource<Object, Object, Object>() {
      @Override
      public void prepareRun(BatchSourceContext context) {
      }
    }).when(configurer).usePlugin(
        Mockito.eq(BatchSource.PLUGIN_TYPE),
        Mockito.eq("mockSrc"),
        Mockito.eq("src"),
        Mockito.any(PluginProperties.class),
        Mockito.any(PluginSelector.class));
    Mockito.doReturn(new BatchSink<Object, Object, Object>() {
      @Override
      public void prepareRun(BatchSinkContext context) {
      }
    }).when(configurer).usePlugin(
        Mockito.eq(BatchSink.PLUGIN_TYPE),
        Mockito.eq("mockSink"),
        Mockito.eq("sink"),
        Mockito.any(PluginProperties.class),
        Mockito.any(PluginSelector.class));
    return configurer;
  }
}
