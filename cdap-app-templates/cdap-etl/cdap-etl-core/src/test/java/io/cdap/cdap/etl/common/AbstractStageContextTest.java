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

package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.proto.v2.spec.PluginSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AbstractStageContext}.
 */
public class AbstractStageContextTest {

  @Test
  public void testNewPluginInstanceSuccess() throws Exception {
    PluginContext pluginContext = Mockito.mock(PluginContext.class);
    String expectedPlugin = "myPluginInstance";
    Mockito.when(pluginContext.newPluginInstance(
        Mockito.eq("stageA" + Constants.ID_SEPARATOR + "childId"),
        Mockito.any(MacroEvaluator.class))).thenReturn(expectedPlugin);

    AbstractStageContext stageContext = createStageContext("stageA", pluginContext);
    Assert.assertSame(expectedPlugin, stageContext.newPluginInstance("childId"));
  }

  @Test
  public void testNewPluginInstancePropagatesInstantiationExceptionDirectly() throws Exception {
    PluginContext pluginContext = Mockito.mock(PluginContext.class);
    InstantiationException expected = new InstantiationException("cannot instantiate plugin");
    Mockito.when(pluginContext.newPluginInstance(
        Mockito.eq("stageA" + Constants.ID_SEPARATOR + "childId"),
        Mockito.any(MacroEvaluator.class))).thenThrow(expected);

    AbstractStageContext stageContext = createStageContext("stageA", pluginContext);
    try {
      stageContext.newPluginInstance("childId");
      Assert.fail("Expected InstantiationException");
    } catch (InstantiationException actual) {
      Assert.assertSame(expected, actual);
    }
  }

  @Test
  public void testNewPluginInstancePropagatesRuntimeExceptionDirectly() throws Exception {
    PluginContext pluginContext = Mockito.mock(PluginContext.class);
    IllegalArgumentException expected = new IllegalArgumentException("invalid plugin config");
    Mockito.when(pluginContext.newPluginInstance(
        Mockito.eq("stageA" + Constants.ID_SEPARATOR + "childId"),
        Mockito.any(MacroEvaluator.class))).thenThrow(expected);

    AbstractStageContext stageContext = createStageContext("stageA", pluginContext);
    try {
      stageContext.newPluginInstance("childId");
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException actual) {
      Assert.assertSame(expected, actual);
    }
  }

  @Test
  public void testNewPluginInstanceWrapsCheckedNonInstantiationException() throws Exception {
    PluginContext pluginContext = Mockito.mock(PluginContext.class);
    IOException checkedFailure = new IOException("unexpected checked failure");
    Mockito.when(pluginContext.newPluginInstance(
        Mockito.eq("stageA" + Constants.ID_SEPARATOR + "childId"),
        Mockito.any(MacroEvaluator.class))).thenAnswer(invocation -> {
          throw checkedFailure;
        });

    AbstractStageContext stageContext = createStageContext("stageA", pluginContext);
    try {
      stageContext.newPluginInstance("childId");
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException actual) {
      Assert.assertSame(checkedFailure, actual.getCause());
    }
  }

  private AbstractStageContext createStageContext(String stageName, PluginContext pluginContext) {
    PipelineRuntime pipelineRuntime = Mockito.mock(PipelineRuntime.class);
    Mockito.when(pipelineRuntime.getNamespace()).thenReturn("default");
    Mockito.when(pipelineRuntime.getPipelineName()).thenReturn("testPipeline");
    Mockito.when(pipelineRuntime.getLogicalStartTime()).thenReturn(1000L);
    Mockito.when(pipelineRuntime.getMetrics()).thenReturn(new MockMetrics());
    Mockito.when(pipelineRuntime.getArguments()).thenReturn(new BasicArguments(Collections.emptyMap()));
    Mockito.when(pipelineRuntime.getPluginContext()).thenReturn(pluginContext);
    Mockito.when(pipelineRuntime.getSecureStore()).thenReturn(Mockito.mock(SecureStore.class));
    Mockito.when(pipelineRuntime.getServiceDiscoverer()).thenReturn(Mockito.mock(ServiceDiscoverer.class));

    PluginSpec pluginSpec = new PluginSpec("transform", "mock", Collections.emptyMap(), null);
    StageSpec stageSpec = StageSpec.builder(stageName, pluginSpec).build();

    return new AbstractStageContext(pipelineRuntime, stageSpec) {
      @Override
      public void record(List<FieldOperation> fieldOperations) {
        // no-op
      }
    };
  }
}
