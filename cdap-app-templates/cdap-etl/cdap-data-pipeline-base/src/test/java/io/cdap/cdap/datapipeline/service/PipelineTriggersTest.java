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

package io.cdap.cdap.datapipeline.service;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.WorkflowAppWithFork;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.schedule.ProgramStatusTriggerInfo;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.etl.proto.v2.ArgumentMapping;
import io.cdap.cdap.etl.proto.v2.PluginPropertyMapping;
import io.cdap.cdap.etl.proto.v2.TriggeringPipelineId;
import io.cdap.cdap.etl.proto.v2.TriggeringPropertyMapping;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.DefaultProgramStatusTriggerInfo;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PipelineTriggersTest {
  private static final Gson GSON = new Gson();

  private static final String expectedValue1 = "headArgValue";
  private static final String expectedValue2 = "headPluginValue";
  private static final String defaultNamespace = NamespaceId.DEFAULT.getNamespace();
  private ProgramStatusTriggerInfo triggerInfo;
  private TriggeringPropertyMapping triggeringPropertyMapping;

  @Before
  public void init() {
    TriggeringPipelineId pipelineIdHead = new TriggeringPipelineId(defaultNamespace, "head");
    TriggeringPipelineId pipelineIdMiddle = new TriggeringPipelineId(defaultNamespace, "middle");
    ArgumentMapping argHead = new ArgumentMapping("head-arg", "middle-arg", pipelineIdHead);
    ArgumentMapping argMiddle = new ArgumentMapping("middle-arg", "tail-arg", pipelineIdMiddle);
    PluginPropertyMapping pluginHead = new PluginPropertyMapping("action1",
      "value", "middle-plugin", pipelineIdHead);
    PluginPropertyMapping pluginMiddle = new PluginPropertyMapping("action2",
      "value", "tail-plugin", pipelineIdMiddle);
    triggeringPropertyMapping =
      new TriggeringPropertyMapping(ImmutableList.of(argHead, argMiddle), ImmutableList.of(pluginHead, pluginMiddle));

    Map<String, Map<String, String>> pluginProperties = new HashMap<String, Map<String, String>>() {{
      put("action1", new HashMap<String, String>() {{
        put("value", expectedValue2);
      }});
      put("action2", new HashMap<String, String>() {{
        put("value", "pipelineIdMiddleValue");
      }});
    }};
    String pluginPropertiesJson = GSON.toJson(pluginProperties,
      new TypeToken<Map<String, Map<String, String>>>() { }.getType());
    Map<String, String> runtimeArguments =  new HashMap<String, String>() {{
      put("head-arg", expectedValue1);
      put("middle-arg", "pipelineIdMiddleValue");
      put("resolved.plugin.properties.map", pluginPropertiesJson);
    }};

    triggerInfo = new DefaultProgramStatusTriggerInfo(
      defaultNamespace, pipelineIdHead.getName(),
      ProgramType.WORKFLOW,
      WorkflowAppWithFork.WorkflowWithFork.class.getSimpleName(),
      RunIds.generate(), ProgramStatus.COMPLETED,
      new BasicWorkflowToken(1), runtimeArguments);
  }

  @Test
  public void testGetCompositeSchedulePropertiesMapping() {
    Map<String, String> propertyMappingResult = new HashMap<>();
    PipelineTriggers.addSchedulePropertiesMapping(propertyMappingResult, triggerInfo,
      triggeringPropertyMapping, true);

    Assert.assertEquals(propertyMappingResult.size(), 2);
    Assert.assertEquals(propertyMappingResult.get("middle-arg"), expectedValue1);
    Assert.assertEquals(propertyMappingResult.get("middle-plugin"), expectedValue2);
  }

  @Test
  public void testGetSingleSchedulePropertiesMapping() {
    Map<String, String> propertyMappingResult = new HashMap<>();
    PipelineTriggers.addSchedulePropertiesMapping(propertyMappingResult, triggerInfo,
      triggeringPropertyMapping, false);

    Assert.assertEquals(propertyMappingResult.size(), 4);
    Assert.assertEquals(propertyMappingResult.get("middle-arg"), expectedValue1);
    Assert.assertEquals(propertyMappingResult.get("middle-plugin"), expectedValue2);
    Assert.assertEquals(propertyMappingResult.get("tail-arg"), "pipelineIdMiddleValue");
    Assert.assertEquals(propertyMappingResult.get("tail-plugin"), "pipelineIdMiddleValue");
  }
}
