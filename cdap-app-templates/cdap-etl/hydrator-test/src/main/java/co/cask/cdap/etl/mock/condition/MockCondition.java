/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.mock.condition;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.condition.Condition;
import co.cask.cdap.etl.api.condition.ConditionContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Mock implementation of Condition in the pipeline.
 */
@Plugin(type = Condition.PLUGIN_TYPE)
@Name("Mock")
public class MockCondition extends Condition {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;

  public MockCondition(Config config) {
    this.config = config;
  }

  /**
   * Config for the sink.
   */
  public static class Config extends PluginConfig {
    private String name;
  }

  @Override
  public boolean apply(ConditionContext context) throws Exception {
    String propertyName = config.name + ".branch.to.execute";
    String propertyValue = context.getArguments().get(propertyName);
    return propertyValue != null && propertyValue.equals("true");
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("name", new PluginPropertyField("name", "", "string", true, false));
    return new PluginClass(Condition.PLUGIN_TYPE, "Mock", "", MockCondition.class.getName(), "config", properties);
  }

  public static ETLPlugin getPlugin(String name) {
    Map<String, String> properties = ImmutableMap.of("name", name);
    return new ETLPlugin("Mock", Condition.PLUGIN_TYPE, properties, null);
  }
}
