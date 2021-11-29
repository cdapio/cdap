/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.mock.transform;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This transform is basically identity transform but can be used to validate the plugin config can be set as macro
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("PluginValidation")
public class PluginValidationTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;

  public PluginValidationTransform(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    if (!config.containsMacro("plugin1") && !config.containsMacro("plugin1Type")) {
      pipelineConfigurer.usePlugin(config.connectionConfig.plugin1Type, config.connectionConfig.plugin1,
                                   config.connectionConfig.plugin1, PluginProperties.builder().build());
    }

    if (!config.containsMacro("plugin2") && !config.containsMacro("plugin2Type")) {
      pipelineConfigurer.usePlugin(config.plugin2Type, config.plugin2,
                                   config.plugin2, PluginProperties.builder().build());
    }
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    Object plugin1 = context.newPluginInstance(config.connectionConfig.plugin1);
    Object plugin2 = context.newPluginInstance(config.plugin2);
    if (plugin1 == null || plugin2 == null) {
      throw new RuntimeException(String.format("Both %s and %s should get instantiated.",
                                               config.connectionConfig.plugin1, config.plugin2));
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(input);
  }

  /**
   * Config for the source.
   */
  public static class Config extends PluginConfig {
    @Macro
    public ConnectionConfig connectionConfig;

    @Macro
    public String plugin2;

    @Macro
    public String plugin2Type;
  }

  /**
   * Connection Config for mock source
   */
  public static class ConnectionConfig extends PluginConfig {
    @Macro
    public String plugin1;

    @Macro
    public String plugin1Type;
  }

  public static ETLPlugin getPlugin(String plugin1, String plugin1Type, String plugin2, String plugin2Type) {
    Map<String, String> properties = new HashMap<>();
    properties.put("plugin1", plugin1);
    properties.put("plugin1Type", plugin1Type);
    properties.put("plugin2", plugin2);
    properties.put("plugin2Type", plugin2Type);
    return new ETLPlugin("PluginValidation", Transform.PLUGIN_TYPE, properties, null);
  }

  public static ETLPlugin getPluginUsingConnection(String connectionName, String plugin2, String plugin2Type) {
    Map<String, String> properties = new HashMap<>();
    properties.put("connectionConfig", String.format("${conn(%s)}", connectionName));
    properties.put("plugin2", plugin2);
    properties.put("plugin2Type", plugin2Type);
    return new ETLPlugin("PluginValidation", Transform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("connectionConfig", new PluginPropertyField("connectionConfig", "", "connectionconfig", true, true,
                                                               false, ImmutableSet.of("plugin1", "plugin1Type")));
    properties.put("plugin1", new PluginPropertyField("plugin1", "", "string", true, true));
    properties.put("plugin1Type", new PluginPropertyField("plugin1Type", "", "string", true, true));
    properties.put("plugin2", new PluginPropertyField("plugin2", "", "string", true, true));
    properties.put("plugin2Type", new PluginPropertyField("plugin2Type", "", "string", true, true));
    return PluginClass.builder().setName("PluginValidation").setType(Transform.PLUGIN_TYPE)
             .setDescription("").setClassName(PluginValidationTransform.class.getName()).setProperties(properties)
             .setConfigFieldName("config").build();
  }
}
