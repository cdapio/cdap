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

package io.cdap.cdap.etl.mock.batch;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * Transform that throws null pointer exception while configuring it.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("NullErrorTransform")
public class NullErrorTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final NullErrorTransform.Config config;

  public NullErrorTransform(NullErrorTransform.Config config) {
    this.config = config;
  }

  @Override
  @SuppressWarnings("ConstantConditions")
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
      config.name = null;
    // throw null pointer exception
    if (config.name.equals("xyz")) {
      config.name = "pqr";
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    // no-op
  }

  /**
   * Config for NullErrorTransform.
   */
  public static class Config extends PluginConfig {
    private String name;
  }

  public static ETLPlugin getPlugin() {
    Map<String, String> properties = new HashMap<>();
    properties.put("name", "");
    return new ETLPlugin("NullErrorTransform", Transform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("name", new PluginPropertyField("name", "", "string", true, false));
    return PluginClass.builder().setName("NullErrorTransform").setType(Transform.PLUGIN_TYPE)
             .setDescription("").setClassName(NullErrorTransform.class.getName()).setProperties(properties)
             .setConfigFieldName("config").build();
  }
}
