/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.datapipeline.plugin;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * A Transform that delegates its logic to a plugin. Used to test plugins created by other plugins.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(PluggableFilterTransform.NAME)
public class PluggableFilterTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final String NAME = "PluggableTransform";
  static final String FILTER_PLUGIN_TYPE = "filter";
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private final Conf conf;
  private Predicate<StructuredRecord> filter;

  public PluggableFilterTransform(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    Map<String, String> filterProperties = GSON.fromJson(conf.filterProperties, MAP_TYPE);
    PluginProperties pluginProperties = PluginProperties.builder().addAll(filterProperties).build();
    filter = pipelineConfigurer.usePlugin(FILTER_PLUGIN_TYPE, conf.filterPlugin, "id", pluginProperties);
    if (filter == null) {
      throw new IllegalArgumentException("Could not find filter plugin " + conf.filterPlugin);
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    filter = context.newPluginInstance("id");
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
    if (!filter.test(input)) {
      emitter.emit(input);
    }
  }

  /**
   * Configuration for the transform
   */
  public static class Conf extends PluginConfig {
    private String filterPlugin;

    @Nullable
    private String filterProperties;

    public Conf() {
      filterPlugin = null;
      filterProperties = GSON.toJson(Collections.emptyMap());
    }
  }

  public static ETLPlugin getPlugin(String filterPlugin, Map<String, String> filterProperties) {
    Map<String, String> properties = new HashMap<>();
    properties.put("filterPlugin", filterPlugin);
    properties.put("filterProperties", GSON.toJson(filterProperties));
    return new ETLPlugin(NAME, Transform.PLUGIN_TYPE, properties, null);
  }
}
