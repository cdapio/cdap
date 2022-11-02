/*
 *  Copyright © 2022 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.cdap.etl.mock.batch;

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.dynamic.BatchConfigProvider;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * ConfigProvider that can be used in unit tests.
 */
@Plugin(type = BatchConfigProvider.PLUGIN_TYPE)
@Name("Mock")
public class MockConfigProvider implements BatchConfigProvider {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private static final Gson GSON = new Gson();
  private final Conf config;

  public MockConfigProvider(Conf config) {
    this.config = config;
  }

  @Override
  public String get() {
    return config.conf;
  }

  public static class Conf extends PluginConfig {
    private String conf;
  }

  public static ETLPlugin getPlugin(ETLBatchConfig pipelineConfig) {
    Map<String, String> properties = new HashMap<>();
    properties.put("conf", GSON.toJson(pipelineConfig));
    return new ETLPlugin("Mock", BatchSource.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("conf", new PluginPropertyField("conf", "", "string", true, false));
    return PluginClass.builder().setName("Mock").setType(BatchConfigProvider.PLUGIN_TYPE)
      .setDescription("").setClassName(MockConfigProvider.class.getName()).setProperties(properties)
      .setConfigFieldName("config").build();
  }
}
