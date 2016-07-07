/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.etl.mock.transform;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * Transform that filters out records whose configured field is a configured value.
 * For example, can filter all records whose 'x' field is equal to 5. Assumes the field is of type int.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("IntValueFilter")
public class IntValueFilterTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;

  public IntValueFilterTransform(Config config) {
    this.config = config;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    Integer value = input.get(config.field);
    if (value != config.value) {
      emitter.emit(input);
    }
  }

  /**
   * Config for the transform.
   */
  public static class Config extends PluginConfig {
    private String field;
    private int value;
  }

  public static ETLPlugin getPlugin(String field, int value) {
    Map<String, String> properties = new HashMap<>();
    properties.put("field", field);
    properties.put("value", String.valueOf(value));
    return new ETLPlugin("IntValueFilter", Transform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("field", new PluginPropertyField("field", "", "string", true, false));
    properties.put("value", new PluginPropertyField("value", "", "int", true, false));
    return new PluginClass(Transform.PLUGIN_TYPE, "IntValueFilter", "", IntValueFilterTransform.class.getName(),
                           "config", properties);
  }
}
