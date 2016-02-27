/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.mock;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transform;

import java.util.HashMap;
import java.util.Map;

/**
 * Transform that filters out records whose configured field is a configured value.
 * For example, can filter all records whose 'foo' field is equal to 'bar'. Assumes the field is of type string.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("StringValueFilter")
public class StringValueFilterTransform extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;

  public StringValueFilterTransform(Config config) {
    this.config = config;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    String value = input.get(config.field);
    if (!config.value.equals(value)) {
      emitter.emit(input);
    }
  }

  public static class Config extends PluginConfig {
    private String field;
    private String value;
  }

  public static co.cask.cdap.etl.common.Plugin getPlugin(String field, String value) {
    Map<String, String> properties = new HashMap<>();
    properties.put("field", field);
    properties.put("value", value);
    return new co.cask.cdap.etl.common.Plugin("StringValueFilter", properties);
  }
}
