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

import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.plugin.PluginConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Filters out records where a field has a specific value. Used to test plugins created by other plugins.
 */
@Plugin(type = PluggableFilterTransform.FILTER_PLUGIN_TYPE)
@Name(ValueFilter.NAME)
public class ValueFilter implements Predicate<StructuredRecord> {
  public static final String NAME = "ValueFilter";
  private final Conf conf;

  public ValueFilter(Conf conf) {
    this.conf = conf;
  }

  @Override
  public boolean test(StructuredRecord input) {
    return conf.value.equals(input.get(conf.field));
  }

  public static class Conf extends PluginConfig {
    @Macro
    private String field;

    @Macro
    private String value;
  }

  public static Map<String, String> getProperties(String field, String value) {
    Map<String, String> properties = new HashMap<>();
    properties.put("field", field);
    properties.put("value", value);
    return properties;
  }
}
