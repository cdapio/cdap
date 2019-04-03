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

package co.cask.cdap.etl.mock.batch;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transform which filters structured records
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("FilterTransform")
public class FilterTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;
  private URL url;

  public FilterTransform(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    url = context.getServiceURL("ServiceApp", "Name");
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder outputBuilder = getOutputBuilder(input);

    String name = input.get(config.name);
    URL nameUrl = new URL(url, "name/" + name);
    HttpURLConnection connection = (HttpURLConnection) nameUrl.openConnection();
    String response;
    try {
      response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
      // Only emit records for which name is stored in the service
      if (response.equalsIgnoreCase(name)) {
        emitter.emit(outputBuilder.build());
      }
    } finally {
      connection.disconnect();
    }
  }

  private StructuredRecord.Builder getOutputBuilder(StructuredRecord input) {
    List<Schema.Field> outFields = new ArrayList<>();
    for (Schema.Field field : input.getSchema().getFields()) {
      outFields.add(field);
    }
    Schema outSchema = Schema.recordOf(input.getSchema().getRecordName(), outFields);

    // copy all the values
    StructuredRecord.Builder outputBuilder = StructuredRecord.builder(outSchema);
    for (Schema.Field inField : input.getSchema().getFields()) {
      outFields.add(inField);
      outputBuilder.set(inField.getName(), input.get(inField.getName()));
    }
    return outputBuilder;
  }

  /**
   * Config for FilterTransform
   */
  public static class Config extends PluginConfig {
    private String name;
  }

  // note that destination is only used if the lookup table is a KeyValueTable
  public static ETLPlugin getPlugin(String name) {
    Map<String, String> properties = new HashMap<>();
    properties.put("name", name);
    return new ETLPlugin("FilterTransform", Transform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("name", new PluginPropertyField("name", "", "string", true, false));
    return new PluginClass(Transform.PLUGIN_TYPE, "FilterTransform", "", FilterTransform.class.getName(),
                           "config", properties);
  }
}
