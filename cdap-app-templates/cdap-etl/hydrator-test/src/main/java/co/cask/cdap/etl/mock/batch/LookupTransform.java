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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transform used to test lookup functionality. Takes a field name whose value will be used as the key in a lookup.
 * If the lookup is from a Table, the value of the lookup will be a Row, and the columns of the Row will be added as
 * individual fields of the output StructuredRecord.
 * If the lookup is from a KeyValueTable, the value of the lookup will be a String, and the 'destinationField' will
 * be used to determine where in the output StructuredRecord to place the value.
 *
 * @param <T> The type of object that will be returned for the lookup.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Lookup")
public class LookupTransform<T> extends Transform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;
  private Lookup<T> lookup;

  public LookupTransform(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    lookup = context.provide(config.lookupName, new HashMap<String, String>());
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    T lookedUpValue = lookup.lookup((String) input.get(config.lookupKey));
    // for the output schema, copy all the input fields, and add the 'destinationField'
    List<Schema.Field> outFields = new ArrayList<>();
    for (Schema.Field field : input.getSchema().getFields()) {
      outFields.add(field);
    }
    if (lookedUpValue instanceof String) {
      outFields.add(Schema.Field.of(config.destinationField, Schema.of(Schema.Type.STRING)));
    } else if (lookedUpValue instanceof Row) {
      Row lookedupRow = (Row) lookedUpValue;
      for (byte[] column : lookedupRow.getColumns().keySet()) {
        outFields.add(Schema.Field.of(Bytes.toString(column), Schema.of(Schema.Type.STRING)));
      }
    } else {
      throw new IllegalArgumentException("Unexpected value type: " + lookedUpValue.getClass());
    }
    Schema outSchema = Schema.recordOf(input.getSchema().getRecordName(), outFields);

    // copy all the values
    StructuredRecord.Builder outputBuilder = StructuredRecord.builder(outSchema);
    for (Schema.Field inField : input.getSchema().getFields()) {
      if (inField.getName().equals(config.lookupKey)) {
        if (lookedUpValue instanceof String) {
          outputBuilder.set(config.destinationField, lookedUpValue);
        } else {
          // due to the check above, we know its a Row
          Row lookedupRow = (Row) lookedUpValue;
          for (Map.Entry<byte[], byte[]> entry : lookedupRow.getColumns().entrySet()) {
            outputBuilder.set(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
          }
        }
      }
      // what if the destinationField already exists?
      outputBuilder.set(inField.getName(), input.get(inField.getName()));
    }
    emitter.emit(outputBuilder.build());
  }

  /**
   * Config for the source.
   */
  public static class Config extends PluginConfig {
    private String lookupKey;
    private String destinationField;
    private String lookupName;
  }

  // note that destination is only used if the lookup table is a KeyValueTable
  public static ETLPlugin getPlugin(String lookupKey, String destinationField, String lookupName) {
    Map<String, String> properties = new HashMap<>();
    properties.put("lookupKey", lookupKey);
    properties.put("destinationField", destinationField);
    properties.put("lookupName", lookupName);
    return new ETLPlugin("Lookup", Transform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("lookupKey", new PluginPropertyField("fields", "", "string", true, false));
    properties.put("destinationField", new PluginPropertyField("destinationField", "", "string", true, false));
    properties.put("lookupName", new PluginPropertyField("lookupName", "", "string", true, false));
    return new PluginClass(Transform.PLUGIN_TYPE, "Lookup", "", LookupTransform.class.getName(),
                           "config", properties);
  }
}
