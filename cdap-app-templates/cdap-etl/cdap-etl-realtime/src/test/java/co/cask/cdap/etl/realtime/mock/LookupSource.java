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

package co.cask.cdap.etl.realtime.mock;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.api.realtime.SourceState;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Source used to test lookup functionality. Takes a set of fields as config and emits a single record with
 * each field value being the result of lookup for that field.
 */
@Plugin(type = RealtimeSource.PLUGIN_TYPE)
@Name("Lookup")
public class LookupSource extends RealtimeSource<StructuredRecord> {
  private final Config config;
  private Set<String> fields;
  private Schema schema;
  private Lookup<String> lookup;

  public LookupSource(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    fields = new HashSet<>();
    List<Schema.Field> schemaFields = new ArrayList<>(fields.size());
    for (String fieldName : Splitter.on(',').split(config.fields)) {
      fields.add(fieldName);
      schemaFields.add(Schema.Field.of(fieldName, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    }
    schema = Schema.recordOf("lookupRecord", schemaFields);
    lookup = context.provide(config.lookupName, new HashMap<String, String>());
  }

  @Nullable
  @Override
  public SourceState poll(Emitter<StructuredRecord> writer, SourceState currentState) throws Exception {
    if (currentState.getState("done") == null) {
      Map<String, String> fieldValues = lookup.lookup(fields);
      StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
      for (Map.Entry<String, String> entry : fieldValues.entrySet()) {
        recordBuilder.set(entry.getKey(), entry.getValue());
      }
      writer.emit(recordBuilder.build());
    }
    return currentState;
  }

  public static class Config extends PluginConfig {
    private String fields;
    private String lookupName;
  }

  public static co.cask.cdap.etl.common.Plugin getPlugin(Set<String> fields, String lookupName) {
    Map<String, String> properties = new HashMap<>();
    properties.put("fields", Joiner.on(',').join(fields));
    properties.put("lookupName", lookupName);
    return new co.cask.cdap.etl.common.Plugin("Lookup", properties);
  }
}
