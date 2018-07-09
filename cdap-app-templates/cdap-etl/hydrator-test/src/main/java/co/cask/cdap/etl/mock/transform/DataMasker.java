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
 */

package co.cask.cdap.etl.mock.transform;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.StageSubmitterContext;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * Scrambles a field which has an associated metadata
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Masker")
public class DataMasker extends Transform<StructuredRecord, StructuredRecord> {

  private static final String FIELD_TO_SCRAMBLE = "fieldToScramble";
  private final DataMaskerConfig config;

  public DataMasker(DataMaskerConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    Schema inputSchema = context.getInputSchema();
    for (Schema.Field field : inputSchema.getFields()) {
      Metadata metadata =
        context.getMetadata(MetadataEntity.builder(MetadataEntity.ofDataset(context.getNamespace(), config.dsName))
                              .appendAsType("field", field.getName()).build()).get(MetadataScope.USER);
      if (metadata.getTags().contains(config.tag)) {
        context.getArguments().set(FIELD_TO_SCRAMBLE, field.getName());
      }
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(input.getSchema());
    for (Schema.Field field : input.getSchema().getFields()) {
      String fieldName = field.getName();
      String val = input.get(fieldName);
      if (fieldName.equals(getContext().getArguments().get(FIELD_TO_SCRAMBLE))) {
        val = config.mask;
      }
      builder.set(fieldName, val);
    }
    emitter.emit(builder.build());
  }

  /**
   * Config for {@link DataMasker}
   */
  public static class DataMaskerConfig extends PluginConfig {
    private String tag;
    private String dsName;
    private String mask;
  }

  public static ETLPlugin getPlugin(String dsName, String tag, String mask) {
    Map<String, String> properties = new HashMap<>();
    properties.put("tag", tag);
    properties.put("dsName", dsName);
    properties.put("mask", mask);
    return new ETLPlugin("Masker", Transform.PLUGIN_TYPE, properties, null);
  }
}
