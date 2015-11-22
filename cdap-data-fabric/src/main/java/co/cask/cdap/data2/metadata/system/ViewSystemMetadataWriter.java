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

package co.cask.cdap.data2.metadata.system;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ViewSpecification;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link AbstractSystemMetadataWriter} for an {@link Id.Stream.View view}.
 */
public class ViewSystemMetadataWriter extends AbstractSystemMetadataWriter {

  private final ViewSpecification viewSpec;

  public ViewSystemMetadataWriter(MetadataStore metadataStore, Id.Stream.View viewId, ViewSpecification viewSpec) {
    super(metadataStore, viewId);
    this.viewSpec = viewSpec;
  }

  @Override
  Map<String, String> getSystemPropertiesToAdd() {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    Schema schema = viewSpec.getFormat().getSchema();
    if (schema != null) {
      for (Schema.Field field : schema.getFields()) {
        properties.put(SCHEMA_FIELD_PROPERTY_PREFIX + CTRL_A + field.getName(), field.getSchema().toString());
      }
    }
    return properties.build();
  }

  @Override
  String[] getSystemTagsToAdd() {
    return new String[0];
  }
}
