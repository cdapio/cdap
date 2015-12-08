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

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link AbstractSystemMetadataWriter} for a {@link Id.DatasetInstance dataset}.
 */
public class DatasetSystemMetadataWriter extends AbstractSystemMetadataWriter {
  private final Id.DatasetInstance dsInstance;
  private final String dsType;
  private final DatasetProperties dsProperties;
  private final SystemDatasetInstantiatorFactory dsInstantiatorFactory;

  public DatasetSystemMetadataWriter(MetadataStore metadataStore,
                                     SystemDatasetInstantiatorFactory dsInstantiatorFactory,
                                     Id.DatasetInstance dsInstance, DatasetProperties dsProperties,
                                     @Nullable String dsType) {
    super(metadataStore, dsInstance);
    this.dsInstance = dsInstance;
    this.dsType = dsType;
    this.dsProperties = dsProperties;
    this.dsInstantiatorFactory = dsInstantiatorFactory;
  }

  @Override
  Map<String, String> getSystemPropertiesToAdd() {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    if (dsProperties.getProperties().containsKey("schema")) {
      addSchema(properties, dsProperties.getProperties().get("schema"));
    }
    if (dsType != null) {
      properties.put("type", dsType);
    }
    return properties.build();
  }

  @Override
  String[] getSystemTagsToAdd() {
    List<String> tags = new ArrayList<>();
    try (SystemDatasetInstantiator dsInstantiator = dsInstantiatorFactory.create();
         Dataset dataset = dsInstantiator.getDataset(dsInstance);) {
      if (dataset instanceof RecordScannable) {
        tags.add(RecordScannable.class.getSimpleName());
      }
      if (dataset instanceof RecordWritable) {
        tags.add(RecordWritable.class.getSimpleName());
      }
      if (dataset instanceof BatchReadable) {
        tags.add(BatchReadable.class.getSimpleName());
      }
      if (dataset instanceof BatchWritable) {
        tags.add(BatchWritable.class.getSimpleName());
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return tags.toArray(new String[tags.size()]);
  }

  private void addSchema(ImmutableMap.Builder<String, String> propertiesToUpdate, String schemaStr) {
    Schema schema = getSchema(schemaStr);
    if (schema.isSimpleOrNullableSimple()) {
      Schema.Type type;
      if (schema.isNullable()) {
        type = schema.getNonNullable().getType();
      } else {
        type = schema.getType();
      }
      propertiesToUpdate.put(SCHEMA_FIELD_PROPERTY_PREFIX, type.toString());
    } else {
      for (Schema.Field field : schema.getFields()) {
        propertiesToUpdate.put(SCHEMA_FIELD_PROPERTY_PREFIX + CTRL_A + field.getName(), field.getSchema().toString());
      }
    }
  }

  private Schema getSchema(String schemaStr) {
    if (schemaStr.startsWith("\"") && schemaStr.endsWith("\"")) {
      // simple type in lower case
      schemaStr = schemaStr.substring(1, schemaStr.length() - 1);
      return Schema.of(Schema.Type.valueOf(schemaStr.toUpperCase()));
    }
    // otherwise its a json
    try {
      return Schema.parseJson(schemaStr);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
