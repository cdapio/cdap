/*
 * Copyright © 2016 Cask Data, Inc.
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


package co.cask.cdap.data2.metadata.indexer;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.MetadataEntry;
import co.cask.cdap.data2.metadata.dataset.SortInfo;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * An {@link Indexer} to generate indexes for a {@link Schema}
 */
public class SchemaIndexer implements Indexer {

  @Override
  public Set<String> getIndexes(MetadataEntry entry) {
    Set<String> indexes = createIndexes(getSchema(entry.getValue()));
    return addKeyValueIndexes(entry.getKey(), indexes);
  }

  @Override
  public SortInfo.SortOrder getSortOrder() {
    return SortInfo.SortOrder.WEIGHTED;
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

  private Set<String> createIndexes(Schema schema) {
    if (schema == null) {
      return Collections.emptySet();
    }
    Set<String> indexes = new HashSet<>();
    processSchema(indexes, schema, schema.getRecordName());
    return indexes;
  }

  private void processSchema(Set<String> indexes, Schema schema, @Nullable String fieldName) {
    switch (schema.getType()) {
      case NULL:
        // Ignore null types
        break;
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BYTES:
      case ENUM:
      case STRING:
        createIndexes(indexes, schema, fieldName);
        break;
      case ARRAY:
        createIndexes(indexes, schema, fieldName);
        processSchema(indexes, schema.getComponentSchema(), schema.getComponentSchema().getRecordName());
        break;
      case MAP:
        createIndexes(indexes, schema, fieldName);
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        processSchema(indexes, mapSchema.getKey(), mapSchema.getKey().getRecordName());
        processSchema(indexes, mapSchema.getValue(), mapSchema.getValue().getRecordName());
        break;
      case RECORD:
        createIndexes(indexes, schema, fieldName);
        for (Schema.Field field : schema.getFields()) {
          processSchema(indexes, field.getSchema(), field.getName());
        }
        break;
      case UNION:
        createIndexes(indexes, schema, fieldName);
        for (Schema us : schema.getUnionSchemas()) {
          processSchema(indexes, us, us.getRecordName());
        }
    }
  }

  private void createIndexes(Set<String> indexes, Schema schema, @Nullable String fieldName) {
    if (fieldName != null) {
      String type = getSimpleType(schema);
      indexes.add(fieldName + MetadataDataset.KEYVALUE_SEPARATOR + type);
      indexes.add(fieldName);
    }
  }

  private String getSimpleType(Schema schema) {
    if (schema.isNullable()) {
      schema = schema.getNonNullable();
    }
    return schema.getType().toString();
  }


  private Set<String> addKeyValueIndexes(String key, Set<String> indexes) {
    Set<String> indexesWithKeyValue = new HashSet<>(indexes);
    for (String index : indexes) {
      indexesWithKeyValue.add(key + MetadataDataset.KEYVALUE_SEPARATOR + index);
    }
    return indexesWithKeyValue;
  }
}
