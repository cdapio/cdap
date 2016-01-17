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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * An {@link Indexer} to generate indexes for a {@link Schema}
 */
public class SchemaIndexer implements Indexer {

  @Override
  public Set<String> getIndexes(MetadataEntry entry) {
    return createIndexes(getSchema(entry.getValue()));
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
    Set<String> indexes = new HashSet<>();
    if (schema == null) {
      return indexes;
    }

    if (schema.isSimpleOrNullableSimple()) {
      Schema.Type type = getSimpleType(schema);
      // put the type as index
      indexes.add(type.toString());
    } else {
      for (Schema.Field field : schema.getFields()) {
        String fieldName = field.getName();
        // TODO: What if field.getSchema() is not simple or nullable simple?
        String fieldType = getSimpleType(field.getSchema()).toString();
        indexes.add(fieldName + MetadataDataset.KEYVALUE_SEPARATOR + fieldType);
        indexes.add(fieldName);
      }
    }
    return indexes;
  }

  private Schema.Type getSimpleType(Schema schema) {
    if (schema.isNullable()) {
      schema = schema.getNonNullable();
    }
    return schema.getType();
  }
}
