/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
import co.cask.cdap.api.data.schema.SchemaWalker;
import co.cask.cdap.data2.metadata.dataset.MetadataEntry;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.spi.metadata.MetadataConstants;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * An {@link Indexer} to generate indexes for a {@link Schema}
 */
public class SchemaIndexer extends DefaultValueIndexer {

  @Override
  public Set<String> getIndexes(MetadataEntry entry) {
    try {
      Set<String> indexes = createIndexes(getSchema(entry.getValue()));
      return addKeyValueIndexes(entry.getKey(), indexes);
    } catch (IOException e) {
      // this happens if the schema is invalid - fall back to default indexer
      return super.getIndexes(entry);
    }
  }

  @Override
  public SortInfo.SortOrder getSortOrder() {
    return SortInfo.SortOrder.WEIGHTED;
  }

  private Schema getSchema(String schemaStr) throws IOException {
    if (schemaStr.startsWith("\"") && schemaStr.endsWith("\"")) {
      // simple type in lower case
      schemaStr = schemaStr.substring(1, schemaStr.length() - 1);
      return Schema.of(Schema.Type.valueOf(schemaStr.toUpperCase()));
    }
    // otherwise its a json
    return Schema.parseJson(schemaStr);
  }

  private Set<String> createIndexes(Schema schema) {
    if (schema == null) {
      return Collections.emptySet();
    }
    Set<String> indexes = new HashSet<>();
    SchemaWalker.walk(schema, (field, subschema) -> createIndexes(indexes, subschema, field));
    return indexes;
  }

  private void createIndexes(Set<String> indexes, Schema schema, @Nullable String fieldName) {
    if (fieldName != null) {
      String type = getSimpleType(schema);
      indexes.add(fieldName + MetadataConstants.KEYVALUE_SEPARATOR + type);
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
      indexesWithKeyValue.add(key + MetadataConstants.KEYVALUE_SEPARATOR + index);
    }
    indexesWithKeyValue.add(MetadataConstants.PROPERTIES_KEY + MetadataConstants.KEYVALUE_SEPARATOR + key);
    return indexesWithKeyValue;
  }
}
