/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.api.data.schema;

import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import java.io.IOException;

/**
 * Class that provides a JVM-singleton schema cache. There are two ways to use it:
 * <ul>
 *   <li>If you already has a schema object, you can deduplicate it using {@link #intern(Schema) method}</li>
 *   <li>If you have a schema hash string and schema JSON representation, use {@link #fromJson(String, String)}
 *   method. This would allow you to skip schema deserialization if schema is already in the cache.
 *   </li>
 *   <li>If you have a top schema JSON previously produced by {@link Schema#toString()} you can use
 *   {@link #parseJson(String)} that will compute a hash out of json. Note that you should not use any modified
 *   (e.g. indented) json as a different hash would be computed and cache slot will be wasted. Also you should use it
 *   only for top level schemas for not to pollute cache.
 *   </li>
 * </ul>
 */
public class SchemaCache {

  private static final LRUCache<String, Schema> SCHEMA_CACHE = new LRUCache<>(100);
  private static final SchemaTypeAdapter SCHEMA_TYPE_ADAPTER = new SchemaTypeAdapter();

  public static final Schema intern(Schema schema) {
    return SCHEMA_CACHE.putIfAbsent(schema.getSchemaHash().toString(), schema);
  }

  public static final Schema fromJson(String schemaHashStr, String json) {
    return SCHEMA_CACHE.computeIfAbsent(schemaHashStr, () -> {
      try {
        return SCHEMA_TYPE_ADAPTER.fromJson(json);
      } catch (IOException e) {
        throw new IllegalArgumentException("Can't parse schema", e);
      }
    });
  }
}
