/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.format;

import co.cask.cdap.internal.io.Schema;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Instance of a record structured by a {@link Schema}. Fields are accessible by name.
 */
public class StructuredRecord {
  private final Schema schema;
  private final Map<String, Object> fields;

  private StructuredRecord(Schema schema, Map<String, Object> fields) {
    this.schema = schema;
    this.fields = fields;
  }

  /**
   * Get the schema of the record.
   *
   * @return schema of the record.
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Get the value of a field in the record.
   *
   * @param fieldName field to get.
   * @param <T> type of object of the field value.
   * @return value of the field.
   */
  public <T> T get(String fieldName) {
    return (T) fields.get(fieldName);
  }

  /**
   * Get a builder for creating a record with the given schema.
   *
   * @param schema schema for the record to build.
   * @return builder for creating a record with the given schema.
   */
  public static Builder builder(Schema schema) {
    return new Builder(schema);
  }

  /**
   * Builder for creating a {@link StructuredRecord}.
   * TODO: enforce schema correctness?
   */
  public static class Builder {
    private final Schema schema;
    private Map<String, Object> fields;

    private Builder(Schema schema) {
      Preconditions.checkArgument(schema.getType() == Schema.Type.RECORD);
      this.schema = schema;
      this.fields = Maps.newHashMap();
    }

    public Builder set(String fieldName, Object value) {
      fields.put(fieldName, value);
      return this;
    }

    public StructuredRecord build() {
      return new StructuredRecord(schema, fields);
    }
  }
}
