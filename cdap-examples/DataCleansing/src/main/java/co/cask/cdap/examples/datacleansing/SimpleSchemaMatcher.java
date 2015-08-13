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

package co.cask.cdap.examples.datacleansing;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Map;

/**
 * A schema matcher for flat-record schemas with simple (or nullable of simple) fields.
 */
public class SimpleSchemaMatcher {

  private final Schema schema;

  public SimpleSchemaMatcher(Schema schema) {
    this.schema = schema;
  }

  /**
   * Determines whether or not this matcher's schema fits a piece of data.
   * A failure to match could arise from any of:
   *  - a non-nullable field of the schema is missing from the data
   *  - a numerical field of the schema has non-numerical characters in it
   *  - the schema has non-simple types
   *
   * @param data a JSON string to check if the schema matches it
   * @return true if the schema matches the given data
   */
  public boolean matches(String data) {
    // rely on validations in the StructuredRecord's builder
    try {
      JsonObject jsonObject = new JsonParser().parse(data).getAsJsonObject();
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
        builder.convertAndSet(entry.getKey(), entry.getValue().getAsString());
      }
      builder.build();
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
