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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Serializes the fields of a StructuredRecord as a JsonObject.
 */
public class StructuredRecordSerializer implements JsonSerializer<StructuredRecord> {

  @Override
  public JsonElement serialize(StructuredRecord src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject obj = new JsonObject();
    for (Schema.Field field : src.getSchema().getFields()) {
      obj.add(field.getName(), context.serialize(src.get(field.getName())));
    }
    return obj;
  }
}
