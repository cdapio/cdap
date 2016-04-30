/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.etl.mock.realtime;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 * Codec for serializing, deserializing {@link StructuredRecord}.
 */
public class StructuredRecordCodec implements JsonDeserializer<StructuredRecord>, JsonSerializer<StructuredRecord> {

  @Override
  public StructuredRecord deserialize(JsonElement json, Type typeOfT,
                                      JsonDeserializationContext context) throws JsonParseException {
    JsonObject obj = json.getAsJsonObject();
    try {
      Schema schema = Schema.parseJson(obj.get("schema").getAsString());
      return StructuredRecordStringConverter.fromJsonString(obj.get("record").getAsString(), schema);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public JsonElement serialize(StructuredRecord src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject obj = new JsonObject();
    try {
      obj.addProperty("record", StructuredRecordStringConverter.toJsonString(src));
      obj.addProperty("schema", src.getSchema().toString());
      return obj;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
