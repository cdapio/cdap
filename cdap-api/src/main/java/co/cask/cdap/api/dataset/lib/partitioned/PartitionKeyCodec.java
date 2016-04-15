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

package co.cask.cdap.api.dataset.lib.partitioned;

import co.cask.cdap.api.dataset.lib.PartitionKey;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Codec used to serialize and deserialize {@link PartitionKey}s.
 */
public class PartitionKeyCodec extends ComparableCodec
  implements JsonSerializer<PartitionKey>, JsonDeserializer<PartitionKey> {

  @Override
  public PartitionKey deserialize(JsonElement jsonElement, Type type,
                                  JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
    JsonObject jsonObject = jsonElement.getAsJsonObject();
    PartitionKey.Builder builder = PartitionKey.builder();
    for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
      JsonArray jsonArray = entry.getValue().getAsJsonArray();
      builder.addField(entry.getKey(), deserializeComparable(jsonArray, jsonDeserializationContext));
    }
    return builder.build();
  }

  @Override
  public JsonElement serialize(PartitionKey partitionKey, Type type,
                               JsonSerializationContext jsonSerializationContext) {
    JsonObject jsonObj = new JsonObject();
    for (Map.Entry<String, Comparable> entry : partitionKey.getFields().entrySet()) {
      jsonObj.add(entry.getKey(), serializeComparable(entry.getValue(), jsonSerializationContext));
    }
    return jsonObj;
  }
}
