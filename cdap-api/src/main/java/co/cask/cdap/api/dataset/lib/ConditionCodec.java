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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.dataset.lib.partitioned.ComparableCodec;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Codec used to serialize and deserialize {@link PartitionFilter.Condition}s.
 */
public class ConditionCodec extends ComparableCodec
  implements JsonSerializer<PartitionFilter.Condition>, JsonDeserializer<PartitionFilter.Condition> {

  @Override
  public PartitionFilter.Condition deserialize(JsonElement jsonElement, Type type,
                                               JsonDeserializationContext deserializationContext)
    throws JsonParseException {

    JsonObject jsonObject = jsonElement.getAsJsonObject();
    boolean isSingleValue = jsonObject.get("isSingleValue").getAsBoolean();
    if (isSingleValue) {
      return new PartitionFilter.Condition<>(jsonObject.get("fieldName").getAsString(),
                                             deserializeComparable(jsonObject.get("lower"), deserializationContext));
    } else {
      return new PartitionFilter.Condition<>(jsonObject.get("fieldName").getAsString(),
                                             deserializeComparable(jsonObject.get("lower"), deserializationContext),
                                             deserializeComparable(jsonObject.get("upper"), deserializationContext));
    }
  }

  @Override
  public JsonElement serialize(PartitionFilter.Condition condition, Type type,
                               JsonSerializationContext jsonSerializationContext) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty("fieldName", condition.getFieldName());
    jsonObj.add("lower", serializeComparable(condition.getLower(), jsonSerializationContext));
    jsonObj.add("upper", serializeComparable(condition.getUpper(), jsonSerializationContext));
    jsonObj.addProperty("isSingleValue", condition.isSingleValue());
    return jsonObj;
  }
}
