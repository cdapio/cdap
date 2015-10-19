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

package co.cask.cdap.proto.codec;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.EntityId;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Class for serialize/deserialize Id object to/from json through {@link com.google.gson.Gson Gson}.
 * Uses {@link EntityId} as the actual object to be serialized and deserialized.
 */
public final class IdTypeAdapter implements JsonSerializer<Id>, JsonDeserializer<Id> {

  @Override
  public Id deserialize(JsonElement json, Type typeOfT,
                        JsonDeserializationContext context) throws JsonParseException {
    JsonObject map = json.getAsJsonObject();
    JsonElement elementTypeJson = map.get("elementType");
    if (elementTypeJson == null) {
      throw new JsonParseException("Expected elementType in Id JSON");
    }

    String elementTypeString = elementTypeJson.getAsString();
    EntityType type = EntityType.valueOf(elementTypeString);
    if (type == null) {
      throw new JsonParseException("Invalid elementType: " + elementTypeString);
    }

    return context.deserialize(json, type.getIdClass());
  }

  @Override
  public JsonElement serialize(Id src, Type typeOfSrc, JsonSerializationContext context) {
    return context.serialize(src.toEntityId());
  }
}
