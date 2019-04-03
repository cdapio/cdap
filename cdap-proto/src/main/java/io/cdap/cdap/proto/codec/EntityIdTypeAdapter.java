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
 * Class for serialize/deserialize EntityId object to/from json through {@link com.google.gson.Gson Gson}.
 */
public final class EntityIdTypeAdapter implements JsonSerializer<EntityId>, JsonDeserializer<EntityId> {

  @Override
  public EntityId deserialize(JsonElement json, Type typeOfT,
                        JsonDeserializationContext context) throws JsonParseException {
    JsonObject map = json.getAsJsonObject();
    JsonElement entityTypeJson = map.get("entity");
    if (entityTypeJson == null) {
      throw new JsonParseException("Expected entity in EntityId JSON");
    }

    String entityTypeString = entityTypeJson.getAsString();
    EntityType type = EntityType.valueOf(entityTypeString);
    if (type == null) {
      throw new JsonParseException("Invalid entity: " + entityTypeString);
    }

    return context.deserialize(json, type.getIdClass());
  }

  @Override
  public JsonElement serialize(EntityId src, Type typeOfSrc, JsonSerializationContext context) {
    return context.serialize(src);
  }
}
