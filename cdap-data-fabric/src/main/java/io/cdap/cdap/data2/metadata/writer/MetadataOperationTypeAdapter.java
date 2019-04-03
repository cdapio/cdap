/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.writer;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;

/**
 * Gson type adapter for {@link MetadataOperation}.
 */
public class MetadataOperationTypeAdapter implements JsonDeserializer<MetadataOperation> {
  @Override
  public MetadataOperation deserialize(JsonElement json, Type type, JsonDeserializationContext context)
    throws JsonParseException {

    JsonObject jsonObj = json.getAsJsonObject();
    MetadataOperation.Type opType = context.deserialize(jsonObj.get("type"), MetadataOperation.Type.class);
    switch (opType) {
      case CREATE:
        return context.deserialize(json, MetadataOperation.Create.class);
      case DROP:
        return context.deserialize(json, MetadataOperation.Drop.class);
      case PUT:
        return context.deserialize(json, MetadataOperation.Put.class);
      case DELETE:
        return context.deserialize(json, MetadataOperation.Delete.class);
      case DELETE_ALL:
        return context.deserialize(json, MetadataOperation.DeleteAll.class);
      case DELETE_ALL_PROPERTIES:
        return context.deserialize(json, MetadataOperation.DeleteAllProperties.class);
      case DELETE_ALL_TAGS:
        return context.deserialize(json, MetadataOperation.DeleteAllTags.class);
      default:
        throw new IllegalArgumentException("Unknown operation type " + opType);
    }
  }
}
