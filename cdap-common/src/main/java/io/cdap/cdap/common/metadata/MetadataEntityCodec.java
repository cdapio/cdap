/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.metadata;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataEntity.KeyValue;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.proto.id.ApplicationId;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JSON Type adapter for MetadataEntity. Version for {@link MetadataUtil#isVersionedEntityType(String)}
 * will be removed during serialization since it is not persisted in storage and will always be the
 * default value "-SNAPSHOT".
 */
public class MetadataEntityCodec implements JsonSerializer<MetadataEntity>,
    JsonDeserializer<MetadataEntity> {

  private static final Type MAP_DETAILS_TYPE = new TypeToken<LinkedHashMap<String, String>>() {
  }.getType();

  @Override
  public MetadataEntity deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context)
      throws JsonParseException {
    if (!typeOfT.equals(MetadataEntity.class)) {
      return context.deserialize(json, typeOfT);
    }
    JsonObject object = json.getAsJsonObject();
    JsonElement detailsJson = object.get("details");
    JsonElement typeJson = object.get("type");
    LinkedHashMap<String, String> details = context.deserialize(detailsJson, MAP_DETAILS_TYPE);
    String type = context.deserialize(typeJson, String.class);
    MetadataEntity.Builder builder = MetadataEntity.builder();
    int index = 0;
    for (Map.Entry<String, String> keyValue : details.entrySet()) {
      if (keyValue.getKey().equals(type)) {
        builder.appendAsType(keyValue.getKey(), keyValue.getValue());
      } else {
        builder.append(keyValue.getKey(), keyValue.getValue());
      }
      index++;
      // add back default version that's removed during serialization
      if (index == 2 && MetadataUtil.isVersionedEntityType(type) && !details.containsKey(
          MetadataEntity.VERSION)) {
        builder.append(MetadataEntity.VERSION, ApplicationId.DEFAULT_VERSION);
      }
    }
    return builder.build();
  }

  @Override
  public JsonElement serialize(MetadataEntity src, Type typeOfSec,
      JsonSerializationContext context) {
    JsonObject entityObject = new JsonObject();
    JsonObject details = new JsonObject();
    for (KeyValue keyValue : src) {
      // skip version if serializing a versionless entity
      if (!(keyValue.getKey().equals(MetadataEntity.VERSION) && MetadataUtil.isVersionedEntityType(
          src.getType()))) {
        details.add(keyValue.getKey(), context.serialize(keyValue.getValue()));
      }
    }
    entityObject.add("details", details);
    entityObject.add("type", context.serialize(src.getType()));
    return entityObject;
  }
}
