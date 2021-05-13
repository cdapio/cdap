/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.proto.security;

import com.google.gson.Gson;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.Streams;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 * This {@link com.google.gson.Gson} adapter allows to serialize actions or permissions,
 * but automatically converts to permissions on read.
 * To maintain backwards compatibility it uses same format (plan string) for actions and json object for permissions.
 */
public class PermissionAdapterFactory extends TypeAdapter<ActionOrPermission> implements TypeAdapterFactory {
  @Override
  public ActionOrPermission read(JsonReader in) throws IOException {
    JsonElement json = Streams.parse(in);
    if (json.isJsonNull()) {
      return null;
    }
    if (json.isJsonPrimitive()) {
      Action action = Action.valueOf(json.getAsString());
      return action.getPermission();
    }
    JsonObject map = json.getAsJsonObject();
    JsonElement permissionType = map.get("type");
    if (permissionType == null) {
      throw new JsonParseException("Expected type in Permission JSON");
    }
    JsonElement permissionName = map.get("name");
    if (permissionName == null) {
      throw new JsonParseException("Expected name in Permission JSON");
    }

    try {
      return PermissionType.valueOf(permissionType.getAsString(), permissionName.getAsString());
    } catch (IllegalArgumentException e) {
      //Fall through
    }
    JsonElement checkedOnParent = map.get("parent");
    if (checkedOnParent != null && checkedOnParent.getAsBoolean()) {
      return SpecialPermission.OTHER_CHECKED_ON_PARENT;
    }
    return SpecialPermission.OTHER;
  }

  @Override
  public void write(JsonWriter out, ActionOrPermission src) throws IOException {
    if (src instanceof Action) {
      out.value(((Action) src).name());
    }
    Permission permission = (Permission) src;
    out
      .beginObject()
      .name("type").value(permission.getPermissionType().name())
      .name("name").value(permission.name());
    if (permission.isCheckedOnParent()) {
      out.name("parent").value(true);
    }
    out.endObject();
  }

  @Override
  public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
    if (ActionOrPermission.class.isAssignableFrom(type.getRawType())) {
      return (TypeAdapter<T>) this;
    }
    return null;
  }

}
