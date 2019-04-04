/*
 * Copyright © 2019 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.proto.v2.validation;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Serializes and deserializes {@link ValidationError ValidtionErrors} into and from their specific types.
 */
public class ValidationErrorSerDe implements JsonSerializer<ValidationError>, JsonDeserializer<ValidationError> {

  @Override
  public ValidationError deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject obj = json.getAsJsonObject();
    ValidationError.Type type = context.deserialize(obj.get("type"), ValidationError.Type.class);
    switch (type) {
      case INVALID_FIELD:
        return context.deserialize(json, InvalidConfigPropertyError.class);
      case PLUGIN_NOT_FOUND:
        return context.deserialize(json, PluginNotFoundError.class);
      case STAGE_ERROR:
        return context.deserialize(json, StageValidationError.class);
      default:
        JsonElement messageElement = obj.get("message");
        String message = messageElement == null ? null : messageElement.getAsString();
        return new ValidationError(type, message);
    }
  }

  @Override
  public JsonElement serialize(ValidationError src, Type typeOfSrc, JsonSerializationContext context) {
    switch (src.getType()) {
      case INVALID_FIELD:
        return context.serialize(src, InvalidConfigPropertyError.class);
      case PLUGIN_NOT_FOUND:
        return context.serialize(src, PluginNotFoundError.class);
      case STAGE_ERROR:
        return context.serialize(src, StageValidationError.class);
      default:
        JsonObject obj = new JsonObject();
        obj.add("type", context.serialize(src.getType(), ValidationError.Type.class));
        obj.add("message", context.serialize(src.getMessage(), String.class));
        return obj;
    }
  }
}
