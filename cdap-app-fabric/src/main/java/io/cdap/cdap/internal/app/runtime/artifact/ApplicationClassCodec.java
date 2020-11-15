/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.Requirements;

import java.lang.reflect.Type;
import javax.annotation.Nullable;

/**
 * JSON codec for {@link ApplicationClass}
 */
public class ApplicationClassCodec implements JsonDeserializer<ApplicationClass>, JsonSerializer<ApplicationClass> {

  private static final String REQUIREMENTS = "requirements";
  private static final String CLASS_NAME = "className";
  private static final String DESCRIPTION = "description";
  private static final String CONFIG_SCHEMA = "configSchema";

  @Override
  public ApplicationClass deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    Schema configSchema = getConfigSchema(jsonObject, context);
    Requirements requirements = getRequirements(jsonObject, context);
    return new ApplicationClass(getStringValue(jsonObject, CLASS_NAME), getStringValue(jsonObject, DESCRIPTION),
                                configSchema, requirements);
  }

  @Override
  public JsonElement serialize(ApplicationClass src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty(CLASS_NAME, src.getClassName());
    jsonObject.addProperty(DESCRIPTION, src.getDescription());
    jsonObject.add(CONFIG_SCHEMA, context.serialize(src.getConfigSchema()));
    jsonObject.add(REQUIREMENTS, context.serialize(src.getRequirements()));
    return jsonObject;
  }

  @Nullable
  private String getStringValue(JsonObject jsonObject, String name) {
    JsonElement jsonElement = jsonObject.get(name);
    if (jsonElement == null) {
      return null;
    }
    return jsonElement.getAsString();
  }

  @Nullable
  private Schema getConfigSchema(JsonObject jsonObject, JsonDeserializationContext context) {
    JsonElement jsonElement = jsonObject.get(CONFIG_SCHEMA);
    if (jsonElement == null) {
      return null;
    }
    return context.deserialize(jsonElement, Schema.class);
  }

  private Requirements getRequirements(JsonObject jsonObject, JsonDeserializationContext context) {
    JsonElement requirementJSON = jsonObject.get(REQUIREMENTS);
    if (requirementJSON == null) {
      return Requirements.EMPTY;
    }
    return context.deserialize(requirementJSON, Requirements.class);
  }
}
