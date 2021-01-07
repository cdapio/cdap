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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.cdap.api.plugin.Requirements;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Set;

/**
 * JSON Codec for @link{Requirements}
 */
public class RequirementsCodec implements JsonDeserializer<Requirements>, JsonSerializer<Requirements> {

  public static final String DATASET_TYPES = "datasetTypes";
  public static final String CAPABILITIES = "capabilities";

  @Override
  public Requirements deserialize(JsonElement json, Type typeOfT,
                                  JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    return new Requirements(getSet(jsonObject, context, DATASET_TYPES), getSet(jsonObject, context, CAPABILITIES));
  }

  private Set<String> getSet(JsonObject jsonObject, JsonDeserializationContext context, String fieldName) {
    JsonElement jsonElement = jsonObject.get(fieldName);
    if (jsonElement == null) {
      return Collections.emptySet();
    }
    return context.deserialize(jsonElement, Set.class);
  }

  @Override
  public JsonElement serialize(Requirements src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.add(DATASET_TYPES, context.serialize(src.getDatasetTypes()));
    jsonObject.add(CAPABILITIES, context.serialize(src.getCapabilities()));
    return jsonObject;
  }
}
