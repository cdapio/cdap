/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store.adapters;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.cdap.api.plugin.PluginClass;
import java.lang.reflect.Type;

/**
 * Gson serializer for {@link PluginClass}. Serializes only essential identification fields: 'type'
 * and 'name'.
 */
public class PluginClassSerializer implements JsonSerializer<PluginClass> {

  /**
   * Serializes a {@link PluginClass} to a JSON object. Only 'type' and 'name' fields are included
   * in the serialized output, excluding other plugin information.
   */
  @Override
  public JsonElement serialize(PluginClass src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("type", src.getType());
    jsonObject.addProperty("name", src.getName());
    // Exclude other plugin information.
    return jsonObject;
  }
}
