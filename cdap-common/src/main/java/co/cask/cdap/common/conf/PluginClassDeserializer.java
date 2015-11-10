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

package co.cask.cdap.common.conf;

import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginPropertyField;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * A Gson deserializater for creating {@link PluginClass} object from external plugin config file.
 * Used to verify that required fields are present and property map is never null.
 */
public class PluginClassDeserializer implements JsonDeserializer<PluginClass> {

  // Type for the PluginClass.properties map.
  private static final Type PROPERTIES_TYPE = new TypeToken<Map<String, PluginPropertyField>>() { }.getType();

  @Override
  public PluginClass deserialize(JsonElement json, Type typeOfT,
                                 JsonDeserializationContext context) throws JsonParseException {
    if (!json.isJsonObject()) {
      throw new JsonParseException("PluginClass should be a JSON Object");
    }

    JsonObject jsonObj = json.getAsJsonObject();

    String type = jsonObj.has("type") ? jsonObj.get("type").getAsString() : Plugin.DEFAULT_TYPE;
    String name = getRequired(jsonObj, "name").getAsString();
    String description = jsonObj.has("description") ? jsonObj.get("description").getAsString() : "";
    String className = getRequired(jsonObj, "className").getAsString();

    Map<String, PluginPropertyField> properties = jsonObj.has("properties")
      ? context.<Map<String, PluginPropertyField>>deserialize(jsonObj.get("properties"), PROPERTIES_TYPE)
      : ImmutableMap.<String, PluginPropertyField>of();

    return new PluginClass(type, name, description, className, null, properties);
  }

  private JsonElement getRequired(JsonObject jsonObj, String name) {
    if (!jsonObj.has(name)) {
      throw new JsonParseException("Property '" + name + "' is missing from PluginClass.");
    }
    return jsonObj.get(name);
  }
}
