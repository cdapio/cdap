/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.datapipeline;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Codec to encode/decode TriggeringPipelinePropertyId object.
 */
public class TriggeringPipelinePropertyIdCodec implements JsonSerializer<TriggeringPipelinePropertyId>,
  JsonDeserializer<TriggeringPipelinePropertyId> {

  /**
   * Maps each type to a class for deserialization.
   */
  private static final Map<TriggeringPipelinePropertyId.Type, Class<? extends TriggeringPipelinePropertyId>>
    TYPE_TO_ID = generateMap();

  private static Map<TriggeringPipelinePropertyId.Type, Class<? extends TriggeringPipelinePropertyId>> generateMap() {
    Map<TriggeringPipelinePropertyId.Type, Class<? extends TriggeringPipelinePropertyId>> map = new HashMap<>();
    map.put(TriggeringPipelinePropertyId.Type.RUNTIME_ARG, TriggeringPipelineRuntimeArgId.class);
    map.put(TriggeringPipelinePropertyId.Type.PLUGIN_PROPERTY, TriggeringPipelinePluginPropertyId.class);
    map.put(TriggeringPipelinePropertyId.Type.TOKEN, TriggeringPipelineTokenId.class);
    return map;
  }

  /**
   * Maps each trigger type to a class for deserialization.
   */
  private final Map<TriggeringPipelinePropertyId.Type, Class<? extends TriggeringPipelinePropertyId>> typeClassMap;

  /**
   * Constructs a Codec with the default mapping from property type to property id class.
   */
  public TriggeringPipelinePropertyIdCodec() {
    this(TYPE_TO_ID);
  }

  /**
   * Constructs a Codec with a custom mapping from property type to property id class.
   */
  protected TriggeringPipelinePropertyIdCodec(Map<TriggeringPipelinePropertyId.Type,
    Class<? extends TriggeringPipelinePropertyId>> typeClassMap) {
    this.typeClassMap = typeClassMap;
  }

  @Override
  public JsonElement serialize(TriggeringPipelinePropertyId src, Type typeOfSrc, JsonSerializationContext context) {
    return context.serialize(src, src.getClass());
  }

  @Override
  public TriggeringPipelinePropertyId deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    if (json == null) {
      return null;
    }
    if (!(json instanceof JsonObject)) {
      throw new JsonParseException("Expected a JsonObject but found a " + json.getClass().getName());
    }
    JsonObject object = (JsonObject) json;
    JsonElement typeJson = object.get("type");
    TriggeringPipelinePropertyId.Type propertyType =
      context.deserialize(typeJson, TriggeringPipelinePropertyId.Type.class);
    Class<? extends TriggeringPipelinePropertyId> subClass = typeClassMap.get(propertyType);
    if (subClass == null) {
      throw new JsonParseException("Unable to map property type " + propertyType + " to a property id class");
    }
    TriggeringPipelinePropertyId propertyId = context.deserialize(json, subClass);
    return propertyId;
  }
}
