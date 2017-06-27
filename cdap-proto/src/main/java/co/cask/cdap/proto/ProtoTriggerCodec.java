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

package co.cask.cdap.proto;

import co.cask.cdap.internal.schedule.trigger.Trigger;
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
 * Serialization and deserialization of Triggers as Json.
 */
public class ProtoTriggerCodec implements JsonSerializer<Trigger>, JsonDeserializer<Trigger> {

  /**
   * Maps each type to a class for deserialization.
   */
  private static final Map<ProtoTrigger.Type, Class<? extends Trigger>> TYPE_TO_TRIGGER = generateMap();

  private static Map<ProtoTrigger.Type, Class<? extends Trigger>> generateMap() {
    Map<ProtoTrigger.Type, Class<? extends Trigger>> map = new HashMap<>();
    map.put(ProtoTrigger.Type.TIME, ProtoTrigger.TimeTrigger.class);
    map.put(ProtoTrigger.Type.PARTITION, ProtoTrigger.PartitionTrigger.class);
    map.put(ProtoTrigger.Type.STREAM_SIZE, ProtoTrigger.StreamSizeTrigger.class);
    map.put(ProtoTrigger.Type.PROGRAM_STATUS, ProtoTrigger.ProgramStatusTrigger.class);
    return map;
  }

  /**
   * Maps each trigger type to a class for deserialization.
   */
  private final Map<ProtoTrigger.Type, Class<? extends Trigger>> typeClassMap;

  /**
   * Constructs a Codec with the default mapping from trigger type to trigger class.
   */
  public ProtoTriggerCodec() {
    this(TYPE_TO_TRIGGER);
  }

  /**
   * Constructs a Codec with a custom mapping from trigger type to trigger class.
   */
  protected ProtoTriggerCodec(Map<ProtoTrigger.Type, Class<? extends Trigger>> typeClassMap) {
    this.typeClassMap = typeClassMap;
  }

  @Override
  public JsonElement serialize(Trigger src, Type typeOfSrc, JsonSerializationContext context) {
    // this assumes that Trigger is an abstract class (every instance will have a concrete type that's not Trigger)
    return context.serialize(src, src.getClass());
  }

  @Override
  public Trigger deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    if (json == null) {
      return null;
    }
    if (!(json instanceof JsonObject)) {
      throw new JsonParseException("Expected a JsonObject but found a " + json.getClass().getName());
    }
    JsonObject object = (JsonObject) json;
    JsonElement typeJson = object.get("type");
    ProtoTrigger.Type triggerType = context.deserialize(typeJson, ProtoTrigger.Type.class);
    Class<? extends Trigger> subClass = typeClassMap.get(triggerType);
    if (subClass == null) {
      throw new JsonParseException("Unable to map trigger type " + triggerType + " to a trigger class");
    }
    ProtoTrigger trigger = context.deserialize(json, subClass);
    trigger.validate();
    return trigger;
  }
}
