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

package co.cask.cdap.internal.app.runtime.schedule.trigger;

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

import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.api.schedule.TriggerInfo;
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
 * Serialization and deserialization of TriggerInfo as Json.
 */
public class TriggerInfoCodec implements JsonSerializer<TriggerInfo>, JsonDeserializer<TriggerInfo> {

  /**
   * Maps each type to a class for deserialization.
   */
  private static final Map<Trigger.Type, Class<? extends TriggerInfo>> TYPE_TO_TRIGGER_INFO = generateMap();

  private static Map<Trigger.Type, Class<? extends TriggerInfo>> generateMap() {
    Map<Trigger.Type, Class<? extends TriggerInfo>> map = new HashMap<>();
    map.put(Trigger.Type.TIME, DefaultTimeTriggerInfo.class);
    map.put(Trigger.Type.PARTITION, DefaultPartitionTriggerInfo.class);
    map.put(Trigger.Type.STREAM_SIZE, DefaultStreamSizeTriggerInfo.class);
    map.put(Trigger.Type.PROGRAM_STATUS, DefaultProgramStatusTriggerInfo.class);
    return map;
  }

  /**
   * Maps each trigger type to a class for deserialization.
   */
  private final Map<Trigger.Type, Class<? extends TriggerInfo>> typeClassMap;

  /**
   * Constructs a Codec with the default mapping from trigger type to trigger info class.
   */
  public TriggerInfoCodec() {
    this(TYPE_TO_TRIGGER_INFO);
  }

  /**
   * Constructs a Codec with a custom mapping from trigger type to trigger info class.
   */
  protected TriggerInfoCodec(Map<Trigger.Type, Class<? extends TriggerInfo>> typeClassMap) {
    this.typeClassMap = typeClassMap;
  }

  @Override
  public JsonElement serialize(TriggerInfo src, Type typeOfSrc, JsonSerializationContext context) {
    return context.serialize(src, src.getClass());
  }

  @Override
  public TriggerInfo deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    if (json == null) {
      return null;
    }
    if (!(json instanceof JsonObject)) {
      throw new JsonParseException("Expected a JsonObject but found a " + json.getClass().getName());
    }
    JsonObject object = (JsonObject) json;
    JsonElement typeJson = object.get("type");
    Trigger.Type triggerType = context.deserialize(typeJson, Trigger.Type.class);
    Class<? extends TriggerInfo> subClass = typeClassMap.get(triggerType);
    if (subClass == null) {
      throw new JsonParseException("Unable to map trigger type " + triggerType + " to a TriggerInfo class");
    }
    return context.deserialize(json, subClass);
  }
}

