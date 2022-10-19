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

package io.cdap.cdap.internal.app.runtime.schedule.trigger;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.proto.TriggeringInfo;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Serializer/Deserializer for {@link TriggeringInfo}
 */
public class TriggeringInfoCodec implements JsonDeserializer<TriggeringInfo>,
  JsonSerializer<TriggeringInfo> {

  private static final Map<Trigger.Type, Class<? extends TriggeringInfo>> TYPE_TO_TRIGGER_INFO =
    generateMap();

  private static Map<Trigger.Type, Class<? extends TriggeringInfo>> generateMap() {
    Map<Trigger.Type, Class<? extends TriggeringInfo>> map = new HashMap<>();
    map.put(Trigger.Type.AND, TriggeringInfo.AndTriggeringInfo.class);
    map.put(Trigger.Type.OR, TriggeringInfo.OrTriggeringInfo.class);
    map.put(Trigger.Type.PROGRAM_STATUS, TriggeringInfo.ProgramStatusTriggeringInfo.class);
    map.put(Trigger.Type.TIME, TriggeringInfo.TimeTriggeringInfo.class);
    map.put(Trigger.Type.PARTITION, TriggeringInfo.PartitionTriggeringInfo.class);
    return map;
  }

  private final Map<Trigger.Type, Class<? extends TriggeringInfo>> typeClassMap;

  public TriggeringInfoCodec() {
    this(TYPE_TO_TRIGGER_INFO);
  }

  protected TriggeringInfoCodec(Map<Trigger.Type, Class<? extends TriggeringInfo>> typeClassMap) {
    this.typeClassMap = typeClassMap;
  }

  @Override
  @Nullable
  public TriggeringInfo deserialize(@Nullable JsonElement json, Type type,
                                    JsonDeserializationContext context) throws JsonParseException {
    if (json == null) {
      return null;
    }
    if (!(json instanceof JsonObject)) {
      throw new JsonParseException("Expected a JsonObject but found a " + json.getClass().getName());
    }
    JsonObject object = (JsonObject) json;
    JsonElement typeJson = object.get("type");
    Trigger.Type triggerType = context.deserialize(typeJson, Trigger.Type.class);
    Class<? extends TriggeringInfo> subClass = typeClassMap.get(triggerType);
    if (subClass == null) {
      throw new JsonParseException("Unable to map trigger type " + triggerType + " to a TriggerInfo class");
    }
    return context.deserialize(json, subClass);
  }

  @Override
  public JsonElement serialize(TriggeringInfo src, Type type, JsonSerializationContext context) {
    return context.serialize(src, src.getClass());
  }
}
