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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.schedule.TriggerInfo;
import co.cask.cdap.api.schedule.TriggeringScheduleInfo;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TriggerInfoCodec;
import com.google.common.reflect.TypeToken;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.twill.api.RunId;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Helper class to encoded/decode {@link co.cask.cdap.api.schedule.TriggeringScheduleInfo} to/from json.
 */
public class TriggeringScheduleInfoAdapter {

  private static final Type LIST_TRIGGER_INFO_TYPE = new TypeToken<List<TriggerInfo>>() { }.getType();
  private static final Type Map_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  public static GsonBuilder addTypeAdapters(GsonBuilder builder) {
    return ApplicationSpecificationAdapter.addTypeAdapters(builder)
      .registerTypeAdapter(TriggeringScheduleInfo.class, new TriggeringScheduleInfoCodec())
      .registerTypeAdapter(TriggerInfo.class, new TriggerInfoCodec())
      .registerTypeAdapter(RunId.class, new RunIds.RunIdCodec());
  }

  private static final class TriggeringScheduleInfoCodec implements JsonSerializer<TriggeringScheduleInfo>,
                                                                    JsonDeserializer<TriggeringScheduleInfo> {

    @Override
    public JsonElement serialize(TriggeringScheduleInfo src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject jsonObj = new JsonObject();
      jsonObj.addProperty("name", src.getName());
      jsonObj.addProperty("description", src.getDescription());
      jsonObj.add("triggerInfos", context.serialize(src.getTriggerInfos(), LIST_TRIGGER_INFO_TYPE));
      jsonObj.add("properties", context.serialize(src.getProperties(), Map_STRING_STRING_TYPE));

      return jsonObj;
    }

    @Override
    public TriggeringScheduleInfo deserialize(JsonElement json, Type typeOfT,
                                              JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();

      String name = jsonObj.get("name").getAsString();
      String description = jsonObj.get("description").getAsString();
      List<TriggerInfo> triggerInfos = context.deserialize(jsonObj.get("triggerInfos"), LIST_TRIGGER_INFO_TYPE);
      Map<String, String> properties = context.deserialize(jsonObj.get("properties"), Map_STRING_STRING_TYPE);

      return new DefaultTriggeringScheduleInfo(name, description, triggerInfos, properties);
    }
  }
}
