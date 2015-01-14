/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app;

import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.Map;

/**
 *
 */
public class ScheduleSpecificationCodec extends AbstractSpecificationCodec<ScheduleSpecification> {
  @Override
  public JsonElement serialize(ScheduleSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("schedule", context.serialize(src.getSchedule(), Schedule.class));
    jsonObj.add("program", context.serialize(src.getProgram(), ScheduleProgramInfo.class));
    jsonObj.add("properties", serializeMap(src.getRuntimeArgs(), context, String.class));
    return jsonObj;
  }

  @Override
  public ScheduleSpecification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    Schedule schedule = context.deserialize(jsonObj.get("schedule"), Schedule.class);
    ScheduleProgramInfo program = context.deserialize(jsonObj.get("program"), ScheduleProgramInfo.class);
    Map<String, String> properties = deserializeMap(jsonObj.get("properties"), context, String.class);
    return new ScheduleSpecification(schedule, program, properties);
  }
}
