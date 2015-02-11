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
import co.cask.cdap.api.schedule.StreamSizeSchedule;
import co.cask.cdap.api.schedule.TimeSchedule;
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

  /**
   * Schedule Type.
   */
  private enum ScheduleType {
    ORIGINAL_TIME,

    /**
     * Represents {@link TimeSchedule} objects.
     */
    TIME,

    /**
     * Represents {@link StreamSizeSchedule} objects.
     */
    STREAM_DATA;

    private static ScheduleType fromSchedule(Schedule schedule) {
      if (schedule instanceof StreamSizeSchedule) {
        return STREAM_DATA;
      } else if (schedule instanceof TimeSchedule) {
        return TIME;
      } else if (schedule.isTimeSchedule()) {
        return ORIGINAL_TIME;
      }
      return null;
    }
  }

  @Override
  public JsonElement serialize(ScheduleSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    ScheduleType scheduleType = ScheduleType.fromSchedule(src.getSchedule());
    jsonObj.add("scheduleType", context.serialize(scheduleType, ScheduleType.class));
    if (scheduleType.equals(ScheduleType.ORIGINAL_TIME)) {
      jsonObj.add("schedule", context.serialize(src.getSchedule(), Schedule.class));
    } else if (scheduleType.equals(ScheduleType.TIME)) {
      jsonObj.add("schedule", context.serialize(src.getSchedule(), TimeSchedule.class));
    } else if (scheduleType.equals(ScheduleType.STREAM_DATA)) {
      jsonObj.add("schedule", context.serialize(src.getSchedule(), StreamSizeSchedule.class));
    }

    jsonObj.add("program", context.serialize(src.getProgram(), ScheduleProgramInfo.class));
    jsonObj.add("properties", serializeMap(src.getProperties(), context, String.class));
    return jsonObj;
  }

  @Override
  public ScheduleSpecification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    JsonElement scheduleTypeJson = jsonObj.get("scheduleType");
    ScheduleType scheduleType;
    if (scheduleTypeJson == null) {
      // For backwards compatibility with spec persisted with older versions than 2.8, we need these lines
      scheduleType = ScheduleType.ORIGINAL_TIME;
    } else {
      scheduleType = context.deserialize(jsonObj.get("scheduleType"), ScheduleType.class);
    }

    Schedule schedule = null;
    switch (scheduleType) {
      case ORIGINAL_TIME:
        schedule = context.deserialize(jsonObj.get("schedule"), Schedule.class);
        break;
      case TIME:
        schedule = context.deserialize(jsonObj.get("schedule"), TimeSchedule.class);
        break;
      case STREAM_DATA:
        schedule = context.deserialize(jsonObj.get("schedule"), StreamSizeSchedule.class);
        break;
    }

    ScheduleProgramInfo program = context.deserialize(jsonObj.get("program"), ScheduleProgramInfo.class);
    Map<String, String> properties = deserializeMap(jsonObj.get("properties"), context, String.class);
    return new ScheduleSpecification(schedule, program, properties);
  }
}
