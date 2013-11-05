package com.continuuity.internal.app;

import com.continuuity.api.schedule.Schedule;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;

/**
 *
 */
public class ScheduleCodec extends AbstractSpecificationCodec<Schedule> {
  @Override
  public JsonElement serialize(Schedule src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("cronExpression", new JsonPrimitive(src.getCronEntry()));
    jsonObj.add("action", new JsonPrimitive(src.getAction().toString()));
    return jsonObj;
  }

  @Override
  public Schedule deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    String cronExpression = jsonObj.get("cronExpression").getAsString();
    String action = jsonObj.get("action").getAsString();

    return new Schedule(name, description, cronExpression, Schedule.Action.valueOf(action));
  }
}
