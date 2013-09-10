/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app;

import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.internal.workflow.DefaultWorkflowActionSpecification;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.Map;

/**
 *
 */
final class WorkflowActionSpecificationCodec implements JsonSerializer<WorkflowActionSpecification>,
                                                        JsonDeserializer<WorkflowActionSpecification> {

  @Override
  public JsonElement serialize(WorkflowActionSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("options", context.serialize(src.getOptions(), new TypeToken<Map<String, String>>(){}.getType()));

    return jsonObj;
  }

  @Override
  public WorkflowActionSpecification deserialize(JsonElement json, Type typeOfT,
                                                 JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    Map<String, String> options = context.deserialize(jsonObj.get("options"),
                                                        new TypeToken<Map<String, String>>(){}.getType());


    return new DefaultWorkflowActionSpecification(className, name, description, options);
  }
}
