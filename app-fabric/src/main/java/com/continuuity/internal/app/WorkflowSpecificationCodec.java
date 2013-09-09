/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app;

import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.internal.workflow.DefaultWorkflowSpecification;
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
import java.util.List;

/**
 *
 */
final class WorkflowSpecificationCodec implements JsonSerializer<WorkflowSpecification>,
                                                  JsonDeserializer<WorkflowSpecification> {

  @Override
  public JsonElement serialize(WorkflowSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("actions", context.serialize(src.getActions(),
                                             new TypeToken<List<WorkflowActionSpecification>>(){}.getType()));
    return jsonObj;
  }

  @Override
  public WorkflowSpecification deserialize(JsonElement json, Type typeOfT,
                                           JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    List<WorkflowActionSpecification> actions = context.deserialize(
      jsonObj.get("actions"), new TypeToken<List<WorkflowActionSpecification>>(){}.getType());

    return new DefaultWorkflowSpecification(className, name, description, actions);
  }
}
