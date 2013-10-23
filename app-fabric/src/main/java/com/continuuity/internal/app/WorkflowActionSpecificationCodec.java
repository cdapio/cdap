/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app;

import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.internal.workflow.DefaultWorkflowActionSpecification;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.Map;

/**
 *
 */
public final class WorkflowActionSpecificationCodec extends AbstractSpecificationCodec<WorkflowActionSpecification> {

  @Override
  public JsonElement serialize(WorkflowActionSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("properties", serializeMap(src.getProperties(), context, String.class));

    return jsonObj;
  }

  @Override
  public WorkflowActionSpecification deserialize(JsonElement json, Type typeOfT,
                                                 JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    Map<String, String> properties = deserializeMap(jsonObj.get("properties"), context, String.class);

    return new DefaultWorkflowActionSpecification(className, name, description, properties);
  }
}
