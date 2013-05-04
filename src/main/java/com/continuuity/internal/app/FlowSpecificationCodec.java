/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.internal.flow.DefaultFlowSpecification;
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
import java.util.Map;

/**
 *
 */
final class FlowSpecificationCodec implements JsonSerializer<FlowSpecification>, JsonDeserializer<FlowSpecification> {

  @Override
  public JsonElement serialize(FlowSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("flowlets", context.serialize(src.getFlowlets(),
                                              new TypeToken<Map<String, FlowletDefinition>>() {}.getType()));
    jsonObj.add("connections", context.serialize(src.getConnections(),
                                                 new TypeToken<List<FlowletConnection>>() {}.getType()));

    return jsonObj;
  }

  @Override
  public FlowSpecification deserialize(JsonElement json, Type typeOfT,
                                       JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    Map<String, FlowletDefinition> flowlets = context.deserialize(
          jsonObj.get("flowlets"), new TypeToken<Map<String, FlowletDefinition>>() {}.getType());
    List<FlowletConnection> connections = context.deserialize(
          jsonObj.get("connections"), new TypeToken<List<FlowletConnection>>() {}.getType());

    return new DefaultFlowSpecification(className, name, description, flowlets, connections);
  }
}
