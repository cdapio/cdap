/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.internal.flow.DefaultFlowSpecification;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 *
 */
final class FlowSpecificationCodec extends AbstractSpecificationCodec<FlowSpecification> {

  @Override
  public JsonElement serialize(FlowSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("flowlets", serializeMap(src.getFlowlets(), context, FlowletDefinition.class));
    jsonObj.add("connections", serializeList(src.getConnections(), context, FlowletConnection.class));

    return jsonObj;
  }

  @Override
  public FlowSpecification deserialize(JsonElement json, Type typeOfT,
                                       JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    Map<String, FlowletDefinition> flowlets = deserializeMap(jsonObj.get("flowlets"), context, FlowletDefinition.class);
    List<FlowletConnection> connections = deserializeList(jsonObj.get("connections"), context, FlowletConnection.class);

    return new DefaultFlowSpecification(className, name, description, flowlets, connections);
  }
}
