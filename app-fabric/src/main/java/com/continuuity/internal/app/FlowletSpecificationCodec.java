/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.ResourceSpecification;
import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.internal.flowlet.DefaultFlowletSpecification;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

/**
 *
 */
final class FlowletSpecificationCodec extends AbstractSpecificationCodec<FlowletSpecification> {

  @Override
  public JsonElement serialize(FlowletSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("failurePolicy", new JsonPrimitive(src.getFailurePolicy().name()));
    jsonObj.add("datasets", serializeSet(src.getDataSets(), context, String.class));
    jsonObj.add("properties", serializeMap(src.getProperties(), context, String.class));
    jsonObj.add("resources", context.serialize(src.getResources(),
                                               new TypeToken<ResourceSpecification>() { }.getType()));

    return jsonObj;
  }

  @Override
  public FlowletSpecification deserialize(JsonElement json, Type typeOfT,
                                          JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    FailurePolicy policy = FailurePolicy.valueOf(jsonObj.get("failurePolicy").getAsString());
    Set<String> dataSets = deserializeSet(jsonObj.get("datasets"), context, String.class);
    Map<String, String> properties = deserializeMap(jsonObj.get("properties"), context, String.class);
    ResourceSpecification resources = context.deserialize(jsonObj.get("resources"),
                                                          new TypeToken<ResourceSpecification>() { }.getType());

    return new DefaultFlowletSpecification(className, name, description, policy, dataSets, properties, resources);
  }
}
