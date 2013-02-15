package com.continuuity.internal.app;

import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.internal.api.flowlet.DefaultFlowletSpecification;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 *
 */
final class FlowletSpecificationCodec implements JsonSerializer<FlowletSpecification>, JsonDeserializer<FlowletSpecification> {

  @Override
  public JsonElement serialize(FlowletSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("failurePolicy", new JsonPrimitive(src.getFailurePolicy().name()));

    return jsonObj;
  }

  @Override
  public FlowletSpecification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    FailurePolicy policy = FailurePolicy.valueOf(jsonObj.get("failurePolicy").getAsString());

    return new DefaultFlowletSpecification(className, name, description, policy);
  }
}
