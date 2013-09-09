package com.continuuity.internal.app;

import com.continuuity.api.ResourceSpecification;
import com.continuuity.internal.DefaultResourceSpecification;
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
final class ResourceSpecificationCodec implements JsonSerializer<ResourceSpecification>,
  JsonDeserializer<ResourceSpecification> {

  @Override
  public JsonElement serialize(ResourceSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("cores", new JsonPrimitive(src.getCores()));
    jsonObj.add("memorySize", new JsonPrimitive(src.getMemorySize()));

    return jsonObj;
  }

  @Override
  public ResourceSpecification deserialize(JsonElement json, Type typeOfT,
                                          JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    int cores = jsonObj.get("cores").getAsInt();
    int memorySize = jsonObj.get("memorySize").getAsInt();

    return new DefaultResourceSpecification(cores, memorySize);
  }
}
