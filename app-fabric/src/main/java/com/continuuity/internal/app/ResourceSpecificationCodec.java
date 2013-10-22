package com.continuuity.internal.app;

import com.continuuity.api.ResourceSpecification;
import com.continuuity.api.Resources;
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
final class ResourceSpecificationCodec extends AbstractSpecificationCodec<ResourceSpecification> {

  @Override
  public JsonElement serialize(ResourceSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("virtualCores", new JsonPrimitive(src.getVirtualCores()));
    jsonObj.add("memoryMB", new JsonPrimitive(src.getMemoryMB()));

    return jsonObj;
  }

  @Override
  public ResourceSpecification deserialize(JsonElement json, Type typeOfT,
                                          JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    int cores = jsonObj.get("virtualCores").getAsInt();
    int memorySize = jsonObj.get("memoryMB").getAsInt();

    return new Resources(memorySize, cores);
  }
}
