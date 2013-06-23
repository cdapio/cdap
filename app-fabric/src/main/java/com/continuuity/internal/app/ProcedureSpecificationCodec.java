/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.internal.procedure.DefaultProcedureSpecification;
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
import java.util.Set;

/**
 *
 */
final class ProcedureSpecificationCodec implements JsonSerializer<ProcedureSpecification>,
                                                     JsonDeserializer<ProcedureSpecification> {

  @Override
  public JsonElement serialize(ProcedureSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("datasets", context.serialize(src.getDataSets(), new TypeToken<Set<String>>(){}.getType()));
    jsonObj.add("arguments", context.serialize(src.getArguments(), new TypeToken<Map<String, String>>(){}.getType()));

    return jsonObj;
  }

  @Override
  public ProcedureSpecification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    Set<String> dataSets = context.deserialize(jsonObj.get("datasets"), new TypeToken<Set<String>>(){}.getType());
    Map<String, String> arguments = context.deserialize(jsonObj.get("arguments"),
                                                        new TypeToken<Map<String, String>>(){}.getType());

    return new DefaultProcedureSpecification(className, name, description, dataSets, arguments);
  }
}
