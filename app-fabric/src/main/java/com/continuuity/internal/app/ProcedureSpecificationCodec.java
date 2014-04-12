/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.ResourceSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.internal.procedure.DefaultProcedureSpecification;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

/**
 *
 */
final class ProcedureSpecificationCodec extends AbstractSpecificationCodec<ProcedureSpecification> {

  @Override
  public JsonElement serialize(ProcedureSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("datasets", serializeSet(src.getDataSets(), context, String.class));
    jsonObj.add("properties", serializeMap(src.getProperties(), context, String.class));
    jsonObj.add("resources", context.serialize(src.getResources(),
                                               new TypeToken<ResourceSpecification>() { }.getType()));
    jsonObj.addProperty("instances", src.getInstances());
    return jsonObj;
  }

  @Override
  public ProcedureSpecification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    Set<String> dataSets = deserializeSet(jsonObj.get("datasets"), context, String.class);
    Map<String, String> properties = deserializeMap(jsonObj.get("properties"), context, String.class);
    ResourceSpecification resourceSpec = context.deserialize(jsonObj.get("resources"),
                                                             new TypeToken<ResourceSpecification>() { }.getType());

    JsonElement instanceElem = jsonObj.get("instances");
    int instances = (instanceElem == null || instanceElem.isJsonNull()) ? 1 : jsonObj.get("instances").getAsInt();
    return new DefaultProcedureSpecification(className, name, description, dataSets,
                                             properties, resourceSpec, instances);
  }
}
