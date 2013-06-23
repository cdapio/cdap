/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.internal.DefaultApplicationSpecification;
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

/**
 *
 */
final class ApplicationSpecificationCodec implements JsonSerializer<ApplicationSpecification>,
  JsonDeserializer<ApplicationSpecification> {

  @Override
  public JsonElement serialize(ApplicationSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("streams", context.serialize(src.getStreams(),
                                             new TypeToken<Map<String, StreamSpecification>>(){}.getType()));
    jsonObj.add("datasets", context.serialize(src.getDataSets(),
                                              new TypeToken<Map<String, DataSetSpecification>>(){}.getType()));
    jsonObj.add("flows", context.serialize(src.getFlows(),
                                           new TypeToken<Map<String, FlowSpecification>>(){}.getType()));
    jsonObj.add("procedures", context.serialize(src.getProcedures(),
                                                new TypeToken<Map<String, ProcedureSpecification>>(){}.getType()));
    jsonObj.add("mapReduces", context.serialize(src.getMapReduces(),
                                                new TypeToken<Map<String, MapReduceSpecification>>(){}.getType()));
    return jsonObj;
  }

  @Override
  public ApplicationSpecification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    Map<String, StreamSpecification> streams = context.deserialize(
          jsonObj.get("streams"), new TypeToken<Map<String, StreamSpecification>>(){}.getType());
    Map<String, DataSetSpecification> datasets = context.deserialize(
          jsonObj.get("datasets"), new TypeToken<Map<String, DataSetSpecification>>(){}.getType());
    Map<String, FlowSpecification> flows = context.deserialize(
          jsonObj.get("flows"), new TypeToken<Map<String, FlowSpecification>>(){}.getType());
    Map<String, ProcedureSpecification> procedures = context.deserialize(
          jsonObj.get("procedures"), new TypeToken<Map<String, ProcedureSpecification>>(){}.getType());
    Map<String, MapReduceSpecification> mapReduces = context.deserialize(
          jsonObj.get("mapReduces"), new TypeToken<Map<String, MapReduceSpecification>>(){}.getType());

    return new DefaultApplicationSpecification(name, description, streams, datasets, flows, procedures, mapReduces);
  }
}
