/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.internal.batch.DefaultMapReduceSpecification;
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
final class MapReduceSpecificationCodec implements JsonSerializer<MapReduceSpecification>,
                                                     JsonDeserializer<MapReduceSpecification> {

  @Override
  public JsonElement serialize(MapReduceSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    if (src.getInputDataSet() != null) {
      jsonObj.add("inputDataSet", new JsonPrimitive(src.getInputDataSet()));
    }
    if (src.getOutputDataSet() != null) {
      jsonObj.add("outputDataSet", new JsonPrimitive(src.getOutputDataSet()));
    }
    jsonObj.add("datasets", context.serialize(src.getDataSets(), new TypeToken<Set<String>>(){}.getType()));
    jsonObj.add("arguments", context.serialize(src.getArguments(), new TypeToken<Map<String, String>>(){}.getType()));

    return jsonObj;
  }

  @Override
  public MapReduceSpecification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    JsonElement inputDataSetElem = jsonObj.get("inputDataSet");
    String inputDataSet = inputDataSetElem == null ? null : inputDataSetElem.getAsString();
    JsonElement outputDataSetElem = jsonObj.get("outputDataSet");
    String outputDataSet = outputDataSetElem == null ? null : outputDataSetElem.getAsString();
    Set<String> dataSets = context.deserialize(jsonObj.get("datasets"), new TypeToken<Set<String>>(){}.getType());
    Map<String, String> arguments = context.deserialize(jsonObj.get("arguments"),
                                                        new TypeToken<Map<String, String>>(){}.getType());

    return new DefaultMapReduceSpecification(className, name, description,
                                                inputDataSet, outputDataSet,
                                                dataSets, arguments);
  }
}
