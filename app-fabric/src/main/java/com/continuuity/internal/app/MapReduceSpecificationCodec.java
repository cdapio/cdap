/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.internal.batch.DefaultMapReduceSpecification;
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
final class MapReduceSpecificationCodec extends AbstractSpecificationCodec<MapReduceSpecification> {

  @Override
  public JsonElement serialize(MapReduceSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("mapperMemoryMB", new JsonPrimitive(src.getMapperMemoryMB()));
    jsonObj.add("reducerMemoryMB", new JsonPrimitive(src.getReducerMemoryMB()));
    if (src.getInputDataSet() != null) {
      jsonObj.add("inputDataSet", new JsonPrimitive(src.getInputDataSet()));
    }
    if (src.getOutputDataSet() != null) {
      jsonObj.add("outputDataSet", new JsonPrimitive(src.getOutputDataSet()));
    }
    jsonObj.add("datasets", serializeSet(src.getDataSets(), context, String.class));
    jsonObj.add("properties", serializeMap(src.getProperties(), context, String.class));

    return jsonObj;
  }

  @Override
  public MapReduceSpecification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    int mapperMemoryMB = jsonObj.get("mapperMemoryMB").getAsInt();
    int reducerMemoryMB = jsonObj.get("reducerMemoryMB").getAsInt();
    JsonElement inputDataSetElem = jsonObj.get("inputDataSet");
    String inputDataSet = inputDataSetElem == null ? null : inputDataSetElem.getAsString();
    JsonElement outputDataSetElem = jsonObj.get("outputDataSet");
    String outputDataSet = outputDataSetElem == null ? null : outputDataSetElem.getAsString();

    Set<String> dataSets = deserializeSet(jsonObj.get("datasets"), context, String.class);
    Map<String, String> properties = deserializeMap(jsonObj.get("properties"), context, String.class);

    return new DefaultMapReduceSpecification(className, name, description, inputDataSet, outputDataSet,
                                             dataSets, properties, mapperMemoryMB, reducerMemoryMB);
  }
}
