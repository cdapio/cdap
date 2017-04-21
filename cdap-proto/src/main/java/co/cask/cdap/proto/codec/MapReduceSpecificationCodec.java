/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.proto.codec;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.retry.RetryPolicy;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class MapReduceSpecificationCodec extends AbstractSpecificationCodec<MapReduceSpecification> {

  @Override
  public JsonElement serialize(MapReduceSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.addProperty("className", src.getClassName());
    jsonObj.addProperty("name", src.getName());
    jsonObj.addProperty("description", src.getDescription());

    if (src.getDriverResources() != null) {
      jsonObj.add("driverResources", context.serialize(src.getDriverResources()));
    }
    if (src.getMapperResources() != null) {
      jsonObj.add("mapperResources", context.serialize(src.getMapperResources()));
    }
    if (src.getReducerResources() != null) {
      jsonObj.add("reducerResources", context.serialize(src.getReducerResources()));
    }
    if (src.getInputDataSet() != null) {
      jsonObj.addProperty("inputDataSet", src.getInputDataSet());
    }
    if (src.getOutputDataSet() != null) {
      jsonObj.addProperty("outputDataSet", src.getOutputDataSet());
    }
    jsonObj.add("datasets", serializeSet(src.getDataSets(), context, String.class));
    jsonObj.add("properties", serializeMap(src.getProperties(), context, String.class));
    jsonObj.add("remoteRetryPolicy", context.serialize(src.getRemoteRetryPolicy()));

    return jsonObj;
  }

  @Override
  public MapReduceSpecification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    Resources driverResources = deserializeResources(jsonObj, "driver", context);
    Resources mapperResources = deserializeResources(jsonObj, "mapper", context);
    Resources reducerResources = deserializeResources(jsonObj, "reducer", context);
    JsonElement inputDataSetElem = jsonObj.get("inputDataSet");
    String inputDataSet = inputDataSetElem == null ? null : inputDataSetElem.getAsString();
    JsonElement outputDataSetElem = jsonObj.get("outputDataSet");
    String outputDataSet = outputDataSetElem == null ? null : outputDataSetElem.getAsString();

    Set<String> dataSets = deserializeSet(jsonObj.get("datasets"), context, String.class);
    Map<String, String> properties = deserializeMap(jsonObj.get("properties"), context, String.class);
    RetryPolicy remoteRetryPolicy = context.deserialize(jsonObj.get("remoteRetryPolicy"), RetryPolicy.class);
    return new MapReduceSpecification(className, name, description, inputDataSet, outputDataSet,
                                      dataSets, properties, driverResources, mapperResources, reducerResources,
                                      remoteRetryPolicy);
  }

  /**
   * Deserialize the resources field from the serialized object.
   *
   * @param jsonObj The object representing the MapReduceSpecification
   * @param prefix Field name prefix. Either "mapper" or "reducer"
   * @param context The context to deserialize object.
   * @return A {@link Resources} or {@code null}.
   */
  private Resources deserializeResources(JsonObject jsonObj, String prefix, JsonDeserializationContext context) {
    // See if it of new format
    String name = prefix + "Resources";
    JsonElement element = jsonObj.get(name);
    if (element != null) {
      return context.deserialize(element, Resources.class);
    }

    // Try the old format, which is an int field representing the memory in MB.
    name = prefix + "MemoryMB";
    element = jsonObj.get(name);
    if (element != null) {
      return new Resources(element.getAsInt());
    }
    return null;
  }
}
