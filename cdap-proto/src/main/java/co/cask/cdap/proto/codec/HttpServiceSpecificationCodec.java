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

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.api.service.http.ServiceHttpEndpoint;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * GSON codec to serialize/deserialize {@link HttpServiceHandlerSpecification}.
 */
public class HttpServiceSpecificationCodec extends AbstractSpecificationCodec<HttpServiceHandlerSpecification> {
  @Override
  public HttpServiceHandlerSpecification deserialize(JsonElement json, Type typeOfT,
                                              JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    Map<String, String> properties = deserializeMap(jsonObj.get("properties"), context, String.class);
    Set<String> datasets = deserializeSet(jsonObj.get("datasets"), context, String.class);
    List<ServiceHttpEndpoint> endpointsExposed;
    if (isOldSpec(jsonObj)) {
      endpointsExposed = ImmutableList.of();
    } else {
      endpointsExposed = deserializeList(jsonObj.get("endpoints"), context, ServiceHttpEndpoint.class);
    }

    JsonElement streamElement = jsonObj.get("streams");
    Map<String, StreamSpecification> streams = (streamElement == null) ?
      Maps.<String, StreamSpecification>newHashMap() : deserializeMap(streamElement, context,
                                                                      StreamSpecification.class);
    JsonElement dataSetModElement = jsonObj.get("dataSetModules");
    Map<String, String> dataSetModules = (dataSetModElement == null) ? Maps.<String, String>newHashMap() :
      deserializeMap(dataSetModElement, context, String.class);

    JsonElement dataSetInstElement = jsonObj.get("dataSetInstances");
    Map<String, DatasetCreationSpec> dataSetInstances = (dataSetInstElement == null) ?
      Maps.<String, DatasetCreationSpec>newHashMap() : deserializeMap(dataSetInstElement, context,
                                                                      DatasetCreationSpec.class);

    return new HttpServiceHandlerSpecification(className, name, description, properties, datasets, endpointsExposed,
                                               streams, dataSetModules, dataSetInstances);
  }

  private boolean isOldSpec(JsonObject json) {
    return !json.has("endpoints"); // This field wasn't in the original spec
  }

  @Override
  public JsonElement serialize(HttpServiceHandlerSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("className", src.getClassName());
    json.addProperty("name", src.getName());
    json.addProperty("description", src.getDescription());
    json.add("properties", serializeMap(src.getProperties(), context, String.class));
    json.add("datasets", serializeSet(src.getDatasets(), context, String.class));
    json.add("endpoints", serializeList(src.getEndpoints(), context, ServiceHttpEndpoint.class));
    json.add("streams", serializeMap(src.getStreams(), context, StreamSpecification.class));
    json.add("dataSetModules", serializeMap(src.getDataSetModules(), context, String.class));
    json.add("dataSetInstances", serializeMap(src.getDataSetInstances(), context, DatasetCreationSpec.class));
    return json;
  }
}
