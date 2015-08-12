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
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.internal.flow.DefaultFlowSpecification;
import com.google.common.collect.Maps;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class FlowSpecificationCodec extends AbstractSpecificationCodec<FlowSpecification> {

  @Override
  public JsonElement serialize(FlowSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("flowlets", serializeMap(src.getFlowlets(), context, FlowletDefinition.class));
    jsonObj.add("connections", serializeList(src.getConnections(), context, FlowletConnection.class));
    jsonObj.add("streams", serializeMap(src.getStreams(), context, StreamSpecification.class));
    jsonObj.add("datasetModules", serializeMap(src.getDatasetModules(), context, String.class));
    jsonObj.add("datasetSpecs", serializeMap(src.getDatasetSpecs(), context, DatasetCreationSpec.class));

    return jsonObj;
  }

  @Override
  public FlowSpecification deserialize(JsonElement json, Type typeOfT,
                                       JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    Map<String, FlowletDefinition> flowlets = deserializeMap(jsonObj.get("flowlets"), context, FlowletDefinition.class);
    List<FlowletConnection> connections = deserializeList(jsonObj.get("connections"), context, FlowletConnection.class);

    JsonElement streamElement = jsonObj.get("streams");
    Map<String, StreamSpecification> streams = (streamElement == null) ?
      Maps.<String, StreamSpecification>newHashMap() : deserializeMap(streamElement, context,
                                                                      StreamSpecification.class);

    JsonElement datasetModElement = jsonObj.get("datasetModules");
    Map<String, String> datasetModules = (datasetModElement == null) ?
      Maps.<String, String>newHashMap() : deserializeMap(datasetModElement, context, String.class);

    JsonElement datasetSpecsElement = jsonObj.get("datasetSpecs");
    Map<String, DatasetCreationSpec> datasetSpecs = (datasetSpecsElement == null) ?
      Maps.<String, DatasetCreationSpec>newHashMap() : deserializeMap(datasetSpecsElement, context,
                                                                      DatasetCreationSpec.class);
    return new DefaultFlowSpecification(className, name, description, flowlets, connections, streams, datasetModules,
                                        datasetSpecs);
  }
}
