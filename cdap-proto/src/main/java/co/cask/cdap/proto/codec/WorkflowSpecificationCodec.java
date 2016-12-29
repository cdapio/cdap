/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.retry.RetryPolicy;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
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
public final class WorkflowSpecificationCodec extends AbstractSpecificationCodec<WorkflowSpecification> {

  @Override
  public JsonElement serialize(WorkflowSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("properties", serializeMap(src.getProperties(), context, String.class));
    jsonObj.add("nodes", serializeList(src.getNodes(), context, WorkflowNode.class));
    jsonObj.add("localDatasetSpecs", serializeMap(src.getLocalDatasetSpecs(), context, DatasetCreationSpec.class));
    jsonObj.add("remoteRetryPolicy", context.serialize(src.getRemoteRetryPolicy()));
    return jsonObj;
  }

  @Override
  public WorkflowSpecification deserialize(JsonElement json, Type typeOfT,
                                           JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    Map<String, String> properties = deserializeMap(jsonObj.get("properties"), context, String.class);
    List<WorkflowNode> nodes = deserializeList(jsonObj.get("nodes"), context, WorkflowNode.class);
    Map<String, DatasetCreationSpec> localDatasetSpec = deserializeMap(jsonObj.get("localDatasetSpecs"), context,
                                                                       DatasetCreationSpec.class);
    RetryPolicy remoteRetryPolicy = context.deserialize(jsonObj.get("remoteRetryPolicy"), RetryPolicy.class);

    return new WorkflowSpecification(className, name, description, properties,
                                     nodes, localDatasetSpec, remoteRetryPolicy);
  }
}
