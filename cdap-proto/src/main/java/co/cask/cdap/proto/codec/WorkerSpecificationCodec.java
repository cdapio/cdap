/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.worker.WorkerSpecification;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

/**
 * Codec to serialize and deserialize {@link WorkerSpecification}
 */
public final class WorkerSpecificationCodec extends AbstractSpecificationCodec<WorkerSpecification> {

  @Override
  public JsonElement serialize(WorkerSpecification spec, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty("className", spec.getClassName());
    jsonObj.addProperty("name", spec.getName());
    jsonObj.addProperty("description", spec.getDescription());
    jsonObj.add("plugins", serializeMap(spec.getPlugins(), context, Plugin.class));
    jsonObj.add("properties", serializeMap(spec.getProperties(), context, String.class));
    jsonObj.add("resources", context.serialize(spec.getResources(), Resources.class));
    jsonObj.add("datasets", serializeSet(spec.getDatasets(), context, String.class));
    jsonObj.addProperty("instances", spec.getInstances());
    return jsonObj;
  }

  @Override
  public WorkerSpecification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = (JsonObject) json;

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    Map<String, Plugin> plugins = deserializeMap(jsonObj.get("plugins"), context, Plugin.class);
    Map<String, String> properties = deserializeMap(jsonObj.get("properties"), context, String.class);
    Resources resources = context.deserialize(jsonObj.get("resources"), Resources.class);
    Set<String> datasets = deserializeSet(jsonObj.get("datasets"), context, String.class);
    int instances = jsonObj.get("instances").getAsInt();

    return new WorkerSpecification(className, name, description, properties, datasets, resources, instances, plugins);
  }
}
