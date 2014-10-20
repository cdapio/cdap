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

package co.cask.cdap.internal.app;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.internal.app.services.DefaultServiceWorkerSpecification;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

/**
 * The GSON codec for {@link ServiceWorkerSpecification}.
 */
public class ServiceWorkerSpecificationCodec extends AbstractSpecificationCodec<ServiceWorkerSpecification> {

  @Override
  public ServiceWorkerSpecification deserialize(JsonElement json, Type typeOfT,
                                                JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    Map<String, String> properties = deserializeMap(jsonObj.get("properties"), context, String.class);
    Set<String> datasets = deserializeSet(jsonObj.get("datasets"), context, String.class);
    Resources resources = context.deserialize(jsonObj.get("resources"), Resources.class);
    JsonElement instanceElem = jsonObj.get("instances");
    int instances = (instanceElem == null || instanceElem.isJsonNull()) ? 1 : jsonObj.get("instances").getAsInt();

    return new DefaultServiceWorkerSpecification(className, name, description,
                                                 properties, datasets, resources, instances);
  }

  @Override
  public JsonElement serialize(ServiceWorkerSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("className", src.getClassName());
    json.addProperty("name", src.getName());
    json.addProperty("description", src.getDescription());
    json.add("properties", serializeMap(src.getProperties(), context, String.class));
    json.add("datasets", serializeSet(src.getDatasets(), context, String.class));
    json.add("resources", context.serialize(src.getResources(), Resources.class));
    json.addProperty("instances", src.getInstances());

    return json;
  }
}
