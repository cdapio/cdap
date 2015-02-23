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
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.api.service.http.ServiceHttpEndpoint;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.internal.json.TwillSpecificationAdapter;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Codec to serialize and deserialize {@link ServiceSpecification}
 */
public class ServiceSpecificationCodec extends AbstractSpecificationCodec<ServiceSpecification>  {

  // For decoding old spec. Remove later.
  private static final Gson GSON = new Gson();

  private final TwillSpecificationAdapter twillSpecificationAdapter;

  public ServiceSpecificationCodec() {
    twillSpecificationAdapter = TwillSpecificationAdapter.create();
  }

  @Override
  public ServiceSpecification deserialize(JsonElement json, Type typeOfT,
                                          JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = (JsonObject) json;

    if (isOldSpec(jsonObj)) {
      return decodeOldSpec(jsonObj);
    }

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    Map<String, HttpServiceHandlerSpecification> handlers = deserializeMap(jsonObj.get("handlers"), context,
                                                                    HttpServiceHandlerSpecification.class);
    Map<String, ServiceWorkerSpecification> workers = deserializeMap(jsonObj.get("workers"), context,
                                                                     ServiceWorkerSpecification.class);
    Resources resources = context.deserialize(jsonObj.get("resources"), Resources.class);
    int instances = jsonObj.get("instances").getAsInt();

    return new ServiceSpecification(className, name, description, handlers, workers, resources, instances);
  }

  @Override
  public JsonElement serialize(ServiceSpecification spec, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject object = new JsonObject();
    object.addProperty("className", spec.getClassName());
    object.addProperty("name", spec.getName());
    object.addProperty("description", spec.getDescription());
    object.add("handlers", serializeMap(spec.getHandlers(), context, HttpServiceHandlerSpecification.class));
    object.add("workers", serializeMap(spec.getWorkers(), context, ServiceWorkerSpecification.class));
    object.add("resources", context.serialize(spec.getResources(), Resources.class));
    object.addProperty("instances", spec.getInstances());
    return object;
  }

  private boolean isOldSpec(JsonObject json) {
    return json.has("classname");   // In old spec, it's misspelled as classname, not className.
  }

  private ServiceSpecification decodeOldSpec(JsonObject json) {
    String className = json.get("classname").getAsString();
    TwillSpecification twillSpec = twillSpecificationAdapter.fromJson(json.get("spec").getAsString());
    Map<String, HttpServiceHandlerSpecification> handlers = Maps.newHashMap();
    Map<String, ServiceWorkerSpecification> workers = Maps.newHashMap();

    RuntimeSpecification handlerSpec = twillSpec.getRunnables().get(twillSpec.getName());
    Map<String, String> configs = handlerSpec.getRunnableSpecification().getConfigs();

    // Get the class names of all handlers. It is stored in the handler runnable spec configs
    List<String> handlerClasses = GSON.fromJson(configs.get("service.runnable.handlers"),
                                                new TypeToken<List<String>>() { }.getType());
    List<JsonObject> handlerSpecs = GSON.fromJson(configs.get("service.runnable.handler.spec"),
                                                  new TypeToken<List<JsonObject>>() { }.getType());

    for (int i = 0; i < handlerClasses.size(); i++) {
      String handlerClass = handlerClasses.get(i);
      JsonObject spec = handlerSpecs.get(i);
      Map<String, String> properties = GSON.fromJson(spec.get("properties"),
                                                     new TypeToken<Map<String, String>>() { }.getType());

      // Reconstruct the HttpServiceSpecification. However there is no way to determine the datasets or endpoints
      // as it is not recorded in old spec. It's ok since the spec is only used to load data from MDS during redeploy.
      handlers.put(spec.get("name").getAsString(),
                   new HttpServiceHandlerSpecification(handlerClass,
                                                       spec.get("name").getAsString(),
                                                       spec.get("description").getAsString(),
                                                       properties, ImmutableSet.<String>of(),
                                                       ImmutableList.<ServiceHttpEndpoint>of()));
    }

    // Generates worker specs.
    for (Map.Entry<String, RuntimeSpecification> entry : twillSpec.getRunnables().entrySet()) {
      TwillRunnableSpecification runnableSpec = entry.getValue().getRunnableSpecification();
      Set<String> datasets = GSON.fromJson(runnableSpec.getConfigs().get("service.datasets"),
                                           new TypeToken<Set<String>>() { }.getType());
      ResourceSpecification resourceSpec = entry.getValue().getResourceSpecification();
      ServiceWorkerSpecification workerSpec = new ServiceWorkerSpecification(
        runnableSpec.getConfigs().get("service.class.name"),
        runnableSpec.getName(), runnableSpec.getName(),
        runnableSpec.getConfigs(), datasets,
        new Resources(resourceSpec.getMemorySize(), resourceSpec.getVirtualCores()),
        resourceSpec.getInstances());
      workers.put(entry.getKey(), workerSpec);
    }

    ResourceSpecification resourceSpec = handlerSpec.getResourceSpecification();
    return new ServiceSpecification(className, twillSpec.getName(), twillSpec.getName(), handlers, workers,
                                    new Resources(resourceSpec.getMemorySize(), resourceSpec.getVirtualCores()),
                                    resourceSpec.getInstances());
  }
}
