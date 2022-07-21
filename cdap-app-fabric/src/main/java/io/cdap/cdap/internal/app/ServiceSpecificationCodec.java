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

package io.cdap.cdap.internal.app;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.api.service.http.HttpServiceHandlerSpecification;
import io.cdap.cdap.proto.codec.AbstractSpecificationCodec;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.internal.json.TwillRuntimeSpecificationAdapter;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Codec to serialize and deserialize {@link ServiceSpecification}
 *
 * TODO: Move to cdap-proto
 */
public class ServiceSpecificationCodec extends AbstractSpecificationCodec<ServiceSpecification> {

  // For decoding old spec. Remove later.
  private static final Gson GSON = new Gson();

  private final TwillRuntimeSpecificationAdapter twillSpecificationAdapter;

  public ServiceSpecificationCodec() {
    twillSpecificationAdapter = TwillRuntimeSpecificationAdapter.create();
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
    Map<String, Plugin> plugins = deserializeMap(jsonObj.get("plugins"), context, Plugin.class);
    Map<String, HttpServiceHandlerSpecification> handlers = deserializeMap(jsonObj.get("handlers"), context,
                                                                    HttpServiceHandlerSpecification.class);
    Resources resources = context.deserialize(jsonObj.get("resources"), Resources.class);
    int instances = jsonObj.get("instances").getAsInt();
    Map<String, String> properties = deserializeMap(jsonObj.get("properties"), context, String.class);

    return new ServiceSpecification(className, name, description, handlers, resources, instances, plugins, properties);
  }

  @Override
  public JsonElement serialize(ServiceSpecification spec, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject object = new JsonObject();
    object.addProperty("className", spec.getClassName());
    object.addProperty("name", spec.getName());
    object.addProperty("description", spec.getDescription());
    object.add("plugins", serializeMap(spec.getPlugins(), context, Plugin.class));
    object.add("handlers", serializeMap(spec.getHandlers(), context, HttpServiceHandlerSpecification.class));
    object.add("resources", context.serialize(spec.getResources(), Resources.class));
    object.addProperty("instances", spec.getInstances());
    object.add("properties", serializeMap(spec.getProperties(), context, String.class));
    return object;
  }

  private boolean isOldSpec(JsonObject json) {
    return json.has("classname");   // In old spec, it's misspelled as classname, not className.
  }

  private ServiceSpecification decodeOldSpec(JsonObject json) {
    String className = json.get("classname").getAsString();
    TwillSpecification twillSpec =
      twillSpecificationAdapter.fromJson(json.get("spec").getAsString()).getTwillSpecification();
    Map<String, HttpServiceHandlerSpecification> handlers = Maps.newHashMap();

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
                                                       properties, Collections.emptySet(),
                                                       Collections.emptyList()));
    }

    ResourceSpecification resourceSpec = handlerSpec.getResourceSpecification();
    return new ServiceSpecification(className, twillSpec.getName(), twillSpec.getName(), handlers,
                                    new Resources(resourceSpec.getMemorySize(), resourceSpec.getVirtualCores()),
                                    resourceSpec.getInstances(), Collections.emptyMap());
  }
}
