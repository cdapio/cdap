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
import co.cask.cdap.api.spark.SparkSpecification;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public final class SparkSpecificationCodec extends AbstractSpecificationCodec<SparkSpecification> {

  @Override
  public JsonElement serialize(SparkSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("mainClassName", new JsonPrimitive(src.getMainClassName()));
    jsonObj.add("properties", serializeMap(src.getProperties(), context, String.class));

    if (src.getDriverResources() != null) {
      jsonObj.add("driverResources", context.serialize(src.getDriverResources()));
    }
    if (src.getExecutorResources() != null) {
      jsonObj.add("executorResources", context.serialize(src.getExecutorResources()));
    }

    return jsonObj;
  }

  @Override
  public SparkSpecification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    String mainClassName = jsonObj.get("mainClassName").getAsString();
    Map<String, String> properties = deserializeMap(jsonObj.get("properties"), context, String.class);

    Resources driverResources = deserializeResources(jsonObj, "driver", context);
    Resources executorResources = deserializeResources(jsonObj, "executor", context);

    return new SparkSpecification(className, name, description, mainClassName,
                                  properties, driverResources, executorResources);
  }

  /**
   * Deserialize {@link Resources} object from a json property named with {@code <prefix>Resources}.
   * A {@code null} value will be returned if no such property exist.
   */
  @Nullable
  private Resources deserializeResources(JsonObject jsonObj, String prefix, JsonDeserializationContext context) {
    String name = prefix + "Resources";
    JsonElement element = jsonObj.get(name);
    return element == null ? null : (Resources) context.deserialize(element, Resources.class);
  }
}
