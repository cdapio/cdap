/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.trigger;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;

import java.lang.reflect.Type;

/**
 * Serialization and deserializtion of Triggers as Json.
 *
 * All triggers inherit the {@link Trigger#className} field from the base class. Here we use that
 * during deserialization to determine the actual subclass, and then deserialize based on that.
 */
public class TriggerJsonDeserializer implements JsonDeserializer<Trigger> {
  @Override
  public Trigger deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    if (json == null) {
      return null;
    }
    if (!(json instanceof JsonObject)) {
      throw new JsonParseException("Expected a JsonObject but found a " + json.getClass().getName());
    }
    JsonObject object = (JsonObject) json;
    JsonPrimitive prim = object.getAsJsonPrimitive("className");
    try {
      Class<?> triggerClass = Class.forName(prim.getAsString());
      return context.deserialize(json, triggerClass);
    } catch (ClassNotFoundException e) {
      throw new JsonParseException("Unable to load class " + prim.getAsString(), e);
    }
  }

  // this would serialize into { "class" : "<className>", "value": { ... <actual object> ... } }
  // however, it does not seem to work, Gson goes into infinite loop with this approach.
  /*
  @Override
  public Trigger deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    if (json == null) {
      return null;
    }
    if (!(json instanceof JsonObject)) {
      throw new JsonParseException("Expected a JsonObject but found a " + json.getClass().getName());
    }
    JsonObject object = (JsonObject) json;
    JsonPrimitive prim = object.getAsJsonPrimitive("class");
    String className = prim.getAsString();
    JsonElement elem = object.get("value");
    try {
      return context.deserialize(elem, Class.forName(className));
    } catch (ClassNotFoundException e) {
      throw new JsonParseException("Unable to load class " + className, e);
    }
  }

  @Override
  public JsonElement serialize(Trigger trigger, Type typeOfSrc, JsonSerializationContext context) {
    if (trigger == null) {
      return null;
    }
    JsonObject result = new JsonObject();
    result.add("class", new JsonPrimitive(trigger.getClass().getName()));
    result.add("value", context.serialize(trigger));
    return result;
  }
  */
}
