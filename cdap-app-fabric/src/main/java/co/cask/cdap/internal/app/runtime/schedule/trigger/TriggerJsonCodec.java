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

import co.cask.cdap.internal.schedule.trigger.Trigger;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Serialization and deserialization of Triggers as Json.
 *
 * All triggers must be serialized by passing in the {@link Trigger} class as the type of the object.
 * Without that, the "className" field will not be generated, and we use that during deserialization
 * to determine the actual subclass, and then deserialize based on that.
 *
 * Note that when serializing an object that contains a Trigger, Gson calls serialize with Trigger as
 * the class of the object, and this is exactly the behavior that is needed.
 */
public class TriggerJsonCodec implements JsonSerializer<Trigger>, JsonDeserializer<Trigger> {

  @Override
  public JsonElement serialize(Trigger src, Type typeOfSrc, JsonSerializationContext context) {
    // this assumes that Trigger is an abstract class (every instance will have a concrete type that's not Trigger)
    JsonObject object = (JsonObject) context.serialize(src, src.getClass());
    object.add("className", new JsonPrimitive(src.getClass().getName()));
    return object;
  }

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
}
