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

package co.cask.cdap.internal.app.runtime.batch;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * TODO: javadocs
 * @param <T>
 */
public class OutputCodec<T> implements JsonSerializer<T>, JsonDeserializer<T> {

  @Override
  public JsonElement serialize(T src, Type typeOfSrc, JsonSerializationContext context) {
    // this assumes that ProtoConstraint is an abstract class (every instance will have a concrete subclass type)
    JsonObject jsonObject = (JsonObject) context.serialize(src, src.getClass());
    jsonObject.addProperty("className", src.getClass().getName());
    return jsonObject;
  }

  @Override
  public T deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    if (json == null) {
      return null;
    }
    if (!(json instanceof JsonObject)) {
      throw new JsonParseException("Expected a JsonObject but found a " + json.getClass().getName());
    }
    JsonObject object = (JsonObject) json;
    JsonElement className = object.get("className");
    Class<? extends T> subClass = forName(className.getAsString());
    if (subClass == null) {
      throw new JsonParseException("Unable to map constraint type to a run constraint class");
    }
    return context.deserialize(json, subClass);
  }

  private Class<? extends T> forName(String className) {
    try {
      return (Class<? extends T>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
