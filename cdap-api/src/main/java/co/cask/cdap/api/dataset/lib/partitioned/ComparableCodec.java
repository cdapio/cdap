/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib.partitioned;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;

import javax.annotation.Nullable;

/**
 * Responsible for serialization and deserialization of {@link Comparable}.
 */
public class ComparableCodec {

  @Nullable
  protected JsonElement serializeComparable(@Nullable Comparable comparable,
                                            JsonSerializationContext jsonSerializationContext) {
    if (comparable == null) {
      return null;
    }
    JsonArray jsonArray = new JsonArray();
    jsonArray.add(jsonSerializationContext.serialize(comparable.getClass().getName()));
    jsonArray.add(jsonSerializationContext.serialize(comparable));
    return jsonArray;
  }

  @Nullable
  protected Comparable deserializeComparable(@Nullable JsonElement comparableJson,
                                             JsonDeserializationContext jsonDeserializationContext) {
    if (comparableJson == null) {
      return null;
    }
    JsonArray jsonArray = comparableJson.getAsJsonArray();
    // the classname is serialized as the first element, the value is serialized as the second
    Class<? extends Comparable> comparableClass = forName(jsonArray.get(0).getAsString());
    return jsonDeserializationContext.deserialize(jsonArray.get(1), comparableClass);
  }

  @SuppressWarnings("unchecked")
  private Class<? extends Comparable> forName(String className) {
    try {
      // we know that the class is a Comparable because all partition keys are of type Comparable
      return (Class<? extends Comparable>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
