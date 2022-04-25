/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.api.data.batch;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Adapter for serialization/deserialization of the {@link Input} class
 */
public class InputTypeAdapter implements JsonSerializer<Input>, JsonDeserializer<Input> {
  @Override
  public Input deserialize(JsonElement json,
                           Type type,
                            JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject obj = json.getAsJsonObject();

    // InputFormatProviderInput
    if (obj.get("inputFormatProvider") != null) {
      Class<?> inputFormatProviderInputClass = Input.InputFormatProviderInput.class;
      return context.deserialize(json, inputFormatProviderInputClass);
    }

    // DatasetInput
    Class<?> datasetInputClass = Input.DatasetInput.class;
    return context.deserialize(json, datasetInputClass);
  }

  @Override
  public JsonElement serialize(Input object,
                               Type type,
                               JsonSerializationContext jsonSerializationContext) {
    JsonElement jsonElem = jsonSerializationContext.serialize(object, object.getClass());
    return jsonElem;
  }
}
