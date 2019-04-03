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

package co.cask.cdap.internal.app.runtime.codec;

import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * A Gson codec for {@link Arguments}.
 */
public class ArgumentsCodec implements JsonSerializer<Arguments>, JsonDeserializer<Arguments> {

  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  @Override
  public Arguments deserialize(JsonElement json, Type typeOfT,
                               JsonDeserializationContext context) throws JsonParseException {
    Map<String, String> args = context.deserialize(json, MAP_STRING_STRING_TYPE);
    return new BasicArguments(args);
  }

  @Override
  public JsonElement serialize(Arguments src, Type typeOfSrc, JsonSerializationContext context) {
    return context.serialize(src.asMap(), MAP_STRING_STRING_TYPE);
  }
}
