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
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * A Gson codec for {@link ProgramOptions}.
 */
public class ProgramOptionsCodec implements JsonSerializer<ProgramOptions>, JsonDeserializer<ProgramOptions> {

  @Override
  public ProgramOptions deserialize(JsonElement json, Type typeOfT,
                                    JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    String name = jsonObj.get("name").getAsString();
    Arguments arguments = context.deserialize(jsonObj.get("arguments"), Arguments.class);
    Arguments userArguments = context.deserialize(jsonObj.get("userArguments"), Arguments.class);
    boolean debug = jsonObj.get("debug").getAsBoolean();

    return new SimpleProgramOptions(name, arguments, userArguments, debug);
  }

  @Override
  public JsonElement serialize(ProgramOptions src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("name", src.getName());
    json.add("arguments", context.serialize(src.getArguments(), Arguments.class));
    json.add("userArguments", context.serialize(src.getUserArguments(), Arguments.class));
    json.addProperty("debug", src.isDebug());

    return json;
  }
}
