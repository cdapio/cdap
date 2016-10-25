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

package co.cask.cdap.etl.spark.batch;

import co.cask.cdap.api.data.batch.InputFormatProvider;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Type adapter for {@link InputFormatProvider}
 */
public class InputFormatProviderTypeAdapter implements JsonSerializer<InputFormatProvider>,
  JsonDeserializer<InputFormatProvider> {
  private static final Type mapType = new TypeToken<Map<String, String>>() { }.getType();

  @Override
  public InputFormatProvider deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject obj = json.getAsJsonObject();
    // if inputFormat is not present, return empty InputFormatProvider
    if (obj.get("inputFormatClass") == null) {
      return new SparkBatchSourceFactory.BasicInputFormatProvider();
    }
    String className = obj.get("inputFormatClass").getAsString();
    Map<String, String> conf = context.deserialize(obj.get("inputFormatConfig"), mapType);
    return new SparkBatchSourceFactory.BasicInputFormatProvider(className, conf);
  }

  @Override
  public JsonElement serialize(InputFormatProvider src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty("inputFormatClass", src.getInputFormatClassName());
    jsonObj.add("inputFormatConfig", context.serialize(src.getInputFormatConfiguration()));
    return jsonObj;
  }
}
