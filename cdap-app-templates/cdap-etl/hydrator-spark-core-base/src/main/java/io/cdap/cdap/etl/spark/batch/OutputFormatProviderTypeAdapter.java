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

import co.cask.cdap.api.data.batch.OutputFormatProvider;
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
 * Type adapter for {@link OutputFormatProvider}
 */
public class OutputFormatProviderTypeAdapter implements JsonSerializer<OutputFormatProvider>,
  JsonDeserializer<OutputFormatProvider> {
  private static final Type mapType = new TypeToken<Map<String, String>>() { }.getType();

  @Override
  public OutputFormatProvider deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject obj = json.getAsJsonObject();
    // if outputformat is not present, return empty OutputFormatProvider
    if (obj.get("outputFormatClass") == null) {
      return new SparkBatchSinkFactory.BasicOutputFormatProvider();
    }
    String className = obj.get("outputFormatClass").getAsString();
    Map<String, String> conf = context.deserialize(obj.get("outputFormatConfig"), mapType);
    return new SparkBatchSinkFactory.BasicOutputFormatProvider(className, conf);
  }

  @Override
  public JsonElement serialize(OutputFormatProvider src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty("outputFormatClass", src.getOutputFormatClassName());
    jsonObj.add("outputFormatConfig", context.serialize(src.getOutputFormatConfiguration()));
    return jsonObj;
  }
}
