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

import co.cask.cdap.api.data.stream.StreamBatchReadable;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.net.URI;

/**
 * Type adapter for {@link StreamBatchReadable}
 */
public class StreamBatchReadableTypeAdapter implements JsonSerializer<StreamBatchReadable>,
  JsonDeserializer<StreamBatchReadable> {
  @Override
  public StreamBatchReadable deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject obj = json.getAsJsonObject();
    if (obj.get("streamReadableUri") == null) {
      throw new JsonParseException("StreamBatchReadable not present");
    }
    return new StreamBatchReadable(URI.create(obj.get("streamReadableUri").getAsString()));
  }

  @Override
  public JsonElement serialize(StreamBatchReadable src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty("streamReadableUri", src.toURI().toString());
    return jsonObj;
  }
}
