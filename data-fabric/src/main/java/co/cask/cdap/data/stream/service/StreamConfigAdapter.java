/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.data2.transaction.stream.StreamConfig;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.concurrent.TimeUnit;

/**
 *  Serializer for {@link co.cask.cdap.data2.transaction.stream.StreamConfig} class
 */
class StreamConfigAdapter implements JsonSerializer<StreamConfig> {
  @Override
  public JsonElement serialize(StreamConfig src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("partitionDuration", src.getPartitionDuration());
    json.addProperty("indexInterval", src.getIndexInterval());
    json.addProperty("ttl", TimeUnit.MILLISECONDS.toSeconds(src.getTTL()));
    return json;
  }
}
