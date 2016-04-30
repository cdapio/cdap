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

package co.cask.cdap.etl.common;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;

/**
 * Codec to serialize/deserialize a Multimap using Gson.
 *
 * @param <K> multimap key type
 * @param <V> multimap val type
 */
public class SetMultimapCodec<K, V> implements JsonSerializer<SetMultimap<K, V>>, JsonDeserializer<SetMultimap<K, V>> {
  private final Type mapType = new TypeToken<Map<K, Collection<V>>>() { }.getType();

  @Override
  public SetMultimap<K, V> deserialize(JsonElement json, Type typeOfT,
                                       JsonDeserializationContext context) throws JsonParseException {
    JsonObject obj = json.getAsJsonObject();
    Map<K, Collection<V>> map = context.deserialize(obj.get("map"), mapType);
    SetMultimap<K, V> multimap = HashMultimap.create();
    for (Map.Entry<K, Collection<V>> entry : map.entrySet()) {
      multimap.putAll(entry.getKey(), entry.getValue());
    }
    return multimap;
  }

  @Override
  public JsonElement serialize(SetMultimap<K, V> src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject obj = new JsonObject();
    obj.add("map", context.serialize(src.asMap()));
    return obj;
  }
}
