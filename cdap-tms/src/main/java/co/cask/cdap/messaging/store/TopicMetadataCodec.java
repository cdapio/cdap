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

package co.cask.cdap.messaging.store;

import co.cask.cdap.internal.guava.reflect.TypeParameter;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.proto.id.TopicId;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Codec class to serialize/deserialize {@link TopicMetadata}.
 */
public class TopicMetadataCodec implements JsonSerializer<TopicMetadata>, JsonDeserializer<TopicMetadata> {

  @Override
  public TopicMetadata deserialize(JsonElement json, Type type,
                                   JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();

    String namespace = jsonObject.get("namespace").getAsString();
    String topic = jsonObject.get("topic").getAsString();
    Map<String, String> properties = deserializeSortedMap(jsonObject.get("properties"), context, String.class);
    int generation = jsonObject.get("generation").getAsInt();
    TopicId topicId = new TopicId(namespace, topic);
    return new TopicMetadata(topicId, properties, generation);
  }

  @Override
  public JsonElement serialize(TopicMetadata topicMetadata, Type type, JsonSerializationContext context) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.add("namespace", new JsonPrimitive(topicMetadata.getTopicId().getNamespace()));
    jsonObject.add("topic", new JsonPrimitive(topicMetadata.getTopicId().getTopic()));
    jsonObject.add("properties", serializeSortedMap(topicMetadata.getProperties(), context, String.class));
    jsonObject.add("generation", new JsonPrimitive(topicMetadata.getGeneration()));
    return jsonObject;
  }

  private <V> JsonElement serializeSortedMap(Map<String, V> map,
                                               JsonSerializationContext context,
                                               Class<V> valueType) {
    Type type = new TypeToken<SortedMap<String, V>>() { }.where(new TypeParameter<V>() { }, valueType).getType();
    return context.serialize(map, type);
  }

  private <V> Map<String, V> deserializeSortedMap(JsonElement json,
                                                    JsonDeserializationContext context,
                                                    Class<V> valueType) {
    Type type = new TypeToken<SortedMap<String, V>>() { }.where(new TypeParameter<V>() { }, valueType).getType();
    Map<String, V> map = context.deserialize(json, type);
    return map == null ? new TreeMap<String, V>() : map;
  }
}
