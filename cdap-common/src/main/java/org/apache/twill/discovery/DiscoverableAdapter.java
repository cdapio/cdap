/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.discovery;

import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.net.InetSocketAddress;

/**
 * Helper class to serialize and deserialize {@link Discoverable}.
 */
final class DiscoverableAdapter {

  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Discoverable.class, new DiscoverableCodec()).create();
  private static final Type BYTE_ARRAY_TYPE = new TypeToken<byte[]>() { }.getType();

  /**
   * Helper function for encoding an instance of {@link Discoverable} into array of bytes.
   * @param discoverable An instance of {@link Discoverable}
   * @return array of bytes representing an instance of <code>discoverable</code>
   */
  static byte[] encode(Discoverable discoverable) {
    return GSON.toJson(discoverable, Discoverable.class).getBytes(Charsets.UTF_8);
  }

  /**
   * Helper function for decoding array of bytes into a {@link Discoverable} object.
   * @param encoded representing serialized {@link Discoverable}
   * @return {@code null} if encoded bytes are null; else an instance of {@link Discoverable}
   */
  static Discoverable decode(byte[] encoded) {
    if (encoded == null) {
      return null;
    }
    return GSON.fromJson(new String(encoded, Charsets.UTF_8), Discoverable.class);
  }

  private DiscoverableAdapter() {
  }

  /**
   * SerDe for converting a {@link Discoverable} into a JSON object
   * or from a JSON object into {@link Discoverable}.
   */
  private static final class DiscoverableCodec implements JsonSerializer<Discoverable>, JsonDeserializer<Discoverable> {

    @Override
    public Discoverable deserialize(JsonElement json, Type typeOfT,
                                    JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();
      String service = jsonObj.get("service").getAsString();
      String hostname = jsonObj.get("hostname").getAsString();
      int port = jsonObj.get("port").getAsInt();
      byte[] payload = context.deserialize(jsonObj.get("payload"), BYTE_ARRAY_TYPE);
      InetSocketAddress address = new InetSocketAddress(hostname, port);
      return new Discoverable(service, address, payload);
    }

    @Override
    public JsonElement serialize(Discoverable src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject jsonObj = new JsonObject();
      jsonObj.addProperty("service", src.getName());
      jsonObj.addProperty("hostname", src.getSocketAddress().getHostName());
      jsonObj.addProperty("port", src.getSocketAddress().getPort());
      jsonObj.add("payload", context.serialize(src.getPayload()));
      return jsonObj;
    }
  }
}
