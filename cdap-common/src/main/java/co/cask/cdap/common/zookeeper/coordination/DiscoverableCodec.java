/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.common.zookeeper.coordination;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.twill.discovery.Discoverable;

import java.lang.reflect.Type;
import java.net.InetSocketAddress;

/**
 * A Gson codec for {@link Discoverable}.
 *
 * NOTE: This class may move to different package when needed.
 */
public class DiscoverableCodec implements JsonSerializer<Discoverable>, JsonDeserializer<Discoverable> {

  @Override
  public JsonElement serialize(Discoverable src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty("service", src.getName());
    jsonObj.addProperty("hostname", src.getSocketAddress().getHostName());
    jsonObj.addProperty("port", src.getSocketAddress().getPort());
    return jsonObj;
  }

  @Override
  public Discoverable deserialize(JsonElement json, Type typeOfT,
                                  JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    final String service = jsonObj.get("service").getAsString();
    String hostname = jsonObj.get("hostname").getAsString();
    int port = jsonObj.get("port").getAsInt();
    final InetSocketAddress address = new InetSocketAddress(hostname, port);

    return new Discoverable() {
      @Override
      public String getName() {
        return service;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return address;
      }
    };
  }
}
