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

package co.cask.cdap.common.zookeeper.coordination;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;

import java.lang.reflect.Type;

/**
 * A Gson codec for {@link org.apache.twill.discovery.ServiceDiscovered}
 */
public class ServiceDiscoveredCodec implements JsonSerializer<ServiceDiscovered> {

  @Override
  public JsonElement serialize(ServiceDiscovered serviceDiscovered, Type typeOfSrc, JsonSerializationContext context) {
    JsonArray object = new JsonArray();
    for (Discoverable discoverable : serviceDiscovered) {
      JsonObject discoverableJson = new JsonObject();
      discoverableJson.addProperty("host", discoverable.getSocketAddress().getHostName());
      discoverableJson.addProperty("port", discoverable.getSocketAddress().getPort());

      object.add(discoverableJson);
    }
    return object;
  }
}
