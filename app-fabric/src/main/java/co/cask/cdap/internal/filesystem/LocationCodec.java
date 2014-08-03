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

package co.cask.cdap.internal.filesystem;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.lang.reflect.Type;

/**
 * Codec for {@link Location}. We write {@link java.net.URI} for location.
 */
public final class LocationCodec implements JsonSerializer<Location>, JsonDeserializer<Location> {
  private final LocationFactory lf;

  public LocationCodec(LocationFactory lf) {
    this.lf = lf;
  }

  @Override
  public JsonElement serialize(Location src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.add("uri", new JsonPrimitive(src.toURI().toASCIIString()));
    return jsonObj;
  }

  @Override
  public Location deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
    JsonObject jsonObj = json.getAsJsonObject();
    String uri = jsonObj.get("uri").getAsString();
    return lf.create(uri);
  }
}
