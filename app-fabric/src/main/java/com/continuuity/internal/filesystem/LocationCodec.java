/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.filesystem;

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
