/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

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
