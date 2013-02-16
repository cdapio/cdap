/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.QueueSpecification;
import com.continuuity.api.io.Schema;
import com.continuuity.internal.io.SimpleQueueSpecification;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class QueueSpecificationCodec implements JsonSerializer<QueueSpecification>, JsonDeserializer<QueueSpecification> {


  @Override
  public JsonElement serialize(QueueSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.add("queue", new JsonPrimitive(src.getURI().toASCIIString()));
    jsonObj.add("schema", context.serialize(src.getSchema(), Schema.class));
    return jsonObj;
  }

  @Override
  public QueueSpecification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    String queue = jsonObj.get("queue").getAsString();
    URI uri = URI.create(queue);
    Schema schema = context.deserialize(jsonObj.get("schema"), Schema.class);
    return new SimpleQueueSpecification(uri, schema);
  }

}
