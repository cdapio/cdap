/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract base class to provides common methods for all Codec.
 *
 * @param <T> The specification type that this codec handle.
 */
abstract class AbstractSpecificationCodec<T> implements JsonSerializer<T>, JsonDeserializer<T> {

  protected final <V> JsonElement serializeMap(Map<String, V> map,
                                               JsonSerializationContext context,
                                               Class<V> valueType) {
    Type type = new TypeToken<Map<String, V>>() { }.where(new TypeParameter<V>() { }, valueType).getType();
    return context.serialize(map, type);
  }

  protected final <V> Map<String, V> deserializeMap(JsonElement json,
                                                    JsonDeserializationContext context,
                                                    Class<V> valueType) {
    Type type = new TypeToken<Map<String, V>>() { }.where(new TypeParameter<V>() { }, valueType).getType();
    Map<String, V> map = context.deserialize(json, type);
    return map == null ? ImmutableMap.<String, V>of() : map;
  }

  protected final <V> JsonElement serializeSet(Set<V> set, JsonSerializationContext context, Class<V> valueType) {
    Type type = new TypeToken<Set<V>>() { }.where(new TypeParameter<V>() { }, valueType).getType();
    return context.serialize(set, type);
  }

  protected final <V> Set<V> deserializeSet(JsonElement json, JsonDeserializationContext context, Class<V> valueType) {
    Type type = new TypeToken<Set<V>>() { }.where(new TypeParameter<V>() { }, valueType).getType();
    Set<V> set = context.deserialize(json, type);
    return set == null ? ImmutableSet.<V>of() : set;
  }

  protected final <V> JsonElement serializeList(List<V> list, JsonSerializationContext context, Class<V> valueType) {
    Type type = new TypeToken<List<V>>() { }.where(new TypeParameter<V>() { }, valueType).getType();
    return context.serialize(list, type);
  }

  protected final <V> List<V> deserializeList(JsonElement json,
                                              JsonDeserializationContext context, Class<V> valueType) {
    Type type = new TypeToken<List<V>>() { }.where(new TypeParameter<V>() { }, valueType).getType();
    List<V> list = context.deserialize(json, type);
    return list == null ? ImmutableList.<V>of() : list;
  }
}
