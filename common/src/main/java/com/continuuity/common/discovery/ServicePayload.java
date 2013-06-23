package com.continuuity.common.discovery;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

/**
 * Defines the payload that is placed for every service.
 */
public class ServicePayload {
  private final Map<String, String> values = Maps.newHashMap();

  /**
   * Adds a key and value as service payload.
   *
   * @param key to be stored.
   * @param value to be associated with key.
   */
  public void add(String key, String value) {
    values.put(key, value);
  }

  public String get(String key) {
    return values.get(key);
  }

  public Set<Map.Entry<String, String>> getAll() {
    return values.entrySet();
  }
}

