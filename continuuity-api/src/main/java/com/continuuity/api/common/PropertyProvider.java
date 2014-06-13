/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.common;

import java.util.Map;

/**
 * Represents classes that provides properties.
 */
public interface PropertyProvider {

  /**
   * Returns an immutable Map of all properties.
   */
  Map<String, String> getProperties();

  /**
   * Return the property value of a given key.
   * @param key for getting specific property value.
   * @return The value associated with the key or {@code null} if not such key exists.
   */
  String getProperty(String key);
}
