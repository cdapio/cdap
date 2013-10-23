/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.batch;

import java.util.HashMap;
import java.util.Map;

/**
 * Handy implementation of the {@link Split}. Acts as a map of attributes.
 */
public final class SimpleSplit extends Split {
  private Map<String, String> attributes = new HashMap<String, String>();

  /**
   * Sets an attribute.
   * @param name Name of the attribute.
   * @param value Value of the attribute.
   */
  public void set(String name, String value) {
    attributes.put(name, value);
  }

  /**
   * Gets an attribute value.
   * @param name Name of the attribute to get the value of.
   * @return value Value of the attribute, or null if not found.
   */
  public String get(String name) {
    return get(name, null);
  }

  /**
   * Gets an attribute value.
   * @param name Name of the attribute to get the value of.
   * @param defaultValue The value to return if the attribute is not found.
   * @return Value of the attribute, or the default value if the value is not found
   */
  public String get(String name, String defaultValue) {
    String value = attributes.get(name);
    return value == null ? defaultValue : value;
  }
}
