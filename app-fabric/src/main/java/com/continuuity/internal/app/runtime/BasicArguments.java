/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime;

import com.continuuity.app.runtime.Arguments;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public final class BasicArguments implements Arguments {

  private final Map<String, String> options;

  public BasicArguments() {
    this(ImmutableMap.<String, String>of());
  }

  public BasicArguments(Map<String, String> options) {
    this.options = ImmutableMap.copyOf(options);
  }

  @Override
  public boolean hasOption(String optionName) {
    return options.containsKey(optionName);
  }

  @Override
  public String getOption(String name) {
    return options.get(name);
  }

  @Override
  public String getOption(String name, String defaultOption) {
    String value = getOption(name);
    return value == null ? defaultOption : value;
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return options.entrySet().iterator();
  }
}
