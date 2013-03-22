package com.continuuity.api.data.batch;

import java.util.HashMap;
import java.util.Map;

public class SimpleSplit extends Split {
  private Map<String, String> attributes = new HashMap<String, String>();

  public void set(String name, String value) {
    attributes.put(name, value);
  }

  public String get(String name) {
    return get(name, null);
  }

  public String get(String name, String defaultValue) {
    String value = attributes.get(name);
    return value == null ? defaultValue : value;
  }
}
