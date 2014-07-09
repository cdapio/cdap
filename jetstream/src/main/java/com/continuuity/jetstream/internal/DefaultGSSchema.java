package com.continuuity.jetstream.internal;

import com.continuuity.jetstream.api.GSSchema;
import com.continuuity.jetstream.api.PrimitiveType;

import java.util.LinkedHashMap;
import java.util.Set;

/**
 *
 */
public class DefaultGSSchema implements GSSchema {

  private String name;
  private LinkedHashMap<String, PrimitiveType> fieldNames;
  private Set<String> increasingFields;
  private Set<String> decreasingFields;

  public DefaultGSSchema(String name, LinkedHashMap<String, PrimitiveType> fieldNames, Set<String> increasingFields,
                         Set<String> decreasingFields) {
    this.name = name;
    this.fieldNames = fieldNames;
    this.increasingFields = increasingFields;
    this.decreasingFields = decreasingFields;
  }

  @Override
  public String getName() {
    return name;
  }

}
