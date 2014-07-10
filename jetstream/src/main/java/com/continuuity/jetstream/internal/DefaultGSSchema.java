package com.continuuity.jetstream.internal;

import com.continuuity.jetstream.api.GSSchema;
import com.continuuity.jetstream.api.PrimitiveType;

import java.util.LinkedHashMap;
import java.util.Set;

/**
 *
 */
public class DefaultGSSchema implements GSSchema {

  private LinkedHashMap<String, PrimitiveType> fieldNames;
  private Set<String> increasingFields;
  private Set<String> decreasingFields;

  public DefaultGSSchema(LinkedHashMap<String, PrimitiveType> fieldNames, Set<String> increasingFields,
                         Set<String> decreasingFields) {
    this.fieldNames = fieldNames;
    this.increasingFields = increasingFields;
    this.decreasingFields = decreasingFields;
  }

  @Override
  public LinkedHashMap<String, PrimitiveType> getFieldNames() {
    return fieldNames;
  }

  @Override
  public Set<String> getIncreasingFields() {
    return increasingFields;
  }

  @Override
  public Set<String> getDecreasingFields() {
    return decreasingFields;
  }

}
