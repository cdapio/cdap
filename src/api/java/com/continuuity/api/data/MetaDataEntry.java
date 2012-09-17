package com.continuuity.api.data;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

public class MetaDataEntry {

  private String name;
  private String type;
  private Map<String, String> fields;

  public MetaDataEntry(String name, String type) {
    if (name == null)
      throw new IllegalArgumentException("name cannot be null");
    if (type == null)
      throw new IllegalArgumentException("type cannot be null");
    this.name = name;
    this.type = type;
    this.fields = Maps.newTreeMap();
  }

  public void addField(String field, String value) {
    if (field == null)
      throw new IllegalArgumentException("field cannot be null");
    if (value == null)
      throw new IllegalArgumentException("value cannot be null");
    this.fields.put(field, value);
  }

  public void setType(String type) {
    if (type == null)
      throw new IllegalArgumentException("type cannot be null");
    this.type = type;
  }

  public String getName() {
    return this.name;
  }

  public String getType() {
    return this.type;
  }

  public String get(String field) {
    if (field == null)
      throw new IllegalArgumentException("field cannot be null");
    return this.fields.get(field);
  }

  public Set<Map.Entry<String, String>> getFields() {
    return this.fields.entrySet();
  }
}
