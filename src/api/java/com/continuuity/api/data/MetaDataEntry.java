package com.continuuity.api.data;

import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class MetaDataEntry {

  private String name;
  private String type;
  private Map<String, String> textFields;
  private Map<String, byte[]> binaryFields;

  public MetaDataEntry(String name, String type) {
    if (name == null)
      throw new IllegalArgumentException("name cannot be null");
    if (name.isEmpty())
      throw new IllegalArgumentException("name cannot be empty");
    if (type == null)
      throw new IllegalArgumentException("type cannot be null");
    if (type.isEmpty())
      throw new IllegalArgumentException("type cannot be empty");
    this.name = name;
    this.type = type;
    this.textFields = Maps.newTreeMap();
    this.binaryFields = Maps.newTreeMap();
  }

  public void addField(String field, String value) {
    if (field == null)
      throw new IllegalArgumentException("field cannot be null");
    if (field.isEmpty())
      throw new IllegalArgumentException("field cannot be empty");
    if (value == null)
      throw new IllegalArgumentException("value cannot be null");
    this.textFields.put(field, value);
  }

  public void addField(String field, byte[] value) {
    if (field == null)
      throw new IllegalArgumentException("field cannot be null");
    if (field.isEmpty())
      throw new IllegalArgumentException("field cannot be empty");
    if (value == null)
      throw new IllegalArgumentException("value cannot be null");
    this.binaryFields.put(field, value);
  }

  public String getName() {
    return this.name;
  }

  public String getType() {
    return this.type;
  }

  public String getTextField(String field) {
    if (field == null)
      throw new IllegalArgumentException("field cannot be null");
    return this.textFields.get(field);
  }

  public byte[] getBinaryField(String field) {
    if (field == null)
      throw new IllegalArgumentException("field cannot be null");
    return this.binaryFields.get(field);
  }

  public Set<String> getTextFields() {
    return this.textFields.keySet();
  }
  public Set<String> getBinaryFields() {
    return this.binaryFields.keySet();
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof MetaDataEntry)) return false;
    MetaDataEntry other = (MetaDataEntry)o;
    if (!this.name.equals(other.name)) return false;
    if (!this.type.equals(other.type)) return false;
    if (!this.textFields.equals(other.textFields)) return false;
    // can't use Map.equals for binary fields, because equals() doesn't work
    // on byte[]. Iterate over the map and compare fields one by one.
    if (!this.getBinaryFields().equals(other.getBinaryFields())) return false;
    for (String key : this.getBinaryFields())
      if (!Arrays.equals(this.getBinaryField(key), other.getBinaryField(key)))
        return false;
    return true;
  }
}
