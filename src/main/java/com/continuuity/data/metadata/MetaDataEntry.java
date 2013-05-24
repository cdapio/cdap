package com.continuuity.data.metadata;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * Class that stores the meta data entries.
 */
public class MetaDataEntry {

  private String account; // can never be null or empty

  private String application; // can be null but never empty

  private String id; // can never be null or empty

  private String type; // can never be null or empty

  private Map<String, String> textFields;

  private Map<String, byte[]> binaryFields;

  /**
   * @param account     account id related to the meta data entry
   * @param application application id related to the meta data entry
   * @param type        Type Meta data type
   * @param id          Meta data id
   */
  public MetaDataEntry(String account, String application, String type, String id) {
    if (account == null) {
      throw new IllegalArgumentException("account cannot be null");
    }
    if (account.isEmpty()) {
      throw new IllegalArgumentException("account cannot be empty");
    }
    if (id == null) {
      throw new IllegalArgumentException("id cannot be null");
    }
    if (id.isEmpty()) {
      throw new IllegalArgumentException("id cannot be empty");
    }
    if (application != null && application.isEmpty()) {
      throw new IllegalArgumentException("application cannot be empty");
    }
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    if (type.isEmpty()) {
      throw new IllegalArgumentException("type cannot be empty");
    }
    this.account = account;
    this.application = application;
    this.id = id;
    this.type = type;
    this.textFields = Maps.newTreeMap();
    this.binaryFields = Maps.newTreeMap();
  }

  /**
   * Adds a text field to the metadata.
   *
   * @param field field name
   * @param value field value
   */
  public void addField(String field, String value) {
    if (field == null) {
      throw new IllegalArgumentException("field cannot be null");
    }
    if (field.isEmpty()) {
      throw new IllegalArgumentException("field cannot be empty");
    }
    if (value == null) {
      throw new IllegalArgumentException("value cannot be null");
    }
    this.textFields.put(field, value);
  }


  /**
   * Adds a binary field to the metadata.
   *
   * @param field field name
   * @param value field value
   */
  public void addField(String field, byte[] value) {
    if (field == null) {
      throw new IllegalArgumentException("field cannot be null");
    }
    if (field.isEmpty()) {
      throw new IllegalArgumentException("field cannot be empty");
    }
    if (value == null) {
      throw new IllegalArgumentException("value cannot be null");
    }
    this.binaryFields.put(field, value);
  }

  /**
   * Get Account id.
   *
   * @return id
   */
  public String getAccount() {
    return account;
  }

  /**
   * Get Application Id.
   *
   * @return application id
   */
  public String getApplication() {
    return application;
  }

  /**
   * Get Metadata id.
   *
   * @return metadata id
   */
  public String getId() {
    return this.id;
  }

  /**
   * Get Metadata type.
   *
   * @return metadata type
   */
  public String getType() {
    return this.type;
  }

  /**
   * Returns Field value as a String.
   *
   * @param field field key
   * @return value
   */
  public String getTextField(String field) {
    if (field == null) {
      throw new IllegalArgumentException("field cannot be null");
    }
    return this.textFields.get(field);
  }


  /**
   * Returns binary field value.
   *
   * @param field field key
   * @return value
   */
  public byte[] getBinaryField(String field) {
    if (field == null) {
      throw new IllegalArgumentException("field cannot be null");
    }
    return this.binaryFields.get(field);
  }

  /**
   * Returns the keys of all text fields.
   *
   * @return Set of keys
   */
  public Set<String> getTextFields() {
    return this.textFields.keySet();
  }

  /**
   * Returns the keys of all binary fields.
   *
   * @return Set of keys
   */
  public Set<String> getBinaryFields() {
    return this.binaryFields.keySet();
  }

  /**
   * Comparison function.
   *
   * @param o Object to be compared
   * @return boolean true if the objects passed is equal, false otherwise
   */
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MetaDataEntry)) {
      return false;
    }
    MetaDataEntry other = (MetaDataEntry) o;
    if (!this.account.equals(other.account)) {
      return false;
    }
    if (this.application == null && other.application != null) {
      return false;
    }
    if (this.application != null && !this.application.equals(other.application)) {
      return false;
    }
    if (!this.id.equals(other.id)) {
      return false;
    }
    if (!this.type.equals(other.type)) {
      return false;
    }
    if (!this.textFields.equals(other.textFields)) {
      return false;
    }
    // can't use Map.equals for binary fields, because equals() doesn't work
    // on byte[]. Iterate over the map and compare fields one by one.
    if (!this.getBinaryFields().equals(other.getBinaryFields())) {
      return false;
    }
    for (String key : this.getBinaryFields()) {
      if (!Arrays.equals(this.getBinaryField(key), other.getBinaryField(key))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns String representation of the MetaDataEntry object. The value returned contains accountId, applicationId,
   * metadata type and metadata id.
   *
   * @return a String representation of the object
   */
  public String toString() {
    return Objects.toStringHelper(this).
      add("account", account).
      add("application", application).
      add("type", type).
      add("id", id).
      toString();
  }
}
