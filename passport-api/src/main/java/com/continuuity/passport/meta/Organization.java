package com.continuuity.passport.meta;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Meta data that represents an organization.
 */
public class Organization {

  private final String id;
  private final String name;

  /**
   * Initialize organization with id and name.
   * @param id Id of the organization.
   * @param name Name of the organization.
   */
  public Organization(String id, String name) {
    this.id = id;
    this.name = name;
  }

  /**
   * @return Id
   */
  public String getId() {
    return id;
  }

  /**
   * @return Name
   */
  public String getName() {
    return name;
  }

  /**
   * Serialize organization into json string. The field names will be lowercase with underscores where captialized.
   * @return Json serialized String representing account
   */
  @Override
  public String toString() {
    Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    return gson.toJson(this);
  }

  /**
   * Deserialize Organization from json string.
   * @param jsonString json string to be deserialized.
   * @return instance of {@code Organization}
   */
  public static Organization fromString(String jsonString) {
    Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    return gson.fromJson(jsonString, Organization.class);
  }
}
