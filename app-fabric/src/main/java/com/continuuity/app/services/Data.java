package com.continuuity.app.services;

/**
 * Defines types of data supported by the system.
 */
public enum Data {
  STREAM(1, "Stream"),
  DATASET(2, "Dataset");

  private final int dataType;
  private final String name;

  private Data(int type, String prettyName) {
    this.dataType = type;
    this.name = prettyName;
  }

  public String prettyName() {
    return name;
  }

  public static Data valueofPrettyName(String pretty) {
    return valueOf(pretty.toUpperCase());
  }
}
