package com.continuuity.jetstream.api;

/**
 * PrimitiveTypes used in the GDAT format.
 */
public enum PrimitiveType {
  BOOL("bool"),
  USHORT("ushort"),
  UINT("uint"),
  INT("int"),
  ULLONG("ullong"),
  LLONG("llong"),
  FLOAT("float"),
  STRING("string");

  private String type;

  private PrimitiveType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }
}
