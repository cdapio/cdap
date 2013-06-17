package com.continuuity.common.logging.logback.serialize;

/**
 * Utility functions for serialization.
 */
public class Util {

  public static String stringOrNull(Object obj) {
    return obj == null ? null : obj.toString();
  }
}
