/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.serialize;

/**
 * Utility functions for serialization.
 */
public final class Util {
  private Util() {}

  public static String stringOrNull(Object obj) {
    return obj == null ? null : obj.toString();
  }
}
