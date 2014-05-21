package com.continuuity.hive;

/**
 * Thrown whenever hive service cannot be discovered.
 */
public class HiveServiceNotFoundException extends Exception {
  public HiveServiceNotFoundException(String msg) {
    super(msg);
  }
}
