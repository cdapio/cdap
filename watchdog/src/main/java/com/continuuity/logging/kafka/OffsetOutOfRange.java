package com.continuuity.logging.kafka;

/**
 * Exception thrown when requested offset is out of range.
 */
public class OffsetOutOfRange extends Exception {
  public OffsetOutOfRange(String s) {
    super(s);
  }
}
