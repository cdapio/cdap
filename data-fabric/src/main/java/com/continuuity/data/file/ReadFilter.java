/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.file;

/**
 * Filter for reading from {@link FileReader}.
 *
 * This class is experimental is still expanding.
 */
public abstract class ReadFilter {

  /**
   * Always accept what it sees.
   */
  public static final ReadFilter ALWAYS_ACCEPT = new ReadFilter() { };

  /**
   * Always reject offset.
   */
  public static final ReadFilter ALWAYS_REJECT_OFFSET = new ReadFilter() {
    @Override
    public boolean acceptOffset(long offset) {
      return false;
    }
  };

  /**
   * Accept or reject based on file offset.
   *
   * @param offset The file offset.
   * @return {@code true} to accept, {@code false} to reject.
   */
  public boolean acceptOffset(long offset) {
    return true;
  }

  /**
   * Accept or reject based on event timestamp.
   *
   * @param timestamp The timestamp of the event.
   * @return {@code true} to accept, {@code false} to reject.
   */
  public boolean acceptTimestamp(long timestamp) {
    return true;
  }
}
