package com.continuuity.data.operation;

import javax.annotation.Nullable;

/**
 * Describes a range of (row) keys.
 */
public class KeyRange {
  final byte[] start, stop;

  /**
   * Constructor from start and end.
   * @param start the start of the range; if null, then starts with the least key available.
   * @param stop the end of the range (exclusive); if null, then extends to the greatest key available.
   */
  public KeyRange(@Nullable byte[] start, @Nullable byte[] stop) {
    this.start = start;
    this.stop = stop;
  }

  /**
   * @return the start key of the range
   */
  public byte[] getStart() {
    return start;
  }

  /**
   * @return the exclusive end key of the range
   */
  public byte[] getStop() {
    return stop;
  }
}
