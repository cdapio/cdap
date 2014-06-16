package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.common.Bytes;
import com.google.common.base.Objects;

import java.util.Arrays;
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

  @Override
  public int hashCode() {
    return Objects.hashCode(start, stop);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (KeyRange.class != obj.getClass()) {
      return false;
    }
    KeyRange other = (KeyRange) obj;
    return Arrays.equals(this.start, other.start) && Arrays.equals(this.stop, other.stop);
  }

  @Override
  public String toString() {
    return "(" + (start == null ? "null" : "'" + Bytes.toStringBinary(start)) + "'"
      + ".." + (stop == null ? "null" : "'" + Bytes.toStringBinary(stop) + "'") + ")";
  }
}
