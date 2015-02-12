/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
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
