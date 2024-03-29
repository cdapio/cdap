/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.table;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.table.Filter;
import io.cdap.cdap.common.utils.ImmutablePair;
import java.util.Arrays;
import java.util.List;

/**
 * This is inspired by HBase's FuzzyRowFilter.
 *
 * Filters data based on fuzzy row key. Performs fast-forwards during scanning. It takes pairs (row
 * key, fuzzy info) to match row keys. Where fuzzy info is a byte array with 0 or 1 as its values:
 * <ul>
 *   <li>
 *     0 - means that this byte in provided row key is fixed, i.e. row key's byte at same position
 *         must match
 *   </li>
 *   <li>
 *     1 - means that this byte in provided row key is NOT fixed, i.e. row key's byte at this
 *         position can be different from the one in provided row key
 *   </li>
 * </ul>
 *
 *
 * Example:
 * Let's assume row key format is userId_actionId_year_month. Length of userId is fixed
 * and is 4, length of actionId is 2 and year and month are 4 and 2 bytes long respectively.
 *
 * Let's assume that we need to fetch all users that performed certain action (encoded as "99")
 * in Jan of any year. Then the pair (row key, fuzzy info) would be the following:
 * row key = "????_99_????_01" (one can use any value instead of "?")
 * fuzzy info = "\x01\x01\x01\x01\x00\x00\x00\x00\x01\x01\x01\x01\x00\x00\x00"
 *
 * I.e. fuzzy info tells the matching mask is "????_99_????_01", where at ? can be any value.
 */
public final class FuzzyRowFilter implements Filter {

  private final List<ImmutablePair<byte[], byte[]>> fuzzyKeysData;

  public FuzzyRowFilter(List<ImmutablePair<byte[], byte[]>> fuzzyKeysData) {
    this.fuzzyKeysData = fuzzyKeysData;
  }

  public List<ImmutablePair<byte[], byte[]>> getFuzzyKeysData() {
    return fuzzyKeysData;
  }

  /**
   * Return codes for filterRow().
   */
  public enum ReturnCode {
    /**
     * Include this row.
     */
    INCLUDE,
    /**
     * Seek to next row which is given as hint by the filter.
     */
    SEEK_NEXT_USING_HINT,
    /**
     * No greater rows can possibly match.
     */
    DONE
  }

  public ReturnCode filterRow(byte[] rowKey) {
    // assigning "worst" result first and looking for better options
    SatisfiesCode bestOption = SatisfiesCode.NO_NEXT;
    for (ImmutablePair<byte[], byte[]> fuzzyData : fuzzyKeysData) {
      SatisfiesCode satisfiesCode =
          satisfies(rowKey, fuzzyData.getFirst(), fuzzyData.getSecond());
      if (satisfiesCode == SatisfiesCode.YES) {
        return ReturnCode.INCLUDE;
      }

      if (satisfiesCode == SatisfiesCode.NEXT_EXISTS) {
        bestOption = SatisfiesCode.NEXT_EXISTS;
      }
    }

    if (bestOption == SatisfiesCode.NEXT_EXISTS) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }

    // the only unhandled SatisfiesCode is NO_NEXT, i.e. we are done
    return ReturnCode.DONE;
  }

  public byte[] getNextRowHint(byte[] rowKey) {
    byte[] nextRowKey = null;
    // Searching for the "smallest" row key that satisfies at least one fuzzy row key
    for (ImmutablePair<byte[], byte[]> fuzzyData : fuzzyKeysData) {
      byte[] nextRowKeyCandidate = getNextForFuzzyRule(rowKey,
          fuzzyData.getFirst(), fuzzyData.getSecond());
      if (nextRowKeyCandidate == null) {
        continue;
      }
      if (nextRowKey == null || Bytes.compareTo(nextRowKeyCandidate, nextRowKey) < 0) {
        nextRowKey = nextRowKeyCandidate;
      }
    }

    if (nextRowKey == null) {
      // SHOULD NEVER happen
      // TODO: is there a better way than throw exception? (stop the scanner?)
      throw new IllegalStateException("No next row key that satisfies fuzzy exists when"
          + " getNextKeyHint() is invoked."
          + " Filter: " + this.toString()
          + " RowKey: " + Bytes.toStringBinary(rowKey));
    }
    return nextRowKey;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("FuzzyRowFilter");
    sb.append("{fuzzyKeysData=");
    for (ImmutablePair<byte[], byte[]> fuzzyData : fuzzyKeysData) {
      sb.append('{').append(Bytes.toStringBinary(fuzzyData.getFirst())).append(":");
      sb.append(Bytes.toStringBinary(fuzzyData.getSecond())).append('}');
    }
    sb.append("}, ");
    return sb.toString();
  }

  // Utility methods

  enum SatisfiesCode {
    // row satisfies fuzzy rule
    YES,
    // row doesn't satisfy fuzzy rule, but there's possible greater row that does
    NEXT_EXISTS,
    // row doesn't satisfy fuzzy rule and there's no greater row that does
    NO_NEXT
  }

  private static SatisfiesCode satisfies(byte[] row,
      byte[] fuzzyKeyBytes, byte[] fuzzyKeyMeta) {
    return satisfies(row, 0, row.length, fuzzyKeyBytes, fuzzyKeyMeta);
  }

  private static SatisfiesCode satisfies(byte[] row, int offset, int length,
      byte[] fuzzyKeyBytes, byte[] fuzzyKeyMeta) {
    if (row == null) {
      // do nothing, let scan to proceed
      return SatisfiesCode.YES;
    }

    boolean nextRowKeyCandidateExists = false;

    for (int i = 0; i < fuzzyKeyMeta.length && i < length; i++) {
      // First, checking if this position is fixed and not equals the given one
      boolean byteAtPositionFixed = fuzzyKeyMeta[i] == 0;
      boolean fixedByteIncorrect = byteAtPositionFixed && fuzzyKeyBytes[i] != row[i + offset];
      if (fixedByteIncorrect) {
        // in this case there's another row that satisfies fuzzy rule and bigger than this row
        if (nextRowKeyCandidateExists) {
          return SatisfiesCode.NEXT_EXISTS;
        }

        // If this row byte is less than fixed then there's a byte array bigger than
        // this row and which satisfies the fuzzy rule. Otherwise there's no such byte array:
        // this row is simply bigger than any byte array that satisfies the fuzzy rule
        boolean rowByteLessThanFixed = (row[i + offset] & 0xFF) < (fuzzyKeyBytes[i] & 0xFF);
        return rowByteLessThanFixed ? SatisfiesCode.NEXT_EXISTS : SatisfiesCode.NO_NEXT;
      }

      // Second, checking if this position is not fixed and byte value is not the biggest. In this
      // case there's a byte array bigger than this row and which satisfies the fuzzy rule. To get
      // bigger byte array that satisfies the rule we need to just increase this byte
      // (see the code of getNextForFuzzyRule below) by one.
      // Note: if non-fixed byte is already at biggest value, this doesn't allow us to say there's
      //       bigger one that satisfies the rule as it can't be increased.
      if (fuzzyKeyMeta[i] == 1 && !isMax(fuzzyKeyBytes[i])) {
        nextRowKeyCandidateExists = true;
      }
    }

    return SatisfiesCode.YES;
  }

  private static boolean isMax(byte fuzzyKeyByte) {
    return (fuzzyKeyByte & 0xFF) == 255;
  }

  private static byte[] getNextForFuzzyRule(byte[] row, byte[] fuzzyKeyBytes, byte[] fuzzyKeyMeta) {
    return getNextForFuzzyRule(row, 0, row.length, fuzzyKeyBytes, fuzzyKeyMeta);
  }

  /**
   * @return greater byte array than given (row) which satisfies the fuzzy rule if it exists, null
   *     otherwise
   */
  private static byte[] getNextForFuzzyRule(byte[] row, int offset, int length,
      byte[] fuzzyKeyBytes, byte[] fuzzyKeyMeta) {
    // To find out the next "smallest" byte array that satisfies fuzzy rule and "greater" than
    // the given one we do the following:
    // 1. setting values on all "fixed" positions to the values from fuzzyKeyBytes
    // 2. if during the first step given row did not increase, then we increase the value at
    //    the first "non-fixed" position (where it is not maximum already)

    // It is easier to perform this by using fuzzyKeyBytes copy and setting "non-fixed" position
    // values than otherwise.
    byte[] result = Arrays.copyOf(fuzzyKeyBytes,
        length > fuzzyKeyBytes.length ? length : fuzzyKeyBytes.length);
    int toInc = -1;

    boolean increased = false;
    for (int i = 0; i < result.length; i++) {
      if (i >= fuzzyKeyMeta.length || fuzzyKeyMeta[i] == 1) {
        result[i] = row[offset + i];
        if (!isMax(row[i])) {
          // this is "non-fixed" position and is not at max value, hence we can increase it
          toInc = i;
        }
      } else if (i < fuzzyKeyMeta.length && fuzzyKeyMeta[i] == 0) {
        if ((row[i + offset] & 0xFF) < (fuzzyKeyBytes[i] & 0xFF)) {
          // if setting value for any fixed position increased the original array,
          // we are OK
          increased = true;
          break;
        }
        if ((row[i + offset] & 0xFF) > (fuzzyKeyBytes[i] & 0xFF)) {
          // if setting value for any fixed position makes array "smaller", then just stop:
          // in case we found some non-fixed position to increase we will do it, otherwise
          // there's no "next" row key that satisfies fuzzy rule and "greater" than given row
          break;
        }
      }
    }

    if (!increased) {
      if (toInc < 0) {
        return null;
      }
      result[toInc]++;

      // Setting all "non-fixed" positions to zeroes to the right of the one we increased so
      // that found "next" row key is the smallest possible
      for (int i = toInc + 1; i < result.length; i++) {
        if (i >= fuzzyKeyMeta.length || fuzzyKeyMeta[i] == 1) {
          result[i] = 0;
        }
      }
    }

    return result;
  }
}
