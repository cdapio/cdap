package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.common.Bytes;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * Provides handy methods for simple table splits calculation.
 * <p>
 * This is copied from old data-fabric, could be improved.
 * <p>
 * NOTE: there's also seem to be a bug: first split should have open start and last one open end... TODO: fix it
 */
final class SplitsUtil {
  // Simplest stateless getSplits method implementation (doesn't use the actual stored data)

  /**
   * If the number of splits is not given, and we have no hints from the table structure (that can be implemented in
   * overriding implementations, though), the primitive getSplits methos will return up to this many splits. Note that
   * we cannot read this number from configuration, because the current OVCTable(Handle) does not pass configuration
   * down into the tables anywhere. See ENG-2395 for the fix.
   */
  static final int DEFAULT_NUMBER_OF_SPLITS = 8;

  /**
   * Simplest possible implementation of getSplits. Takes the given start and end and divides the key space in
   * between into (almost) even partitions, using a long integer approximation of the keys.
   */
  static List<KeyRange> primitiveGetSplits(int numSplits, byte[] start, byte[] stop) {
    // if the range is empty, return no splits
    if (start != null && stop != null && Bytes.compareTo(start, stop) >= 0) {
      return Collections.emptyList();
    }
    if (numSplits <= 0) {
      numSplits = DEFAULT_NUMBER_OF_SPLITS;
    }
    // for simplicity, we construct a long from the begin and end, divide the resulting long range into approximately
    // even splits, and convert the boundaries back to nyte array keys.
    long begin = longForKey(start, false);
    long end = longForKey(stop, true);
    double splitSize = ((double) (end - begin)) / ((double) numSplits);

    // each range will start with the stop key of the previous range.
    // start key of the first range is either the given start, or the least possible key {0};
    List<KeyRange> ranges = Lists.newArrayListWithExpectedSize(numSplits);
    byte[] current = start == null ? new byte[] { 0x00 } : start;
    for (int i = 1; i < numSplits; i++) {
      long bound = begin + (long) (splitSize * i);
      byte[] next = keyForBound(bound);
      // due to rounding and truncation, we may get a bound that is the same as the previous (or if the previous is
      // the start key, less than that). We may also get a bound that exceeds the stop key. In both cases we want to
      // ignore this bound and continue.
      if (Bytes.compareTo(current, next) < 0 && (stop == null || Bytes.compareTo(next, stop) < 0)) {
        ranges.add(new KeyRange(current, next));
        current = next;
      }
    }
    // end of the last range is always the given stop of the range to cover
    ranges.add(new KeyRange(current, stop));

    return ranges;
  }

  // helper method to approximate a row key as a long value. Takes the first 7 bytes from the key and prepends a 0x0;
  // if the key is less than 7 bytes, pads it with zeros to the right.
  static long longForKey(byte[] key, boolean isStop) {
    if (key == null) {
      return isStop ? 0xffffffffffffffL : 0L;
    } else {
      // leading zero helps avoid negative long values for keys beginning with a byte > 0x80
      final byte[] leadingZero = { 0x00 };
      byte[] x;
      if (key.length >= Bytes.SIZEOF_LONG - 1) {
        x = Bytes.add(leadingZero, Bytes.head(key, Bytes.SIZEOF_LONG - 1));
      } else {
        x = Bytes.padTail(Bytes.add(leadingZero, key), Bytes.SIZEOF_LONG - 1 - key.length);
      }
      return Bytes.toLong(x);
    }
  }

  // helper method to convert a long approximation of a long key into a range bound.
  // the following invariant holds: keyForBound(longForKey(key)) == removeTrailingZeros(key).
  // removing the trailing zeros is ok in the context that this is used (only for split bounds)
  // this is called keyForBound on purpose, and not keyForLong.
  static byte[] keyForBound(long value) {
    byte[] bytes = Bytes.tail(Bytes.toBytes(value), Bytes.SIZEOF_LONG - 1);
    int lastNonZero = bytes.length - 1;
    while (lastNonZero > 0 && bytes[lastNonZero] == 0) {
      lastNonZero--;
    }
    return Bytes.head(bytes, lastNonZero + 1);
  }
}
