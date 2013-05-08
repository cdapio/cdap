package com.continuuity.data.table;

import com.continuuity.data.operation.KeyRange;
import com.continuuity.data.operation.executor.ReadPointer;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Provides common implementations for some OVC table methods.
 */
public abstract class AbstractOVCTable implements OrderedVersionedColumnarTable {

  /**
   * Fallback implementation of getSplits, @see #primitiveGetSplits()
   */
  @Override
  public List<KeyRange> getSplits(int numSplits, byte[] start, byte[] stop, byte[][] columns, ReadPointer pointer) {
    return primitiveGetSplits(numSplits, start, stop);
  }

  /**
   * Simplest possible implementation of getSplits. Takes the given start and end and divides the key space in
   * between into (almost) even partitions, using single-byte keys as start and stop, except for the start of the
   * first and the end of the last range - they must be the same as the start and end of the requested range.
   */
  public static List<KeyRange> primitiveGetSplits(int numSplits, byte[] start, byte[] stop) {
    if (numSplits <= 0) {
      numSplits = 8;
    }
    // for simplicity, we will divide the range into splits that have single byte start and stop keys. We start with
    // the first byte of the given start, and end with the first byte of the given stop key.
    int begin = start == null || start.length == 0 ? 0 : ((int) start[0]) & 0xff;
    int end = stop == null || stop.length == 0 ? 256 : ((int) stop[0]) & 0xff;

    // each range will start with the stop key of the previous range.
    // start key of the first range is either the given start, or the least possible key {0};
    List<KeyRange> ranges = Lists.newArrayListWithExpectedSize(numSplits);
    byte[] current = start == null ? new byte[] { 0x00 } : start;
    for (int i = 1; i < numSplits; i++) {
      byte[] next = new byte[] { (byte) (begin + (end - begin) * i / numSplits) };
      ranges.add(new KeyRange(current, next));
      current = next;
    }
    // end of the last range is either the given stop of the range to cover
    ranges.add(new KeyRange(current, stop));

    return ranges;
  }
}
