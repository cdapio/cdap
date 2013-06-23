package com.continuuity.data.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.data.operation.KeyRange;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tests static methods in the abstract OVC table
 */
public class TestAbtractOVCTable {

  private void testPrimitiveNoSplits(int numSplits, byte[] start, byte[] stop) {
    List<KeyRange> splits = AbstractOVCTable.primitiveGetSplits(numSplits, start, stop);
    Assert.assertTrue(splits.isEmpty());
  }

  private void testPrimitiveSplits(int numSplits, byte[] start, byte[] stop) {
    // get the splits
    List<KeyRange> splits = AbstractOVCTable.primitiveGetSplits(numSplits, start, stop);
    System.out.println("splits: " + splits);
    // at least one split is expected
    Assert.assertFalse(splits.isEmpty());
    // if a number was requested, then at most that number of splits must be returned
    if (numSplits > 0) {
      Assert.assertTrue(numSplits >= splits.size());
    }
    // verify the entire requested range was covered
    KeyRange first = splits.get(0);
    Assert.assertArrayEquals(start == null ? new byte[] { 0x00 } : start, first.getStart());
    KeyRange last = splits.get(splits.size() - 1);
    Assert.assertArrayEquals(stop, last.getStop());
    // every split must start with the stop of the previous one
    for (int i = 1; i < splits.size(); i++) {
      Assert.assertArrayEquals(splits.get(i - 1).getStop(), splits.get(i).getStart());
    }
    // all split must be non-empty (stop > start)
    for (KeyRange split : splits) {
      if (split.getStop() != null) {
        Assert.assertTrue(0 < Bytes.compareTo(split.getStop(), split.getStart()));
      }
    }
    // all splits should have about the same length - we verify that by looking at only the
    // first byte of each start and stop
    Set<Integer> lengths = new HashSet<Integer>();
    int prev = first.getStart()[0] & 0xff;
    for (int i = 1; i < splits.size(); i++) {
      int cur = splits.get(i).getStart()[0] & 0xff;
      int diff = cur - prev;
      Assert.assertTrue(diff >= 0);
      lengths.add(diff);
      prev = cur;
    }
    Assert.assertTrue(lengths.size() <= 2);
    if (lengths.size() == 2) {
      Integer[] ls = lengths.toArray(new Integer[2]);
      int diff = ls[0] - ls[1];
      Assert.assertEquals(1, diff * diff);
    }
  }

  @Test
  public void testPrimitiveSplits() {
    testPrimitiveNoSplits(0, new byte[] { 'a' }, new byte[] { 'a' } );
    testPrimitiveNoSplits(0, new byte[] { 'c' }, new byte[] { 'b' } );
    testPrimitiveNoSplits(1, new byte[] { 'a' }, new byte[] { 'a' } );
    testPrimitiveNoSplits(1000, new byte[] { 'c' }, new byte[] { 'b' } );
    testPrimitiveSplits(0, null, null);
    testPrimitiveSplits(1, null, null);
    testPrimitiveSplits(24, null, null);
    testPrimitiveSplits(0, null, new byte[] { (byte)0x80 });
    testPrimitiveSplits(16, null, new byte[] { (byte)0x80 });
    testPrimitiveSplits(0, new byte[] { (byte)0x80 }, null);
    testPrimitiveSplits(1, new byte[] { (byte)0x80 }, null);
    testPrimitiveSplits(5, new byte[] { (byte)0x80 }, null);
    testPrimitiveSplits(0, new byte[] { 'A', 'B', 'C' }, new byte[] { 'a', 'b', 'c' });
    testPrimitiveSplits(1, new byte[] { 'A', 'B', 'C' }, new byte[] { 'a', 'b', 'c' });
    testPrimitiveSplits(10, new byte[] { 'A', 'B', 'C' }, new byte[] { 'a', 'b', 'c' });
    testPrimitiveSplits(0, new byte[] { 'A', 'B', 'C' }, new byte[] { 'B', '1', '2' });
    testPrimitiveSplits(1, new byte[] { 'A', 'B', 'C' }, new byte[] { 'B', '1', '2' });
    testPrimitiveSplits(10, new byte[] { 'A', 'B', 'C' }, new byte[] { 'B', '1', '2' });
    testPrimitiveSplits(10, Bytes.toBytes(1L), Bytes.toBytes(2L));
  }
}
