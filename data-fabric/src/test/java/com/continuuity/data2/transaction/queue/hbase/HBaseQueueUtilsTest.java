package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.api.common.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class HBaseQueueUtilsTest {
  @Test
  public void testGetTwoDigitHex() {
    // Checking that keys ordered lexographycally
    // NOTE: this is important to make equal splits in HBase as row keys ordered lexographicaly
    for (int i = 0; i < 16 * 16 - 2; i++) {
      Assert.assertTrue(Bytes.compareTo(HBaseQueueUtils.getTwoDigitHex(i), HBaseQueueUtils.getTwoDigitHex(i + 1)) < 0);
    }

    Assert.assertEquals("0b", Bytes.toStringBinary(HBaseQueueUtils.getTwoDigitHex(11)));
    Assert.assertEquals("74", Bytes.toStringBinary(HBaseQueueUtils.getTwoDigitHex(7 * 16 + 4)));
    Assert.assertEquals("c0", Bytes.toStringBinary(HBaseQueueUtils.getTwoDigitHex(12 * 16)));
  }

  @Test
  public void testGetSplitKeys() {
    // it should return one key less than required splits count, because HBase will take care of the first automatically
    Assert.assertEquals(19, HBaseQueueUtils.getSplitKeys(20).length);
    Assert.assertEquals(5, HBaseQueueUtils.getSplitKeys(6).length);
    Assert.assertEquals(254, HBaseQueueUtils.getSplitKeys(255).length);
    byte[][] for2 = HBaseQueueUtils.getSplitKeys(2);
    Assert.assertEquals(1, for2.length);
    Assert.assertEquals("80", Bytes.toStringBinary(for2[0]));
    try {
      HBaseQueueUtils.getSplitKeys(256);
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      HBaseQueueUtils.getSplitKeys(1);
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      // expected
    }

    // 16 is default, hence if picking what to check for exact match, it makes sense to verify the default case
    byte[][] for16 = HBaseQueueUtils.getSplitKeys(16);
    Assert.assertEquals(15, for16.length);
    Assert.assertEquals("10", Bytes.toStringBinary(for16[0]));
    Assert.assertEquals("20", Bytes.toStringBinary(for16[1]));
    Assert.assertEquals("30", Bytes.toStringBinary(for16[2]));
    Assert.assertEquals("40", Bytes.toStringBinary(for16[3]));
    Assert.assertEquals("50", Bytes.toStringBinary(for16[4]));
    Assert.assertEquals("60", Bytes.toStringBinary(for16[5]));
    Assert.assertEquals("70", Bytes.toStringBinary(for16[6]));
    Assert.assertEquals("80", Bytes.toStringBinary(for16[7]));
    Assert.assertEquals("90", Bytes.toStringBinary(for16[8]));
    Assert.assertEquals("a0", Bytes.toStringBinary(for16[9]));
    Assert.assertEquals("b0", Bytes.toStringBinary(for16[10]));
    Assert.assertEquals("c0", Bytes.toStringBinary(for16[11]));
    Assert.assertEquals("d0", Bytes.toStringBinary(for16[12]));
    Assert.assertEquals("e0", Bytes.toStringBinary(for16[13]));
    Assert.assertEquals("f0", Bytes.toStringBinary(for16[14]));
  }
}
