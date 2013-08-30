package com.continuuity.data2.transaction.queue.hbase;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class HBaseQueueUtilsTest {
  @Test
  public void testGetSplitKeys() {
    // it should return one key less than required splits count, because HBase will take care of the first automatically
    Assert.assertEquals(19, HBaseQueueUtils.getSplitKeys(20).length);
    Assert.assertEquals(5, HBaseQueueUtils.getSplitKeys(6).length);
    Assert.assertEquals(254, HBaseQueueUtils.getSplitKeys(255).length);
    Assert.assertEquals(15, HBaseQueueUtils.getSplitKeys(16).length);
    Assert.assertEquals(1, HBaseQueueUtils.getSplitKeys(2).length);
    Assert.assertEquals(0, HBaseQueueUtils.getSplitKeys(1).length);
    try {
      HBaseQueueUtils.getSplitKeys(256);
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      HBaseQueueUtils.getSplitKeys(0);
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
