package com.continuuity.data2.util.hbase;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class HBaseTableUtilTest {
  @Test
  public void testGetSplitKeys() {
    // it should return one key less than required splits count, because HBase will take care of the first automatically
    Assert.assertEquals(19, HBaseTableUtil.getSplitKeys(20).length);
    Assert.assertEquals(5, HBaseTableUtil.getSplitKeys(6).length);
    Assert.assertEquals(254, HBaseTableUtil.getSplitKeys(255).length);
    Assert.assertEquals(15, HBaseTableUtil.getSplitKeys(16).length);
    Assert.assertEquals(1, HBaseTableUtil.getSplitKeys(2).length);
    Assert.assertEquals(0, HBaseTableUtil.getSplitKeys(1).length);
    try {
      HBaseTableUtil.getSplitKeys(256);
      Assert.fail("getSplitKeys(256) should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      HBaseTableUtil.getSplitKeys(0);
      Assert.fail("getSplitKeys(0) should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
