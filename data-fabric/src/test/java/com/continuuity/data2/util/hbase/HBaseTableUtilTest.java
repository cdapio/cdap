package com.continuuity.data2.util.hbase;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class HBaseTableUtilTest {
  @Test
  public void testGetSplitKeys() {
    Assert.assertEquals(15, HBaseTableUtil.getSplitKeys(12).length);
    // it should return one key less than required splits count, because HBase will take care of the first automatically
    Assert.assertEquals(15, HBaseTableUtil.getSplitKeys(16).length);
    // at least #buckets - 1, but no less than user asked
    Assert.assertEquals(7, HBaseTableUtil.getSplitKeys(6).length);
    Assert.assertEquals(7, HBaseTableUtil.getSplitKeys(2).length);
    // "1" can be used for queue tables that we know are not "hot", so we do not pre-split in this case
    Assert.assertEquals(0, HBaseTableUtil.getSplitKeys(1).length);
    // allows up to 255 * 8 - 1 splits
    Assert.assertEquals(255 * 8 - 1, HBaseTableUtil.getSplitKeys(255 * 8).length);
    try {
      HBaseTableUtil.getSplitKeys(256 * 8);
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
