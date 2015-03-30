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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.hbase.wd.AbstractRowKeyDistributor;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class HBaseTableUtilTest {
  @Test
  public void testGetSplitKeys() {
    int buckets = 16;
    AbstractRowKeyDistributor distributor = new RowKeyDistributorByHashPrefix(
      new RowKeyDistributorByHashPrefix.OneByteSimpleHash(buckets));

    // Number of splits will be no less than user asked. If splits > buckets, the number of splits will bumped to
    // next multiple of bucket that is no less than user splits requested.
    // it should return one key less than required splits count, because HBase will take care of the first automatically
    Assert.assertEquals(getSplitSize(buckets, 12) - 1, HBaseTableUtil.getSplitKeys(12, buckets, distributor).length);
    Assert.assertEquals(getSplitSize(buckets, 16) - 1, HBaseTableUtil.getSplitKeys(16, buckets, distributor).length);
    // at least #buckets - 1, but no less than user asked
    Assert.assertEquals(buckets - 1, HBaseTableUtil.getSplitKeys(6, buckets, distributor).length);
    Assert.assertEquals(buckets - 1, HBaseTableUtil.getSplitKeys(2, buckets, distributor).length);
    // "1" can be used for queue tables that we know are not "hot", so we do not pre-split in this case
    Assert.assertEquals(0, HBaseTableUtil.getSplitKeys(1, buckets, distributor).length);
    // allows up to 255 * 8 - 1 splits
    Assert.assertEquals(255 * buckets - 1, HBaseTableUtil.getSplitKeys(255 * buckets, buckets, distributor).length);
    try {
      HBaseTableUtil.getSplitKeys(256 * buckets, buckets, distributor);
      Assert.fail("getSplitKeys(256) should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      HBaseTableUtil.getSplitKeys(0, buckets, distributor);
      Assert.fail("getSplitKeys(0) should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  private int getSplitSize(int buckets, int splits) {
    return ((splits - 1) / buckets + 1) * buckets;
  }
}
