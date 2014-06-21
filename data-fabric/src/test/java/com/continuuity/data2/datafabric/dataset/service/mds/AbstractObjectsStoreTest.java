package com.continuuity.data2.datafabric.dataset.service.mds;

import org.junit.Assert;
import org.junit.Test;

/**
 * Testcase for {@link com.continuuity.data2.datafabric.dataset.service.mds.AbstractObjectsStore}
 */
public class AbstractObjectsStoreTest {
  @Test
  public void testCreateStopKey() {
    Assert.assertArrayEquals(new byte[] {1, 2, 3},
                             AbstractObjectsStore.createStopKey(new byte[]{1, 2, 2}));
    Assert.assertArrayEquals(new byte[]{1, 3},
                             AbstractObjectsStore.createStopKey(new byte[] {1, 2, (byte) 255}));
    // "read to the end" case
    Assert.assertNull(AbstractObjectsStore.createStopKey(new byte[] {(byte) 255, (byte) 255}));
  }
}
