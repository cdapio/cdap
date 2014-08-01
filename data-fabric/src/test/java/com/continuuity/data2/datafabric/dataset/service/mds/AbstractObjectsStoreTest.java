/*
 * Copyright 2012-2014 Continuuity, Inc.
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
