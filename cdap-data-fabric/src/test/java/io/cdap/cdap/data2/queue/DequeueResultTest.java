/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.data2.queue;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link DequeueResult.Empty}.
 */
public class DequeueResultTest {

  @Test
  public void testEmptyResultIteratorAndOperations() {
    DequeueResult<byte[]> empty = DequeueResult.Empty.result();
    Assert.assertTrue(empty.isEmpty());
    Assert.assertEquals(0, empty.size());
    empty.reclaim();

    Iterator<byte[]> iterator = empty.iterator();
    Assert.assertFalse(iterator.hasNext());
    try {
      iterator.next();
      Assert.fail("Expected NoSuchElementException");
    } catch (NoSuchElementException expected) {
      // expected
    }
  }
}
