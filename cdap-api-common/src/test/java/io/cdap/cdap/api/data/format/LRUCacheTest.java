/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.api.data.format;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for the {@link LRUCache} class.
 */
public class LRUCacheTest {

  @Test
  public void testCache() {
    LRUCache<String, Integer> cache = new LRUCache<>(1);
    Assert.assertEquals(Integer.valueOf(1), cache.putIfAbsent("key1", 1));
    Assert.assertEquals(Integer.valueOf(2), cache.putIfAbsent("key2", 2));

    Assert.assertNull(cache.get("key1"));
    Assert.assertEquals(Integer.valueOf(2), cache.get("key2"));
  }

  @Test
  public void testLastAccess() {
    LRUCache<String, Integer> cache = new LRUCache<>(2);

    Assert.assertEquals(Integer.valueOf(1), cache.putIfAbsent("key1", 1));
    Assert.assertEquals(Integer.valueOf(2), cache.putIfAbsent("key2", 2));

    // Have key1 accessed after key2
    Assert.assertEquals(Integer.valueOf(1), cache.get("key1"));

    // Adding a new key would push out key2
    Assert.assertEquals(Integer.valueOf(3), cache.putIfAbsent("key3", 3));
    Assert.assertEquals(Integer.valueOf(1), cache.get("key1"));
    Assert.assertNull(cache.get("key2"));

    cache.clear();

    Assert.assertNull(cache.get("key1"));
    Assert.assertNull(cache.get("key2"));
    Assert.assertNull(cache.get("key3"));
  }
}
