/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.transform;

import co.cask.cdap.etl.api.CacheConfig;
import co.cask.cdap.etl.api.Lookup;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class CachingLookupTest {

  @Test
  public void testExpiryByTime() throws InterruptedException {
    Map<String, String> backing = new HashMap<>();
    backing.put("foo", "1");

    Lookup<String> delegate = new MapLookup<>(backing);
    CacheConfig config = new CacheConfig(1, 10);
    CachingLookup<String> lookup = new CachingLookup<>(delegate, config);

    Assert.assertEquals("1", lookup.lookup("foo"));

    backing.put("foo", "2");
    Assert.assertEquals("1", lookup.lookup("foo"));
    Thread.sleep(1100);
    Assert.assertEquals("2", lookup.lookup("foo"));
  }

  @Test
  public void testExpiryByFull() {
    Map<String, String> backing = new HashMap<>();
    for (int i = 1; i <= 100; i++) {
      backing.put("foo" + i, Integer.toString(i));
    }

    Lookup<String> delegate = new MapLookup<>(backing);
    CacheConfig config = new CacheConfig(0, 10);
    CachingLookup<String> lookup = new CachingLookup<>(delegate, config);

    Assert.assertEquals("1", lookup.lookup("foo1"));

    // fill the cache and check that the first values cached are removed
    for (int i = 1; i <= 100; i++) {
      Assert.assertEquals(Integer.toString(i), lookup.lookup("foo" + i));
    }

    backing.put("foo1", "sdf");
    Assert.assertEquals("sdf", lookup.lookup("foo1"));
  }

  @Test
  public void testBatch() throws InterruptedException {
    Map<String, String> backing = new HashMap<>();
    for (int i = 1; i <= 100; i++) {
      backing.put("foo" + i, Integer.toString(i));
    }

    Lookup<String> delegate = new MapLookup<>(backing);
    CacheConfig config = new CacheConfig(1, 10);
    CachingLookup<String> lookup = new CachingLookup<>(delegate, config);

    Assert.assertEquals(
      ImmutableMap.of("foo1", "1", "foo2", "2", "foo4", "4"),
      lookup.lookup("foo1", "foo4", "foo2"));

    backing.put("foo2", "sdf");
    Assert.assertEquals(
      ImmutableMap.of("foo1", "1", "foo2", "2", "foo4", "4"),
      lookup.lookup("foo1", "foo4", "foo2"));
    Thread.sleep(1100);
    Assert.assertEquals(
      ImmutableMap.of("foo1", "1", "foo2", "sdf", "foo4", "4"),
      lookup.lookup("foo1", "foo4", "foo2"));
  }

  private static class MapLookup<T> implements Lookup<T> {

    private final Map<String, T> backing;

    public MapLookup(Map<String, T> backing) {
      this.backing = backing;
    }

    @Override
    public T lookup(String key) {
      return backing.get(key);
    }

    @Override
    public Map<String, T> lookup(String... keys) {
      return lookup(ImmutableSet.copyOf(keys));
    }

    @Override
    public Map<String, T> lookup(Set<String> keys) {
      return Maps.filterKeys(backing, Predicates.in(keys));
    }
  }

}
