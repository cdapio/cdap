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

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A basic implementation of LRU cache. It uses a map to store cache values and a priority queue for tracking
 * last used time. The implementation only use pure Java class. Due to the limitation of the Java
 * priority queue of not having support for updating priority without removing/adding element, the implementation
 * is O(n) to the number of entries in the cache, hence it is more small cache (n < 100).
 *
 * This class is thread safe.
 *
 * @param <K> type of the key
 * @param <V> type of the value
 */
@ThreadSafe
final class LRUCache<K, V> {

  private final Map<K, V> entries;
  private final PriorityQueue<TimedKey<K>> orderedKeys;
  private final int maxSize;

  /**
   * Creates a {@link LRUCache} that caches the given maximum number of entries.
   *
   * @param maxSize maximum cache size
   */
  LRUCache(int maxSize) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException("Maximum size of LRU Cache must be > 0");
    }

    this.entries = new HashMap<>();
    this.orderedKeys = new PriorityQueue<>();
    this.maxSize = maxSize;
  }

  /**
   * Puts a value for a given key in the cache if the key is absent in the cache.
   *
   * @param key the key for lookup in the cache
   * @param value the value to put in the cache
   * @return either the existing value or the new value
   */
  synchronized V putIfAbsent(K key, V value) {
    V oldValue = entries.putIfAbsent(key, value);
    if (oldValue != null) {
      orderedKeys.removeIf(k -> k.getKey().equals(key));
    }
    orderedKeys.add(new TimedKey<>(key));
    if (orderedKeys.size() > maxSize) {
      entries.remove(orderedKeys.remove().getKey());
    }
    return oldValue == null ? value : oldValue;
  }

  /**
   * Returns the value associated with the given key.
   *
   * @param key the key for lookup in the cache
   * @return the associated value or {@code null} if the key is absent in the cache.
   */
  @Nullable
  synchronized V get(K key) {
    if (entries.containsKey(key)) {
      orderedKeys.removeIf(k -> k.getKey().equals(key));
      orderedKeys.add(new TimedKey<>(key));
    }
    return entries.get(key);
  }

  /**
   * Clear the cache.
   */
  synchronized void clear() {
    entries.clear();
    orderedKeys.clear();
  }

  /**
   * Class for tracking last used time.
   *
   * @param <K> type of the key
   */
  private static final class TimedKey<K> implements Comparable<TimedKey<K>> {

    private final long time;
    private final K key;

    TimedKey(K key) {
      this.time = System.nanoTime();
      this.key = key;
    }

    K getKey() {
      return key;
    }

    @Override
    public int compareTo(TimedKey<K> o) {
      return Long.compare(time, o.time);
    }
  }
}
