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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * {@link Lookup} that provides caching over a delegate.
 *
 * @param <T> the type of object that will be returned for a lookup
 */
public class CachingLookup<T> implements Lookup<T> {

  private final Lookup<T> delegate;
  private final LoadingCache<String, T> cache;

  public CachingLookup(final Lookup<T> delegate, CacheConfig cacheConfig) {
    this.delegate = delegate;
    this.cache = CacheBuilder.newBuilder()
      .maximumSize(cacheConfig.getMaxSize())
      .expireAfterWrite(cacheConfig.getExpirySeconds(), TimeUnit.SECONDS)
      .build(new CacheLoader<String, T>() {
        @Override
        public T load(String key) throws Exception {
          return delegate.lookup(key);
        }
      });
  }

  @Override
  public T lookup(String key) {
    return cache.getUnchecked(key);
  }

  @Override
  public Map<String, T> lookup(String... keys) {
    return lookup(ImmutableSet.copyOf(keys));
  }

  @Override
  public Map<String, T> lookup(Set<String> keys) {
    ImmutableMap<String, T> cached = cache.getAllPresent(keys);

    Set<String> missingKeys = Sets.difference(keys, cached.keySet());
    Map<String, T> missing = delegate.lookup(missingKeys);
    cache.putAll(missing);

    return ImmutableMap.<String, T>builder()
      .putAll(cached)
      .putAll(missing)
      .build();
  }
}
