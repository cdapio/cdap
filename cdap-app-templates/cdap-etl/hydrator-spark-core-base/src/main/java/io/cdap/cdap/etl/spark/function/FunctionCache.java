/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.function;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>This cache allows to reuse objects that are created on executor side and have high initialization. By default
 * spark deserializes new task for each partition, so if a single executor processes a lot of partitions it performs
 * a lot of unnecesary work initializing all the plugins for every partition. This cache solves the problem.</p>
 * <p>It works in the next fashion:
 * </p>
 * <ol>
 *  <li>Within a driver one creates a factory that can make cache instance.</li>
 *  <li>A separate cache instance must be created by for each value one wants to cache by calling 
 *  {@link Factory#newCache()}. Each cache internally gets an unique id that would later be used to find objects in 
 *  cache.</li>
 *  <li>The cache can now be serialized and sent over to executors</li>
 *  <li>When one needs to get a value it calls {@link #getValue} passing loader.</li>
 *  <li>During first call for a thread a loader is invoked and must create the value to be used later</li>
 *  <li>If the value was already created for the current thread it's returned without invoking a loader</li>
 * </ol>
 * <p>Note that values are reused only within a single thread. This ensures such caching won't provoke any concurrency
 * issues. It also works good with spark since it uses thread pool to run tasks.
 * </p>
 * <p>Also all values are stored as weak references. It ensures that if value is not used anymore and there is 
 * memory pressure it will be garbage collected.
 * </p>
 * <p>Important: One must not reuse caches for different values! New instance of cache must be created for each value
 * </p>
 * @see PluginFunctionContext#createAndInitializePlugin 
 */
public class FunctionCache implements Serializable {
  private static final ThreadLocal<Cache<String, Object>> VALUES_CACHE = ThreadLocal.withInitial(
    () -> CacheBuilder.newBuilder().softValues().build());

  /**
   * Defines unique key that allows to match values between partition tasks. It consist of factory key and
   * value number.
   */
  private final String key;

  private FunctionCache(String key) {
    this.key = key;
  }

  /**
   * Gets value from a thread-local cache or constructs it using loader.
   * @param loader function to produce a value when it's not present in cache
   * @param <T> value type
   * @throws ExecutionException
   */
  public <T> T getValue(Callable<T> loader) throws ExecutionException {
    T rc = (T) ((Cache) VALUES_CACHE.get()).get(key, loader);
    return rc;
  }

  /**
   * Factory class allows to create cache instances.
   */
  public static class Factory {
    private static final String KEY_SEPARATOR = ":";
    /**
     * Key that should help to distinguish factories from different processed in the hypotetical case of constructing
     * caches within different JVMs. Be default we should do it in driver only, so it's more of a precation measure.
     */
    private static final String PROCESS_KEY = UUID.randomUUID().toString();
    /**
     * Value that is used to differentiate factories created in same JVM.
     */
    private static final AtomicLong FACTORY_COUNTER = new AtomicLong();
    /**
     * Factory key
     */
    private final String key;
    /**
     * Value that is used to differenciate caches produces by a single factory.
     */
    private final AtomicInteger counter = new AtomicInteger();

    private Factory(String key) {
      this.key = key;
    }

    /**
     *
     * @return new instance of cache factory with unique internal id
     */
    public static Factory newInstance() {
      return new Factory(PROCESS_KEY + KEY_SEPARATOR + FACTORY_COUNTER.incrementAndGet());
    }

    /**
     *
     * @return a cache object that can cache a single value and can be serialized.
     */
    public FunctionCache newCache() {
      return new FunctionCache(key + KEY_SEPARATOR + counter.getAndIncrement());
    }

  }
}
