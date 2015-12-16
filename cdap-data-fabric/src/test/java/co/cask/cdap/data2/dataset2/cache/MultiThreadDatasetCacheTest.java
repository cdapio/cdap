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

package co.cask.cdap.data2.dataset2.cache;

import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class MultiThreadDatasetCacheTest extends DynamicDatasetCacheTest {

  @Override
  protected DynamicDatasetCache createCache(SystemDatasetInstantiator instantiator,
                                            Map<String, String> arguments,
                                            Map<String, Map<String, String>> staticDatasets) {
    return new MultiThreadDatasetCache(instantiator, txClient, NAMESPACE_ID, arguments, null, staticDatasets);
  }

  @Test
  public void testDatasetCache() throws Throwable {
    for (int i = 0; i < 25; i++) {
      testDatasetCacheOnce();
    }
  }

  private void testDatasetCacheOnce() throws Throwable {

    Map<String, TestDataset> thread1map = new HashMap<>();
    Map<String, TestDataset> thread2map = new HashMap<>();

    AtomicReference<Throwable> exceptionFromThread1 = new AtomicReference<>();
    AtomicReference<Throwable> exceptionFromThread2 = new AtomicReference<>();

    Thread thread1 = createThread(thread1map, exceptionFromThread1);
    Thread thread2 = createThread(thread2map, exceptionFromThread2);
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    // validate that both threads were successful
    assertNoError(exceptionFromThread1);
    assertNoError(exceptionFromThread2);

    // validate that the threads successfully received non-null and different instances of the datasets
    Assert.assertNotNull(thread1map.get("a"));
    Assert.assertNotNull(thread1map.get("b"));
    Assert.assertNotSame(thread1map.get("a"), thread2map.get("a"));
    Assert.assertNotSame(thread1map.get("b"), thread2map.get("b"));

    // we want to test that the per-thread entries get removed when the thread goes away. Unfortunately there
    // is not reliable way to force GC to collect a weak reference (which is required to trigger removal from
    // the cache. The following code works every time when I run it manually.

    //noinspection UnusedAssignment
    thread1 = thread2 = null;
    //noinspection UnusedAssignment
    thread1map = thread2map = null;

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        System.gc();
        return ((MultiThreadDatasetCache) cache).getCacheKeys().isEmpty();
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  private Thread createThread(final Map<String, TestDataset> datasetMap, final AtomicReference<Throwable> ref) {
    return new Thread() {
      @Override
      public void run() {
        try {
          testDatasetCache(datasetMap);
        } catch (Throwable e) {
          ref.set(e);
        }
      }
    };
  }

  private void assertNoError(AtomicReference<Throwable> ref) throws Throwable {
    Throwable t = ref.get();
    if (t != null) {
      throw t;
    }
  }
}
