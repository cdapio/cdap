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
import com.google.common.base.Throwables;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class MultiThreadDatasetCacheTest extends DynamicDatasetCacheTest {

  @Override
  protected DynamicDatasetCache createCache(SystemDatasetInstantiator instantiator,
                                            Map<String, String> arguments,
                                            Map<String, Map<String, String>> staticDatasets) {
    return new MultiThreadDatasetCache(instantiator, txClient, NAMESPACE, arguments, null, staticDatasets);
  }

  @Test()
  public void testDatasetCache() throws Exception {

    Map<String, TestDataset> thread1map = new HashMap<>();
    Map<String, TestDataset> thread2map = new HashMap<>();

    Thread thread1 = createThread(thread1map);
    Thread thread2 = createThread(thread2map);
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

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

  private Thread createThread(final Map<String, TestDataset> datasetMap) {
    return new Thread() {
      @Override
      public void run() {
        try {
          testDatasetCache(datasetMap);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }
}
