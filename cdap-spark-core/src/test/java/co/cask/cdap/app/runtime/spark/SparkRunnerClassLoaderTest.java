/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.common.lang.ClassLoaders;
import com.google.common.io.Closeables;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link SparkRunnerClassLoader}.
 */
public class SparkRunnerClassLoaderTest {

  @Test
  public void testConcurrentLoadClose() throws Exception {
    // This is for testing CDAP-5822, which apparently is a JDK bug.
    // Since the problem manifest as a race between loading class and closing of another ClassLoader instance,
    // we loop 100 times, which give a very high chance that the test would fail when the bug was not fixed.
    for (int i = 0; i < 100; i++) {
      List<URL> urls = ClassLoaders.getClassLoaderURLs(getClass().getClassLoader(), new ArrayList<URL>());
      final URL[] urlArray = urls.toArray(new URL[urls.size()]);

      SparkRunnerClassLoader firstCL = new SparkRunnerClassLoader(urlArray,
                                                                  getClass().getClassLoader(), false);
      // Load a class from the first CL.
      firstCL.loadClass("org.apache.spark.SparkContext");

      // Create a thread to load the same class name from a different CL instance
      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicReference<Exception> exception = new AtomicReference<>();
      Thread t = new Thread() {
        @Override
        public void run() {
          SparkRunnerClassLoader secondCL = new SparkRunnerClassLoader(urlArray,
                                                                       getClass().getClassLoader(), false);
          try {
            latch.countDown();
            secondCL.loadClass("org.apache.spark.SparkContext");
          } catch (Exception e) {
            exception.set(e);
          } finally {
            Closeables.closeQuietly(secondCL);
          }
        }
      };
      t.start();

      // When the thread started to load the class, close the first ClassLoader
      latch.await();
      firstCL.close();
      t.join();

      // There shouldn't be any exception raised from the thread.
      Exception ex = exception.get();
      if (ex != null) {
        throw ex;
      }
    }
  }
}
