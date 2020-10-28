/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.classloader;

import com.google.common.io.Closeables;
import io.cdap.cdap.common.lang.ClassLoaders;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
                                                                  getClass().getClassLoader(), false, false);
      // Load a class from the first CL.
      firstCL.loadClass("org.apache.spark.SparkContext");

      // Create a thread to load the same class name from a different CL instance
      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicReference<Exception> exception = new AtomicReference<>();
      Thread t = new Thread() {
        @Override
        public void run() {
          SparkRunnerClassLoader secondCL = new SparkRunnerClassLoader(urlArray,
                                                                       getClass().getClassLoader(),
                                                                       false,
                                                                       false);
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

  @Test
  public void testConcurrentLoadClass() throws Exception {
    // This test the fix for CDAP-13598 that loading the same class concurrently from the same classloader is
    // synchronized correctly.
    ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      // Try it 100 times since the error comes from race condition of multiple threads loading the same class
      for (int i = 0; i < 100; i++) {
        List<URL> urls = ClassLoaders.getClassLoaderURLs(getClass().getClassLoader(), new ArrayList<URL>());
        final URL[] urlArray = urls.toArray(new URL[urls.size()]);

        SparkRunnerClassLoader classLoader = new SparkRunnerClassLoader(urlArray,
                                                                        getClass().getClassLoader(),
                                                                        false,
                                                                        false);

        // Load the same class concurrently from two threads.
        CyclicBarrier barrier = new CyclicBarrier(2);
        String className = "org.apache.spark.SparkContext";

        CompletionService<?> completionService = new ExecutorCompletionService<>(executor);

        for (int j = 0; j < 2; j++) {
          completionService.submit(() -> {
            barrier.await();
            classLoader.loadClass(className);
            return null;
          });
        }

        for (int j = 0; j < 2; j++) {
          // Shouldn't throw exception
          completionService.take().get();
        }
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(expected = IOException.class)
  public void testCloseResourceStream() throws IOException {
    List<URL> urls = ClassLoaders.getClassLoaderURLs(getClass().getClassLoader(), new ArrayList<URL>());
    URL[] urlArray = urls.toArray(new URL[urls.size()]);

    SparkRunnerClassLoader cl = new SparkRunnerClassLoader(urlArray, getClass().getClassLoader(), false, false);
    InputStream is = cl.getResourceAsStream("org/apache/spark/SparkContext.class");
    Assert.assertNotNull(is);

    // After closing the ClassLoader,
    // reading from stream acquired through getResourceAsStream() should fail with an exception.
    Closeables.closeQuietly(cl);
    is.read();
  }
}
