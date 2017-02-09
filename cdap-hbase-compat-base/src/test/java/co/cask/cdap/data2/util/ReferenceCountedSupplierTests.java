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

package co.cask.cdap.data2.util;

import co.cask.cdap.data2.transaction.coprocessor.CConfigurationCacheSupplier;
import co.cask.cdap.data2.transaction.coprocessor.CacheSupplier;
import co.cask.cdap.messaging.TopicMetadataCacheSupplier;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests {@link ReferenceCountedSupplier}s.
 */
public class ReferenceCountedSupplierTests {
  private static final Log LOG = LogFactory.getLog(ReferenceCountedSupplierTests.class);
  private static final int NUM_OPS = 100;
  private static final int NUM_THREADS = 5;

  @Test
  public void testSupplier() throws Exception {
    // ReferenceCountSupplier is created only once per CacheSupplier class. Thus it is fine to create separate
    // CacheSupplier objects
    CacheSupplier cConfSupplier = new CConfigurationCacheSupplier(null, null);
    testGetSupplier(Lists.newArrayList(cConfSupplier));
    testReleaseSupplier(Lists.newArrayList(cConfSupplier));

    // Test to make sure we handle more releases than gets.
    for (int i = 0; i < 10; i++) {
      cConfSupplier.release();
    }

    // Test increase and decrease of counts for cConf supplier
    testGetSupplier(Lists.<CacheSupplier>newArrayList(new CConfigurationCacheSupplier(null, null)));
    testReleaseSupplier(Lists.<CacheSupplier>newArrayList(new CConfigurationCacheSupplier(null, null)));

    // Repeat the tests for TopicMetadata CacheSupplier
    testGetSupplier(Lists.<CacheSupplier>newArrayList(new TopicMetadataCacheSupplier(null, null, null, null, null)));
    testReleaseSupplier(Lists.<CacheSupplier>newArrayList(
      new TopicMetadataCacheSupplier(null, null, null, null, null)));
    testGetSupplier(Lists.<CacheSupplier>newArrayList(new TopicMetadataCacheSupplier(null, null, null, null, null)));
    testReleaseSupplier(Lists.<CacheSupplier>newArrayList(
      new TopicMetadataCacheSupplier(null, null, null, null, null)));

    // Tests multiple suppliers at the same time
    List<CacheSupplier> cacheSupplierList = new ArrayList<>();
    cacheSupplierList.add(new CConfigurationCacheSupplier(null, null));
    cacheSupplierList.add(new TopicMetadataCacheSupplier(null, null, null, null, null));
    testGetSupplier(cacheSupplierList);
    testReleaseSupplier(cacheSupplierList);
  }

  /**
   * Starts a few threads to perform get operation on the given {@link CacheSupplier}s. It verifies that subsequent
   * 'get' operation returns the same instance as it did during the first invocation. And it also verifies that the
   * service is running.
   *
   * @param supplierList list of {@link CacheSupplier}
   * @throws Exception if an error occurs during testing
   */
  private void testGetSupplier(final List<CacheSupplier> supplierList) throws Exception {
    // Get one instance now, for later comparisons
    final List<Service> serviceList = new ArrayList<>();
    for (CacheSupplier supplier : supplierList) {
      serviceList.add(supplier.get());
    }

    final AtomicInteger numOps = new AtomicInteger(NUM_OPS);
    final Random random = new Random(System.currentTimeMillis());

    // Start threads that will 'get' DummyService
    ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    List<Future> futureList = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; i++) {
      futureList.add(executor.submit(new Runnable() {

        @Override
        public void run() {
          // Perform NUM_OPS 'gets' of DummyService
          while (numOps.decrementAndGet() > 0) {
            for (int i = 0; i < supplierList.size(); i++) {
              CacheSupplier supplier = supplierList.get(i);
              Service newService = supplier.get();
              Assert.assertTrue(newService == serviceList.get(i));
              Assert.assertTrue(newService.isRunning());
            }
            int waitTime = random.nextInt(10);
            try {
              TimeUnit.MICROSECONDS.sleep(waitTime);
            } catch (InterruptedException e) {
              LOG.warn("Received an exception.", e);
            }
          }
        }
      }));
    }

    for (Future future : futureList) {
      future.get(5, TimeUnit.SECONDS);
    }
    executor.shutdown();
    executor.awaitTermination(2, TimeUnit.SECONDS);
  }

  /**
   * Starts a few threads to perform release operation on the given {@link CacheSupplier}s. It verifies that the
   * services are still running until the last release operation is performed after which they are checked to see
   * if the services have stopped.
   *
   * @param supplierList list of {@link CacheSupplier}
   * @throws Exception if an error occurs during testing
   */
  private void testReleaseSupplier(final List<CacheSupplier> supplierList) throws Exception {
    final AtomicInteger numOps = new AtomicInteger(NUM_OPS);
    final Random random = new Random(System.currentTimeMillis());

    final List<Service> serviceList = new ArrayList<>();
    for (CacheSupplier supplier : supplierList) {
      serviceList.add(supplier.get());
      supplier.release();
    }

    // Start threads that will 'release' DummyService
    ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    List<Future> futureList = new ArrayList<>();

    numOps.set(NUM_OPS);
    for (int i = 0; i < NUM_THREADS; i++) {
      futureList.add(executor.submit(new Runnable() {

        @Override
        public void run() {
          // We need to release all NUM_OPS 'gets' that were executed to trigger shutdown of the single instance of
          // DummyService
          while (numOps.decrementAndGet() > 0) {
            for (int i = 0; i < supplierList.size(); i++) {
              CacheSupplier supplier = supplierList.get(i);
              supplier.release();
              Assert.assertTrue(serviceList.get(i).isRunning());
            }
            try {
              TimeUnit.MICROSECONDS.sleep(random.nextInt(10));
            } catch (InterruptedException e) {
              LOG.warn("Received an exception.", e);
            }
          }
        }
      }));
    }

    for (Future future : futureList) {
      future.get(1, TimeUnit.SECONDS);
    }

    executor.shutdown();
    executor.awaitTermination(2, TimeUnit.SECONDS);

    // Verify that the DummyService is still running.
    for (Service service : serviceList) {
      Assert.assertTrue(service.isRunning());
    }

    // Since we got one instance in the beginning, we need to release it
    for (CacheSupplier supplier : supplierList) {
      supplier.release();
    }

    // Verify that the DummyService is shutdown.
    for (Service service : serviceList) {
      Assert.assertFalse(service.isRunning());
    }

    // Release it again but it should not cause any problem and the service should still be stopped.
    for (CacheSupplier supplier : supplierList) {
      supplier.release();
    }

    // Verify that the DummyService is shutdown.
    for (Service service : serviceList) {
      Assert.assertFalse(service.isRunning());
    }
  }
}
