/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.common.async;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Unit test for {@link KeyedExecutor}.
 */
public class KeyedExecutorTest {

  @Test
  public void testRunnable() throws Exception {
    KeyedExecutor<String> executor = new KeyedExecutor<>(Executors.newScheduledThreadPool(0));
    try {
      CountDownLatch runLatch = new CountDownLatch(1);
      Future<Void> future1 = executor.execute("1", () -> Uninterruptibles.awaitUninterruptibly(runLatch));

      // Submit again with the same key should return the same future
      Future<Void> future2 = executor.execute("1", () -> Uninterruptibles.awaitUninterruptibly(runLatch));

      Assert.assertSame(future1, future2);

      // Let the latch pass to complete the task
      runLatch.countDown();

      // Shouldn't have any exception in the future
      future1.get(5, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testKey() throws Exception {
    KeyedExecutor<String> executor = new KeyedExecutor<>(Executors.newScheduledThreadPool(0));
    try {
      Semaphore semaphore = new Semaphore(0);
      Future<Void> future1 = executor.execute("1", semaphore::acquireUninterruptibly);

      // Submit with different key
      Future<Void> future2 = executor.execute("2", semaphore::acquireUninterruptibly);

      Assert.assertNotSame(future1, future2);

      // Release to complete both tasks
      semaphore.release(2);

      future1.get(5, TimeUnit.SECONDS);
      future2.get(5, TimeUnit.SECONDS);

      // Submit a new task with the same key. It should return a new future.
      Future<Void> newFuture = executor.execute("1", semaphore::acquireUninterruptibly);
      Assert.assertNotSame(future1, newFuture);

      try {
        newFuture.get(2, TimeUnit.SECONDS);
        Assert.fail("Expected timeout exception");
      } catch (TimeoutException e) {
        // expected
      }

      // The getFuture should return the latest future
      Assert.assertSame(newFuture, executor.getFuture("1").orElse(null));

      // Release to complete the task
      semaphore.release(1);
      newFuture.get(5, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testRepeatedTask() throws Exception {
    KeyedExecutor<String> executor = new KeyedExecutor<>(Executors.newScheduledThreadPool(0));
    try {
      BlockingQueue<String> queue = new LinkedBlockingQueue<>();
      Future<Void> future = executor.submit("1", new RepeatedTask() {
        private int count = 0;

        @Override
        public long executeOnce() {
          queue.add("Message" + count);
          if (++count < 10) {
            return 10L;
          }
          return -1;
        }
      });

      // Wait for the task to finish
      future.get(2, TimeUnit.SECONDS);
      for (int i = 0; i < 10; i++) {
        Assert.assertEquals("Message" + i, queue.poll());
      }

      // Make sure the task didn't get executed again
      Assert.assertNull(queue.poll(2, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testCancelTask() throws Exception {
    KeyedExecutor<String> executor = new KeyedExecutor<>(Executors.newScheduledThreadPool(0));
    try {
      CountDownLatch runLatch = new CountDownLatch(1);
      CountDownLatch latch = new CountDownLatch(1);
      CountDownLatch exceptionLatch = new CountDownLatch(1);
      Future<Void> future = executor.execute("1", () -> {
        runLatch.countDown();
        try {
          latch.await();
        } catch (InterruptedException e) {
          exceptionLatch.countDown();
        }
      });

      // Make sure the task is launched
      runLatch.await(5, TimeUnit.SECONDS);
      // Cancel the task, expect the task will get interrupted
      future.cancel(true);

      exceptionLatch.await(5, TimeUnit.SECONDS);

    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testCancelNoInterrupt() throws Exception {
    KeyedExecutor<String> executor = new KeyedExecutor<>(Executors.newScheduledThreadPool(0));
    try {
      Semaphore s1 = new Semaphore(0);
      Semaphore s2 = new Semaphore(0);
      CountDownLatch exceptionLatch = new CountDownLatch(1);

      Future<Void> future = executor.submit("1", () -> {
        try {
          s1.release();
          s2.acquire();
          return 10L;
        } catch (InterruptedException e) {
          exceptionLatch.countDown();
          throw e;
        }
      });

      // Make sure the task is running
      s1.acquire();
      // Cancel the task without interruption
      future.cancel(false);
      // There shouldn't be any exception
      exceptionLatch.await(1, TimeUnit.SECONDS);
      // Release a permit for the task to complete
      s2.release();
      // There shouldn't be any more execution of the task
      s1.tryAcquire(1, TimeUnit.SECONDS);

    } finally {
      executor.shutdownNow();
    }
  }
}
