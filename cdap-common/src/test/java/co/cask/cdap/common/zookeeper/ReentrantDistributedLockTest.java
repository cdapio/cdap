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

package co.cask.cdap.common.zookeeper;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Unit test for {@link ReentrantDistributedLock}.
 */
public class ReentrantDistributedLockTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static final Logger LOG = LoggerFactory.getLogger(ReentrantDistributedLockTest.class);

  private static InMemoryZKServer zkServer;

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();
  }

  @AfterClass
  public static void finish() {
    zkServer.stopAndWait();
  }

  @Test (timeout = 20000)
  public void testReentrant() {
    // Test the lock is reentrant from the same thread
    ZKClientService zkClient = createZKClient();
    try {
      ReentrantDistributedLock lock = new ReentrantDistributedLock(zkClient, "reentrant");
      lock.lock();
      try {
        try {
          lock.lock();
        } finally {
          lock.unlock();
        }
      } finally {
        lock.unlock();
      }
    } finally {
      zkClient.stopAndWait();
    }
  }

  @Test (timeout = 20000)
  public void testMultiThreads() throws InterruptedException {
    // Test the lock mechanism between multiple threads
    ZKClientService zkClient = createZKClient();
    try {
      // Create two threads and compete for the lock
      final ReentrantDistributedLock lock = new ReentrantDistributedLock(zkClient, "multiThreads");
      final CountDownLatch acquired = new CountDownLatch(1);
      Thread t = new Thread() {
        @Override
        public void run() {
          lock.lock();
          try {
            acquired.countDown();
          } finally {
            lock.unlock();
          }
        }
      };

      lock.lock();
      try {
        t.start();
        // Wait for the thread to get the lock, should fail.
        Assert.assertFalse(acquired.await(1, TimeUnit.SECONDS));
      } finally {
        lock.unlock();
      }

      Assert.assertTrue(acquired.await(5, TimeUnit.SECONDS));
      t.join();

    } finally {
      zkClient.stopAndWait();
    }
  }

  @Test (timeout = 20000)
  public void testReentrantMultiClients() throws InterruptedException {
    // Test the reentrant lock/unlock mechanism works correctly across multiple instances of lock
    // This is to simulate the lock between multiple processes
    ZKClientService zkClient1 = createZKClient();
    ZKClientService zkClient2 = createZKClient();
    try {
      // Create two distributed locks
      ReentrantDistributedLock lock1 = new ReentrantDistributedLock(zkClient1, "multiClients");
      final ReentrantDistributedLock lock2 = new ReentrantDistributedLock(zkClient2, "multiClients");

      // Create a thread for locking with lock2
      final CountDownLatch lockAcquired = new CountDownLatch(1);
      Thread t = new Thread() {
        @Override
        public void run() {
          lock2.lock();
          try {
            lockAcquired.countDown();
          } finally {
            lock2.unlock();
          }
        }
      };

      // Lock with lock1
      lock1.lock();
      try {
        // Start the lock2 thread. It should be blocked at acquiring the lock
        t.start();
        Assert.assertFalse(lockAcquired.await(1, TimeUnit.SECONDS));

        // Lock with lock1 again and unlock. The inner unlock shouldn't release the distributed lock
        lock1.lock();
        lock1.unlock();

        Assert.assertFalse(lockAcquired.await(1, TimeUnit.SECONDS));
      } finally {
        lock1.unlock();
      }

      // Now lock1 is unlocked, the lock2 thread should be able to proceed
      Assert.assertTrue(lockAcquired.await(5, TimeUnit.SECONDS));
      t.join();
    } finally {
      zkClient1.stopAndWait();
      zkClient2.stopAndWait();
    }
  }

  @Test (timeout = 20000)
  public void testLockInterrupt() throws InterruptedException {
    // Test lock interruption on multiple threads.
    ZKClientService zkClient = createZKClient();
    try {
      final ReentrantDistributedLock lock = new ReentrantDistributedLock(zkClient, "/interrupt");

      // Create a new thread to acquire the same lock interruptibly.
      lock.lock();
      try {
        final CountDownLatch lockAcquired = new CountDownLatch(1);
        final CountDownLatch lockInterrupted = new CountDownLatch(1);
        Thread t = new Thread() {
          @Override
          public void run() {
            try {
              lock.lockInterruptibly();
              try {
                lockAcquired.countDown();
              } finally {
                lock.unlock();
              }
            } catch (InterruptedException e) {
              lockInterrupted.countDown();
            }
          }
        };

        t.start();
        t.interrupt();
        Assert.assertFalse(lockAcquired.await(2, TimeUnit.SECONDS));
        Assert.assertTrue(lockInterrupted.await(2, TimeUnit.SECONDS));
      } finally {
        lock.unlock();
      }
    } finally {
      zkClient.stopAndWait();
    }
  }

  @Test (timeout = 200000)
  public void testLockInterruptMultiClients() throws InterruptedException {
    // Test lock interruption from multiple clients
    ZKClientService zkClient1 = createZKClient();
    ZKClientService zkClient2 = createZKClient();
    try {
      final ReentrantDistributedLock lock1 = new ReentrantDistributedLock(zkClient1, "/multiInterrupt");
      final ReentrantDistributedLock lock2 = new ReentrantDistributedLock(zkClient2, "/multiInterrupt");

      lock1.lock();
      // Create a new thread to acquire a lock interruptibly
      try {
        final CountDownLatch lockAcquired = new CountDownLatch(1);
        final CountDownLatch lockInterrupted = new CountDownLatch(1);
        Thread t = new Thread() {
          @Override
          public void run() {
            try {
              lock2.lockInterruptibly();
              try {
                lockAcquired.countDown();
              } finally {
                lock2.unlock();
              }
            } catch (InterruptedException e) {
              lockInterrupted.countDown();
            }
          }
        };

        t.start();

        // Sleep for 2 seconds to make sure the lock2 in the thread already entered the distributed lock phase.
        TimeUnit.SECONDS.sleep(2);
        t.interrupt();
        Assert.assertFalse(lockAcquired.await(2, TimeUnit.SECONDS));
        Assert.assertTrue(lockInterrupted.await(2, TimeUnit.SECONDS));

      } finally {
        lock1.unlock();
      }

      // After lock1 unlocked, try to lock with lock2 again and it should succeed.
      // This is to verify when a lock is interrupted, it cleanup the states in ZK
      lock2.lock();
      lock2.unlock();

    } finally {
      zkClient1.stopAndWait();
      zkClient2.stopAndWait();
    }
  }

  @Test (timeout = 20000)
  public void testTryLock() throws InterruptedException {
    // Test tryLock on multiple threads
    ZKClientService zkClient = createZKClient();
    try {
      final ReentrantDistributedLock lock = new ReentrantDistributedLock(zkClient, "/trylock");

      // Create a new thread to acquire the same lock by using tryLock
      final CountDownLatch lockAcquired = new CountDownLatch(1);
      final CountDownLatch lockFailed = new CountDownLatch(2);
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            if (!lock.tryLock()) {
              lockFailed.countDown();
            }
            if (!lock.tryLock(1, TimeUnit.SECONDS)) {
              lockFailed.countDown();
            }
            if (lock.tryLock(5, TimeUnit.SECONDS)) {
              // Test the reentrant park as well
              if (lock.tryLock(1, TimeUnit.SECONDS)) {
                if (lock.tryLock()) {
                  lockAcquired.countDown();
                  lock.unlock();
                }
                lock.unlock();
              }
              lock.unlock();
            }
          } catch (InterruptedException e) {
            // Shouldn't happen
            LOG.error(e.getMessage(), e);
          }
        }
      };

      lock.lock();
      try {
        t.start();
        Assert.assertTrue(lockFailed.await(5, TimeUnit.SECONDS));
      } finally {
        lock.unlock();
      }
      Assert.assertTrue(lockAcquired.await(2, TimeUnit.SECONDS));
      t.join();

      // Try to lock again and it should success immediately.
      Assert.assertTrue(lock.tryLock());
      lock.unlock();
    } finally {
      zkClient.stopAndWait();
    }
  }

  @Test (timeout = 20000)
  public void testTryLockMultiClients() throws InterruptedException {
    // Test tryLock on multiple clients
    ZKClientService zkClient1 = createZKClient();
    ZKClientService zkClient2 = createZKClient();
    try {
      final ReentrantDistributedLock lock1 = new ReentrantDistributedLock(zkClient1, "/multiTrylock");
      final ReentrantDistributedLock lock2 = new ReentrantDistributedLock(zkClient2, "/multiTrylock");

      // Create a new thread to acquire the same lock by using tryLock
      final CountDownLatch lockAcquired = new CountDownLatch(1);
      final CountDownLatch lockFailed = new CountDownLatch(2);
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            if (!lock2.tryLock()) {
              lockFailed.countDown();
            }
            if (!lock2.tryLock(1, TimeUnit.SECONDS)) {
              lockFailed.countDown();
            }
            if (lock2.tryLock(5000, TimeUnit.SECONDS)) {
              // Test the reentrant park as well
              if (lock2.tryLock(1, TimeUnit.SECONDS)) {
                if (lock2.tryLock()) {
                  lockAcquired.countDown();
                  lock2.unlock();
                }
                lock2.unlock();
              }
              lock2.unlock();
            }
          } catch (InterruptedException e) {
            // Shouldn't happen
            LOG.error(e.getMessage(), e);
          }
        }
      };

      lock1.lock();
      try {
        t.start();
        Assert.assertTrue(lockFailed.await(5000, TimeUnit.SECONDS));
      } finally {
        lock1.unlock();
      }
      Assert.assertTrue(lockAcquired.await(10000, TimeUnit.SECONDS));
      t.join();

      // Try to lock again and it should success immediately.
      Assert.assertTrue(lock1.tryLock());
      lock1.unlock();
    } finally {
      zkClient1.stopAndWait();
      zkClient2.stopAndWait();
    }
  }

  @Test (timeout = 60000)
  public void testLockRace() throws Exception {
    // Test for multiple clients race on the lock
    // This is for the case when a lock owner release the lock (node deleted)
    // while the other client tries to watch on the node
    ZKClientService zkClient1 = createZKClient();
    ZKClientService zkClient2 = createZKClient();
    try {
      final Lock[] locks = new Lock[] {
        new ReentrantDistributedLock(zkClient1, "/lockrace"),
        new ReentrantDistributedLock(zkClient2, "/lockrace")
      };

      // Have two clients fight for the lock
      final CyclicBarrier barrier = new CyclicBarrier(2);
      final CountDownLatch lockLatch = new CountDownLatch(2);
      for (int i = 0; i < 2; i++) {
        final int threadId = i;
        Thread t = new Thread() {
          @Override
          public void run() {
            try {
              barrier.await();
              for (int i = 0; i < 100; i++) {
                Lock lock = locks[threadId];
                lock.lock();
                try {
                  // A short sleep to make the race possible to happen
                  Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                } finally {
                  lock.unlock();
                }
              }
              lockLatch.countDown();
            } catch (Exception e) {
              LOG.error("Exception", e);
            }
          }
        };
        t.start();
      }

      Assert.assertTrue(lockLatch.await(30, TimeUnit.SECONDS));

    } finally {
      zkClient1.stopAndWait();
      zkClient2.stopAndWait();
    }
  }

  private ZKClientService createZKClient() {
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    return zkClient;
  }
}
