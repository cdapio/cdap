package com.continuuity.data.util;


import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class LockTableTest {

  @Test(timeout = 1000)
  public void testSingleLock() throws InterruptedException {
    final RowLockTable lockTable = new RowLockTable();
    final RowLockTable.Row rowA = new RowLockTable.Row(new byte[] { 'a' });
    final AtomicInteger a = new AtomicInteger(1);
    // get a lock on row a
    lockTable.lock(rowA);
    // start a thread that attempts to lock a, then increments a
    Thread t = new Thread() {
      @Override
      public void run() {
        lockTable.lock(rowA);
        // expect the value 2. If lock() does not wait, the we are likely to see a 1
        Assert.assertEquals(2, a.get());
        a.incrementAndGet();
        lockTable.unlock(rowA);
      }
    };
    t.start();
    // if the lock does not stop it, then the thread would assert a=2 in the next 10ms
    Thread.sleep(10);
    // increment a
    a.incrementAndGet();
    lockTable.unlock(rowA);
    t.join();
    Assert.assertEquals(3, a.get());
  }

  static int counter = 0;

  static class LockAndRemoveThread extends Thread {
    final RowLockTable locks;
    final long sleep;
    int rounds;
    static final RowLockTable.Row row = new RowLockTable.Row(new byte[] { 'r' });
    LockAndRemoveThread(RowLockTable locks, long sleep, int rounds) {
      this.locks = locks;
      this.sleep = sleep;
      this.rounds = rounds;
    }
    @Override
    public void run() {
      while (rounds > 0) {
        --rounds;
        Assert.assertTrue(locks.validLock(row).isValid());
        int value = counter;
        if (sleep > 0) {
          try {
            Thread.sleep(sleep);
          } catch (InterruptedException e) {
            Assert.fail("caught interrupted exception");
          }
        }
        counter = value + 1;
        locks.unlockAndRemove(row);
      }
    }
  }

  @Test(timeout=5000)
  public void testConcurrentLockAndRemove() throws InterruptedException {
    RowLockTable locks = new RowLockTable();
    long sleep = 10;
    int rounds = 10;
    // run three tests that repeatedly lock, sleep, and remove the same row
    // when one thread removes, both other threads will be waiting and get an invalid lock
    // all threads will do non-atomic increment on the counter, the locks make them atomic
    Thread t1 = new LockAndRemoveThread(locks, sleep, rounds);
    Thread t2 = new LockAndRemoveThread(locks, sleep, rounds);
    Thread t3 = new LockAndRemoveThread(locks, sleep, rounds);
    counter = 0;
    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();
    Assert.assertEquals(3 * rounds, counter);
  }

  @Test(timeout=5000)
  public void testLotsOfLockAndRemove() throws InterruptedException {
    RowLockTable locks = new RowLockTable();
    long sleep = 0;
    int rounds = 10000;
    // run three tests that repeatedly lock and remove the same row without sleeping
    // when one thread removes, the other threads may be waiting and get an invalid lock
    // all threads will do non-atomic increment on the counter, the locks make them atomic
    Thread t1 = new LockAndRemoveThread(locks, sleep, rounds);
    Thread t2 = new LockAndRemoveThread(locks, sleep, rounds);
    Thread t3 = new LockAndRemoveThread(locks, sleep, rounds);
    counter = 0;
    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();
    Assert.assertEquals(3 * rounds, counter);
  }

  static class LockAndUnlockThread extends Thread {
    final RowLockTable locks;
    int rounds;
    static final RowLockTable.Row row = new RowLockTable.Row(new byte[] { 'r' });
    LockAndUnlockThread(RowLockTable locks, int rounds) {
      this.locks = locks;
      this.rounds = rounds;
    }
    @Override
    public void run() {
      while (rounds > 0) {
        --rounds;
        Assert.assertTrue(locks.lock(row).isValid());
        int value = counter;
        counter = value + 1;
        locks.unlock(row);
      }
    }
  }

  @Test(timeout=5000)
  public void testLotsOfLockAndUnlock() throws InterruptedException {
    RowLockTable locks = new RowLockTable();
    int rounds = 10000;
    // run three tests that repeatedly lock and unlock the same row
    // when one thread unlocks, both other threads may be waiting but will never get an invalid lock
    // all threads will do non-atomic increment on the counter, the locks make them atomic
    Thread t1 = new LockAndUnlockThread(locks, rounds);
    Thread t2 = new LockAndUnlockThread(locks, rounds);
    Thread t3 = new LockAndUnlockThread(locks, rounds);
    counter = 0;
    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();
    Assert.assertEquals(3 * rounds, counter);
  }
}

