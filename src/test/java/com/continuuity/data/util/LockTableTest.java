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
    // start a thread that attempts to lock a, then sets a to 2
    Thread t = new Thread() {
      @Override
      public void run() {
        lockTable.lock(rowA);
        // expect the value 2
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
        locks.validLock(row);
        int value = counter;
        try {
          Thread.sleep(sleep);
        } catch (InterruptedException e) {
          Assert.fail("caught interrupted exception");
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
    // run three tests that repeatedly lock and remove the same row
    // when one thread removes, both other threads will be waiting and get an invalid lock
    // all threads will do non-atomic increment on the counter, the locks make them atomic
    LockAndRemoveThread t1 = new LockAndRemoveThread(locks, sleep, rounds);
    LockAndRemoveThread t2 = new LockAndRemoveThread(locks, sleep, rounds);
    LockAndRemoveThread t3 = new LockAndRemoveThread(locks, sleep, rounds);
    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();
    Assert.assertEquals(3 * rounds, counter);
  }
}

