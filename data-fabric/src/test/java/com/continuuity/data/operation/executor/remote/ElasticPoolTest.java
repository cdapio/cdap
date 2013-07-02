package com.continuuity.data.operation.executor.remote;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class ElasticPoolTest {

  static class Dummy {
    static AtomicInteger count = new AtomicInteger(0);
    Dummy() {
      // System.out.println("Creating dummy #" + count.incrementAndGet());
      count.incrementAndGet();
    }
  }

  class DummyPool extends ElasticPool<Dummy, RuntimeException> {

    public DummyPool(int sizeLimit) {
      super(sizeLimit);
    }

    @Override
    protected Dummy create() {
      return new Dummy();
    }
  }

  @Test(timeout = 5000)
  public void testFewerThreadsThanElements() throws InterruptedException {
    final DummyPool pool = new DummyPool(5);
    Dummy.count.set(0);
    Thread[] threads = new Thread[2];
    for (int i = 0; i < threads.length; ++i) {
      threads[i] = new Thread() {
        public void run() {
          for (int j = 0; j < 5; ++j) {
            Dummy dummy = pool.obtain();
            try {
              Thread.sleep(100L);
            } catch (Exception e) {

            }
            pool.release(dummy);
          }
        }
      };
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    Assert.assertEquals(2, Dummy.count.get());
  }

  @Test(timeout = 5000)
  public void testMoreThreadsThanElements() throws InterruptedException {
    final DummyPool pool = new DummyPool(2);
    Dummy.count.set(0);
    Thread[] threads = new Thread[5];
    for (int i = 0; i < threads.length; ++i) {
      threads[i] = new Thread() {
        public void run() {
          for (int j = 0; j < 5; ++j) {
            Dummy dummy = pool.obtain();
            try {
              Thread.sleep(100L);
            } catch (Exception e) {
            }
            pool.release(dummy);
          }
        }
      };
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    Assert.assertEquals(2, Dummy.count.get());
  }

  @Test(timeout = 5000)
  public void testMoreThreadsThanElementsWithDiscard() throws
      InterruptedException {
    Dummy.count.set(0);
    final DummyPool pool = new DummyPool(2);
    Thread[] threads = new Thread[3];
    for (int i = 0; i < threads.length; ++i) {
      threads[i] = new Thread() {
        public void run() {
          for (int j = 0; j < 5; ++j) {
            Dummy dummy = pool.obtain();
            try {
              Thread.sleep(100L);
            } catch (Exception e) {
            }
            pool.discard(dummy);
          }
        }
      };
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    Assert.assertEquals(5 * threads.length, Dummy.count.get());
  }
}
