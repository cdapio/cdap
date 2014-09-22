/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data.operation.executor.remote;

import co.cask.tephra.distributed.ElasticPool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * TODO: Move to Tephra
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
