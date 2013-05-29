package com.continuuity.data.operation.ttqueue.randomized;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 */
public class FlexibleCyclicBarrier {
  private CyclicBarrier barrier;
  private final ReadWriteLock barrierLock;

  public FlexibleCyclicBarrier(int parties) {
    this.barrier = new CyclicBarrier(parties);
    this.barrierLock = new ReentrantReadWriteLock();
  }

  public int getParties() {
    return barrier.getParties();
  }

  public int await() throws InterruptedException, BrokenBarrierException {
    barrierLock.readLock().lock();
    try {
      return barrier.await();
    } finally {
      barrierLock.readLock().unlock();
    }
  }

  public int decrementParties() throws BrokenBarrierException, InterruptedException {
    while(!barrierLock.writeLock().tryLock()) {
      // Someone is holding read lock or write lock
      if(barrierLock.readLock().tryLock()) {
        // Someone is holding a read lock
        try {
          if(barrier.getNumberWaiting() > 0) {
            barrier.await();
          }
        } finally {
          barrierLock.readLock().unlock();
        }
      } else {
        TimeUnit.MICROSECONDS.sleep(500);
      }
    }

    // We have the write lock, reduce the size of barrier by one
    try {
      int newParties = barrier.getParties() - 1;
      if(newParties < 1) {
        return newParties;
      }
      barrier = new CyclicBarrier(newParties);
      return barrier.getParties();
    } finally {
      barrierLock.writeLock().unlock();
    }
  }
}
