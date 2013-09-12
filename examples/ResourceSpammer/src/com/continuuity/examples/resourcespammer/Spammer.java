package com.continuuity.examples.resourcespammer;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class Spammer {
  private final int numThreads;

  public Spammer(int numThreads) {
    this.numThreads = numThreads;
  }

  public long spam() {
    return spamFor(1000000);
  }

  public long spamFor(int loops) {
    long start = System.currentTimeMillis();
    ExecutorService pool = Executors.newFixedThreadPool(this.numThreads);
    try {
      for (int i = 0; i < this.numThreads; i++) {
        pool.execute(new SpamLooper(loops));
      }
      pool.shutdown();
      pool.awaitTermination(60L, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      pool.shutdownNow();
    }
    return System.currentTimeMillis() - start;
  }

  class SpamLooper implements Runnable {
    private final Random random;
    private final int loops;

    SpamLooper(int loops) {
      random = new Random();
      this.loops = loops;
    }

    @Override
    public void run() {
      for (int i = 0; i < loops; i++) {
        Integer.toString(random.nextInt()).hashCode();
      }
    }
  }

}
