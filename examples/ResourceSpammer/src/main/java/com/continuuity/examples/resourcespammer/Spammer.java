/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.resourcespammer;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
