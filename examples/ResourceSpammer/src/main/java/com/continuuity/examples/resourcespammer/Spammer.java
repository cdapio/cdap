/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
