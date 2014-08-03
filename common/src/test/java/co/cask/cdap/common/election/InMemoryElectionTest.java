/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.common.election;

import org.apache.twill.api.ElectionHandler;
import org.apache.twill.common.Cancellable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test for the {@link InMemoryElectionRegistry}.
 */
public class InMemoryElectionTest {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryElectionTest.class);

  @Test(timeout = 5000)
  public void testElection() throws ExecutionException, InterruptedException, BrokenBarrierException {
    final InMemoryElectionRegistry electionRegistry = new InMemoryElectionRegistry();

    ExecutorService executor = Executors.newCachedThreadPool();

    // Create 5 participants to join leader election process simultaneously
    int participantCount = 5;
    final CyclicBarrier barrier = new CyclicBarrier(participantCount + 1);
    final Semaphore leaderSem = new Semaphore(0);
    final Semaphore followerSem = new Semaphore(0);
    final CountDownLatch[] stopLatch = new CountDownLatch[participantCount];

    try {
      final AtomicInteger currentLeader = new AtomicInteger(-1);
      for (int i = 0; i < participantCount; i++) {
        stopLatch[i] = new CountDownLatch(1);

        final int idx = i;
        executor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              barrier.await();

              Cancellable cancel = electionRegistry.join("test", new ElectionHandler() {
                @Override
                public void leader() {
                  currentLeader.set(idx);
                  leaderSem.release();
                }

                @Override
                public void follower() {
                  followerSem.release();
                }
              });

              stopLatch[idx].await(10, TimeUnit.SECONDS);
              cancel.cancel();

            } catch (Exception e) {
              LOG.error(e.getMessage(), e);
            }
          }
        });
      }

      // Sync the joining
      barrier.await();

      // There should be 1 leader and 4 followers
      leaderSem.tryAcquire(10, TimeUnit.SECONDS);
      followerSem.tryAcquire(participantCount - 1, 10, TimeUnit.SECONDS);

      // Continuously stopping leader until there is one left.
      for (int i = 0; i < participantCount - 1; i++) {
        stopLatch[currentLeader.get()].countDown();
        // Each time when the leader is unregistered from the leader election, a new leader would rise and
        // the old leader would become a follower.
        leaderSem.tryAcquire(10, TimeUnit.SECONDS);
        followerSem.tryAcquire(10, TimeUnit.SECONDS);
      }

      // Withdraw the last leader, it'd become follower as well.
      stopLatch[currentLeader.get()].countDown();
      followerSem.tryAcquire(10, TimeUnit.SECONDS);

    } finally {
      executor.shutdown();
      executor.awaitTermination(5L, TimeUnit.SECONDS);
    }
  }
}
