package com.continuuity.common.zookeeper;

import com.continuuity.common.zookeeper.election.ElectionHandler;
import com.continuuity.common.zookeeper.election.LeaderElection;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
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
 * Test for {@link LeaderElection}.
 */
public class LeaderElectionTest {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderElectionTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static InMemoryZKServer zkServer;

  @Test (timeout = 5000)
  public void testElection() throws ExecutionException, InterruptedException, BrokenBarrierException {
    ExecutorService executor = Executors.newCachedThreadPool();

    int participantCount = 5;
    final CyclicBarrier barrier = new CyclicBarrier(participantCount + 1);
    final Semaphore leaderSem = new Semaphore(0);
    final Semaphore followerSem = new Semaphore(0);
    final CountDownLatch[] stopLatch = new CountDownLatch[participantCount];
    final List<ZKClientService> zkClients = Lists.newArrayList();

    try {
      final AtomicInteger currentLeader = new AtomicInteger(-1);
      for (int i = 0; i < participantCount; i++) {
        final ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
        zkClient.startAndWait();
        stopLatch[i] = new CountDownLatch(1);
        zkClients.add(zkClient);

        final int idx = i;
        executor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              barrier.await();

              LeaderElection leaderElection = new LeaderElection(zkClient, "/test", new ElectionHandler() {
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

              stopLatch[idx].await();
              leaderElection.cancel();

            } catch (Exception e) {
              LOG.error(e.getMessage(), e);
            }
          }
        });
      }

      barrier.await();
      leaderSem.acquire(1);
      followerSem.acquire(participantCount - 1);

      // Continuously stopping leader until there is one left.
      for (int i = 0; i < participantCount - 1; i++) {
        stopLatch[currentLeader.get()].countDown();
        leaderSem.acquire(1);
        followerSem.acquire(1);
      }

      stopLatch[currentLeader.get()].countDown();

    } finally {
      executor.shutdown();
      executor.awaitTermination(5L, TimeUnit.SECONDS);

      for (ZKClientService zkClient : zkClients) {
        zkClient.stopAndWait();
      }
    }
  }

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();
  }

  @AfterClass
  public static void finish() {
    zkServer.stopAndWait();
  }
}
