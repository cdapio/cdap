package com.continuuity.common.zookeeper;

import com.continuuity.common.zookeeper.election.ElectionHandler;
import com.continuuity.common.zookeeper.election.LeaderElection;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.internal.zookeeper.KillZKSession;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
              leaderElection.start();

              stopLatch[idx].await(10, TimeUnit.SECONDS);
              leaderElection.stopAndWait();

            } catch (Exception e) {
              LOG.error(e.getMessage(), e);
            }
          }
        });
      }

      barrier.await();
      leaderSem.tryAcquire(10, TimeUnit.SECONDS);
      followerSem.tryAcquire(participantCount - 1, 10, TimeUnit.SECONDS);

      // Continuously stopping leader until there is one left.
      for (int i = 0; i < participantCount - 1; i++) {
        stopLatch[currentLeader.get()].countDown();
        leaderSem.tryAcquire(10, TimeUnit.SECONDS);
        followerSem.tryAcquire(10, TimeUnit.SECONDS);
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

  @Test (timeout = 10000)
  public void testCancel() throws InterruptedException, IOException {
    List<LeaderElection> leaderElections = Lists.newArrayList();
    List<ZKClientService> zkClients = Lists.newArrayList();

    // Creates two participants
    final Semaphore leaderSem = new Semaphore(0);
    final Semaphore followerSem = new Semaphore(0);
    final AtomicInteger leaderIdx = new AtomicInteger();

    try {
      for (int i = 0; i < 2; i++) {
        ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
        zkClient.startAndWait();

        zkClients.add(zkClient);

        final int finalI = i;
        leaderElections.add(new LeaderElection(zkClient, "/testCancel", new ElectionHandler() {
          @Override
          public void leader() {
            leaderIdx.set(finalI);
            leaderSem.release();
          }

          @Override
          public void follower() {
            followerSem.release();
          }
        }));
      }

      for (LeaderElection leaderElection : leaderElections) {
        leaderElection.start();
      }

      leaderSem.tryAcquire(10, TimeUnit.SECONDS);
      followerSem.tryAcquire(10, TimeUnit.SECONDS);

      int leader = leaderIdx.get();
      int follower = 1 - leader;

      // Kill the follower session
      KillZKSession.kill(zkClients.get(follower).getZooKeeperSupplier().get(),
                         zkClients.get(follower).getConnectString(), 5000);

      // Cancel the leader
      leaderElections.get(leader).stopAndWait();

      // Now follower should still be able to become leader.
      leaderSem.tryAcquire(10, TimeUnit.SECONDS);

      leader = leaderIdx.get();
      follower = 1 - leader;

      // Create another participant (use the old leader zkClient)
      leaderElections.set(follower, new LeaderElection(zkClients.get(follower), "/testCancel", new ElectionHandler() {
        @Override
        public void leader() {
          leaderSem.release();
        }

        @Override
        public void follower() {
          followerSem.release();
        }
      }));
      leaderElections.get(follower).start();

      // Cancel the follower first.
      leaderElections.get(follower).stopAndWait();

      // Cancel the leader.
      leaderElections.get(leader).stopAndWait();

      // Since the follower has been cancelled before leader, there should be no leader.
      Assert.assertFalse(leaderSem.tryAcquire(2, TimeUnit.SECONDS));
    } finally {
      for (ZKClientService zkClient : zkClients) {
        zkClient.stopAndWait();
      }
    }
  }

  @Test (timeout = 10000)
  public void testDisconnect() throws IOException, InterruptedException {
    File zkDataDir = tmpFolder.newFolder();
    InMemoryZKServer ownZKServer = InMemoryZKServer.builder().setDataDir(zkDataDir).build();
    ownZKServer.startAndWait();
    try {
      ZKClientService zkClient = ZKClientService.Builder.of(ownZKServer.getConnectionStr()).build();
      zkClient.startAndWait();

      try {
        final Semaphore leaderSem = new Semaphore(0);
        final Semaphore followerSem = new Semaphore(0);

        LeaderElection leaderElection = new LeaderElection(zkClient, "/testDisconnect", new ElectionHandler() {
          @Override
          public void leader() {
            leaderSem.release();
          }

          @Override
          public void follower() {
            followerSem.release();
          }
        });
        leaderElection.start();

        leaderSem.tryAcquire(10, TimeUnit.SECONDS);

        int zkPort = ownZKServer.getLocalAddress().getPort();

        // Disconnect by shutting the server and restart it on the same port
        ownZKServer.stopAndWait();

        // Right after disconnect, it should become follower
        followerSem.tryAcquire(10, TimeUnit.SECONDS);

        ownZKServer = InMemoryZKServer.builder().setDataDir(zkDataDir).setPort(zkPort).build();
        ownZKServer.startAndWait();

        // Right after reconnect, it should be leader again.
        leaderSem.tryAcquire(10, TimeUnit.SECONDS);

        // Now disconnect it again, but then cancel it before reconnect, it shouldn't become leader
        ownZKServer.stopAndWait();

        // Right after disconnect, it should become follower
        followerSem.tryAcquire(10, TimeUnit.SECONDS);

        ListenableFuture<?> cancelFuture = leaderElection.stop();

        ownZKServer = InMemoryZKServer.builder().setDataDir(zkDataDir).setPort(zkPort).build();
        ownZKServer.startAndWait();

        Futures.getUnchecked(cancelFuture);

        // After reconnect, it should not be leader
        Assert.assertFalse(leaderSem.tryAcquire(2, TimeUnit.SECONDS));
      } finally {
        zkClient.stopAndWait();
      }
    } finally {
      ownZKServer.stopAndWait();
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
