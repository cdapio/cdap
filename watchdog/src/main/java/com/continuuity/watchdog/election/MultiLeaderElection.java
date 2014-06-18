package com.continuuity.watchdog.election;

import com.continuuity.common.zookeeper.election.ElectionHandler;
import com.continuuity.common.zookeeper.election.LeaderElection;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.ZKClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Handles leader election for multiple partitions.
 */
public class MultiLeaderElection extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(MultiLeaderElection.class);
  private static final Random RANDOM = new Random(System.nanoTime());

  private final ZKClient zkClient;
  private final String name;
  private final int partitionSize;
  private final ExecutorService executor;
  private final PartitionChangeHandler handler;
  private final Set<Integer> leaderPartitions;
  private final List<LeaderElection> electionCancels;
  private final CountDownLatch stopLatch;

  private Set<Integer> prevLeaderPartitions;
  private int leaderElectionSleepMs = 8 * 1000;

  public MultiLeaderElection(ZKClient zkClient, String name, int partitionSize, PartitionChangeHandler handler) {
    this.zkClient = zkClient;
    this.name = name;
    this.partitionSize = partitionSize;
    this.handler = handler;

    this.executor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("multi-leader-election"));
    this.leaderPartitions = Sets.newCopyOnWriteArraySet();
    this.electionCancels = Lists.newArrayList();
    this.prevLeaderPartitions = ImmutableSet.of();
    this.stopLatch = new CountDownLatch(1);
  }

  @Override
  protected Executor executor() {
    return new Executor() {
      @Override
      public void execute(Runnable command) {
        Thread t = new Thread(command, getServiceName());
        t.setDaemon(true);
        t.start();
      }
    };
  }

  @Override
  protected void run() throws Exception {
    LOG.info("Starting multi leader election...");

    // Divide the set of partitions into 2 and run leader election on them separately.
    Set<Integer> partitions1 = Sets.newHashSet();
    Set<Integer> partitions2 = Sets.newHashSet();
    for (int i = 0; i < partitionSize; ++i) {
      if (RANDOM.nextBoolean()) {
        partitions1.add(i);
      } else {
        partitions2.add(i);
      }
    }

    // Wait for a random time to get even distribution of leader partitions.
    int ms = RANDOM.nextInt(leaderElectionSleepMs) + 1;
    LOG.debug("Sleeping for {} ms for partition {} before leader election", ms, partitions1);
    TimeUnit.MILLISECONDS.sleep(ms);
    runElection(partitions1);

    ms = RANDOM.nextInt(leaderElectionSleepMs) + 1;
    LOG.debug("Sleeping for {} ms for partition {} before leader election", ms, partitions2);
    TimeUnit.MILLISECONDS.sleep(ms);
    runElection(partitions2);

    LOG.info("Multi leader election started.");
    stopLatch.await();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping leader election.");

    List<ListenableFuture<?>> futures = Lists.newArrayList();
    for (LeaderElection election : electionCancels) {
      futures.add(election.stop());
    }

    try {
      Futures.successfulAsList(futures).get(10, TimeUnit.SECONDS);
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
      LOG.info("Leader election stopped.");
    }
  }

  @Override
  protected void triggerShutdown() {
    stopLatch.countDown();
  }

  public void setLeaderElectionSleepMs(int leaderElectionSleepMs) {
    this.leaderElectionSleepMs = leaderElectionSleepMs;
  }

  private void runElection(Set<Integer> partitions) throws Exception {
    for (final int partition : partitions) {
      // Start leader election.
      LOG.info("Start leader election for partition {}", partition);
      LeaderElection election =
        new LeaderElection(zkClient, String.format("/election/%s/part-%d", name, partition), new ElectionHandler() {
          @Override
          public void leader() {
            leaderPartitions.add(partition);
            executor.submit(runHandler);
          }

          @Override
          public void follower() {
            leaderPartitions.remove(partition);
            executor.submit(runHandler);
          }
        });
      election.start();
      electionCancels.add(election);
    }
  }

  private final Runnable runHandler = new Runnable() {
    @Override
    public void run() {
      Set<Integer> newLeaders = ImmutableSet.copyOf(leaderPartitions);
      if (!newLeaders.equals(prevLeaderPartitions)) {
        LOG.info("Leader partitions changed - {}", newLeaders);
        prevLeaderPartitions = newLeaders;
        handler.partitionsChanged(newLeaders);
      }
    }
  };
}
