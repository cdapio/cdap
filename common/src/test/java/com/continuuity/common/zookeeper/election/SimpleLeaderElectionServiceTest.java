package com.continuuity.common.zookeeper.election;

import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Throwables;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test SimpleLeaderElectionService
 */
public class SimpleLeaderElectionServiceTest {
  private static InMemoryZKServer zkServer;
  private static ExecutorService executorService;

  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public ExternalResource startServices = new ExternalResource() {
    @Override
    protected void before() throws Throwable {
      super.before();
      zkServer = InMemoryZKServer.builder().setDataDir(temporaryFolder.newFolder()).build();
      zkServer.startAndWait();

      executorService = Executors.newFixedThreadPool(10);
    }

    @Override
    protected void after() {
      super.after();
      zkServer.stopAndWait();

      executorService.shutdown();
    }
  };

  @Rule
  public TestRule chain = RuleChain.outerRule(temporaryFolder).around(startServices);

  @Test
  public void testLeaderElection() throws Exception {
    int asyncCount = 5;
    CountDownLatch electionCompleteLatch = new CountDownLatch(asyncCount);
    CountDownLatch finishLatch = new CountDownLatch(1);
    AtomicInteger numLeaders = new AtomicInteger(0);
    AtomicInteger numRunners = new AtomicInteger(0);

    for (int i = 0; i < asyncCount; ++i) {
      executorService.submit(new RunElection("Election1", electionCompleteLatch, finishLatch,
                                             numLeaders, numRunners));
    }
    electionCompleteLatch.await();

    try {
      Assert.assertEquals(asyncCount, numRunners.get());
      Assert.assertEquals(1, numLeaders.get());
    } finally {
      finishLatch.countDown();
    }
  }

  private static class RunElection implements Runnable {
    private final String electionId;
    private final CountDownLatch electionCompleteLatch;
    private final CountDownLatch finishLatch;
    private final AtomicInteger numLeaders;
    private final AtomicInteger numRunners;

    private RunElection(String electionId, CountDownLatch electionCompleteLatch, CountDownLatch finishLatch,
                        AtomicInteger numLeaders, AtomicInteger numRunners) {
      this.electionId = electionId;
      this.electionCompleteLatch = electionCompleteLatch;
      this.finishLatch = finishLatch;
      this.numLeaders = numLeaders;
      this.numRunners = numRunners;
    }

    @Override
    public void run() {
      try {
        ZKClientService zkClientService = ZKClientServices.delegate(
          ZKClients.reWatchOnExpire(
            ZKClients.retryOnFailure(
              ZKClientService.Builder.of(zkServer.getConnectionStr()).build(),
              RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
            )
          ));
        zkClientService.startAndWait();

        SimpleLeaderElectionService electionService =
          new SimpleLeaderElectionService(zkClientService);

        numRunners.incrementAndGet();
        electionService.registerElection(new Election(electionId,
                                                      new ElectionHandler() {
                                                        @Override
                                                        public void elected(String id) {
                                                          numLeaders.incrementAndGet();
                                                        }

                                                        @Override
                                                        public void unelected(String id) {
                                                          numLeaders.set(-100000);
                                                        }
                                                      }));
        electionCompleteLatch.countDown();
        finishLatch.await();
        electionService.shutDown();
        zkClientService.stopAndWait();

      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

}
