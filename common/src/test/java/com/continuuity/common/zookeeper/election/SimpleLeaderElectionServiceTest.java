package com.continuuity.common.zookeeper.election;

import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test SimpleLeaderElectionService
 */
public class SimpleLeaderElectionServiceTest {
  private static InMemoryZKServer zkServer;
  private static ListeningExecutorService executorService;

  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public ExternalResource startServices = new ExternalResource() {
    @Override
    protected void before() throws Throwable {
      super.before();
      zkServer = InMemoryZKServer.builder().setDataDir(temporaryFolder.newFolder()).build();
      zkServer.startAndWait();

      executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
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

  @Test(timeout = 5000)
  public void testLeaderElection() throws Exception {
    int asyncCount = 5;
    AtomicInteger numLeaders = new AtomicInteger(0);
    AtomicInteger numRunners = new AtomicInteger(0);

    List<RunElection> runElections = Lists.newArrayList();
    List<ListenableFuture<?>> futures = Lists.newArrayList();
    for (int i = 0; i < asyncCount; ++i) {
      RunElection runElection = new RunElection("Election1", numLeaders, numRunners);
      runElections.add(runElection);
      futures.add(executorService.submit(runElection));
    }
    Futures.allAsList(futures).get();

    Assert.assertEquals(asyncCount, numRunners.get());
    Assert.assertEquals(1, numLeaders.get());

    for (RunElection runElection : runElections) {
      runElection.stop();
    }
  }

  private static class RunElection implements Runnable {
    private final String electionId;
    private final AtomicInteger numLeaders;
    private final AtomicInteger numRunners;
    private final ZKClientService zkClientService;
    private final SimpleLeaderElectionService electionService;

    private RunElection(String electionId, AtomicInteger numLeaders, AtomicInteger numRunners) {
      this.electionId = electionId;
      this.numLeaders = numLeaders;
      this.numRunners = numRunners;

      this.zkClientService = ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(zkServer.getConnectionStr()).build(),
            RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
          )
        ));

      this.electionService = new SimpleLeaderElectionService(zkClientService);
    }

    @Override
    public void run() {
      try {
        zkClientService.startAndWait();

        numRunners.incrementAndGet();
        electionService.registerElection(new Election(electionId,
                                                      new ElectionHandler() {
                                                        @Override
                                                        public void elected(String id) {
                                                          numLeaders.incrementAndGet();
                                                        }

                                                        @Override
                                                        public void unelected(String id) {
                                                          numLeaders.decrementAndGet();
                                                        }
                                                      }));

      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    public void stop() throws Exception {
      electionService.shutDown();
      zkClientService.stop();
    }
  }

}
