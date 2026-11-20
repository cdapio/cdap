/*
 * Copyright © 2017-2024 Cask Data, Inc.
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

package io.cdap.cdap.common.resource;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.common.utils.Tasks;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test for {@link ResourceBalancerService}.
 */
public class ResourceBalancerServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static InMemoryZKServer zkServer;

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TEMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();
  }

  @AfterClass
  public static void finish() {
    zkServer.stopAndWait();
  }

  @Test
  public void testResourceBalancerService() throws Exception {
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    try (ZKDiscoveryService discoveryService = new ZKDiscoveryService(zkClient)) {
      final TestBalancerService balancerService = new TestBalancerService("test", 4, zkClient,
          discoveryService, discoveryService, false, false);
      balancerService.startAndWait();
      try {
        Tasks.waitFor(ImmutableSet.of(0, 1, 2, 3), new Callable<Set<Integer>>() {
          @Override
          public Set<Integer> call() {
            return balancerService.getPartitions();
          }
        }, 10, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);

        Cancellable cancellable = discoveryService.register(
            new Discoverable("test",
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234)));
        try {
          Tasks.waitFor(2, new Callable<Integer>() {
            @Override
            public Integer call() {
              return balancerService.getPartitions().size();
            }
          }, 10, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
        } finally {
          cancellable.cancel();
        }
      } finally {
        balancerService.stopAndWait();
      }
      Assert.assertEquals(Service.State.TERMINATED, balancerService.state());
    } finally {
      zkClient.stopAndWait();
    }
  }

  @Test
  public void testServiceStartFailure() throws Exception {
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    try (ZKDiscoveryService discoveryService = new ZKDiscoveryService(zkClient)) {
      final TestBalancerService startFailureService = new TestBalancerService("test-start-fail", 4,
          zkClient, discoveryService, discoveryService, true, false);
      startFailureService.startAndWait();

      Tasks.waitFor(Service.State.FAILED, new Callable<Service.State>() {
        @Override
        public Service.State call() {
          return startFailureService.state();
        }
      }, 10, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
    } finally {
      zkClient.stopAndWait();
    }
  }

  @Test
  public void testServiceStopFailure() throws Exception {
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    try (ZKDiscoveryService discoveryService = new ZKDiscoveryService(zkClient)) {
      final TestBalancerService stopFailureService = new TestBalancerService("test-stop-fail", 4,
          zkClient, discoveryService, discoveryService, false, true);
      stopFailureService.startAndWait();

      Tasks.waitFor(ImmutableSet.of(0, 1, 2, 3), new Callable<Set<Integer>>() {
        @Override
        public Set<Integer> call() {
          return stopFailureService.getPartitions();
        }
      }, 10, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);

      Cancellable cancellable = discoveryService.register(
          new Discoverable("test-stop-fail",
              new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234)));
      try {
        Tasks.waitFor(Service.State.FAILED, new Callable<Service.State>() {
          @Override
          public Service.State call() {
            return stopFailureService.state();
          }
        }, 10, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
      } finally {
        cancellable.cancel();
      }
    } finally {
      zkClient.stopAndWait();
    }
  }

  @Test
  public void testLeaderFailoverAndShutdownErrorPropagation() throws Exception {
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    try (ZKDiscoveryService discoveryService = new ZKDiscoveryService(zkClient)) {
      // Start two instances so leader election exercises leader() and follower() transitions.
      TestBalancerService first = new TestBalancerService("test-failover", 4,
          zkClient, discoveryService, discoveryService, false, false);
      TestBalancerService second = new TestBalancerService("test-failover", 4,
          zkClient, discoveryService, discoveryService, false, true);

      first.startAndWait();
      second.startAndWait();

      Tasks.waitFor(2, () -> first.getPartitions().size(),
          10, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
      Tasks.waitFor(2, () -> second.getPartitions().size(),
          10, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);

      // Stopping first steps down leadership cleanly and assigns all 4 partitions to second.
      // Wait for second to finish stopping its 2-partition child (failOnStop=true) or let a clean
      // second instance take over. Let's verify shutDown() propagation on a dedicated instance below!
      first.stopAndWait();
      Assert.assertEquals(Service.State.TERMINATED, first.state());

      // Second fails during rebalance because failOnStop=true when switching from 2 -> 4 partitions.
      Tasks.waitFor(Service.State.FAILED, second::state,
          10, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);

      // Now verify shutDown() itself propagating child stop failure via Throwables.propagate(throwable).
      TestBalancerService shutdownFail = new TestBalancerService("test-shutdown-propagate", 2,
          zkClient, discoveryService, discoveryService, false, true);
      shutdownFail.startAndWait();
      Tasks.waitFor(ImmutableSet.of(0, 1), shutdownFail::getPartitions,
          10, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
      try {
        shutdownFail.stopAndWait();
        Assert.fail("Expected shutDown failure to propagate");
      } catch (Exception e) {
        Assert.assertEquals("exception", Throwables.getRootCause(e).getMessage());
      }
      Assert.assertEquals(Service.State.FAILED, shutdownFail.state());
    } finally {
      zkClient.stopAndWait();
    }
  }

  /**
   * A {@link ResourceBalancerService} for unit-testing.
   */
  private static final class TestBalancerService extends ResourceBalancerService {

    private final boolean failOnStart;
    private final boolean failOnStop;
    private final AtomicReference<Set<Integer>> partitions;

    TestBalancerService(String serviceName, int partitionCount, ZKClient zkClient,
        DiscoveryService discoveryService, DiscoveryServiceClient discoveryServiceClient,
        boolean failOnStart, boolean failOnStop) {
      super(serviceName, partitionCount, zkClient, discoveryService, discoveryServiceClient);
      this.failOnStart = failOnStart;
      this.failOnStop = failOnStop;
      this.partitions = new AtomicReference<>(Collections.emptySet());
    }

    @Override
    protected Service createService(final Set<Integer> partitions) {
      return new AbstractIdleService() {
        @Override
        protected void startUp() throws Exception {
          if (failOnStart) {
            throw new Exception("exception");
          }
          TestBalancerService.this.partitions.set(ImmutableSet.copyOf(partitions));
        }

        @Override
        protected void shutDown() throws Exception {
          if (failOnStop) {
            throw new Exception("exception");
          }
        }
      };
    }

    Set<Integer> getPartitions() {
      return partitions.get();
    }
  }
}
