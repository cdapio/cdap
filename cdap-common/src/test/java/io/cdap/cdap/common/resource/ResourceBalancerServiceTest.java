/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.common.resource;

import co.cask.cdap.common.utils.Tasks;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit test for {@link ResourceBalancerService}.
 */
public class ResourceBalancerServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  public ResourceBalancerServiceTest() {
    super();
  }

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
    // Simple test for resource balancer does react to discovery changes correct
    // More detailed tests are in ResourceCoordinatorTest, which the ResourceBalancerService depends on
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    try (ZKDiscoveryService discoveryService = new ZKDiscoveryService(zkClient)) {
      // Test the failure on stop case
      final TestBalancerService stopFailureService = new TestBalancerService("test", 4, zkClient, discoveryService,
                                                                             discoveryService, false, false);
      stopFailureService.startAndWait();

      // Should get all four partitions
      Tasks.waitFor(ImmutableSet.of(0, 1, 2, 3), new Callable<Set<Integer>>() {
        @Override
        public Set<Integer> call() throws Exception {
          return stopFailureService.getPartitions();
        }
      }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // Register a new discoverable, this should trigger a partition change in the resource balancer service
      Cancellable cancellable = discoveryService.register(
        new Discoverable("test", new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234)));
      try {
        // Should get reduced to two partitions
        Tasks.waitFor(2, new Callable<Integer>() {
          @Override
          public Integer call() throws Exception {
            return stopFailureService.getPartitions().size();
          }
        }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      } finally {
        cancellable.cancel();
      }
    } finally {
      zkClient.stopAndWait();
    }
  }

  @Test
  public void testServiceStartFailure() throws Exception {
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    try (ZKDiscoveryService discoveryService = new ZKDiscoveryService(zkClient)) {
      // Test the failure on start case
      final TestBalancerService startFailureService = new TestBalancerService("test", 4, zkClient, discoveryService,
                                                                              discoveryService, true, false);
      startFailureService.startAndWait();

      // The resource balance service should fail
      Tasks.waitFor(Service.State.FAILED, new Callable<Service.State>() {
        @Override
        public Service.State call() throws Exception {
          return startFailureService.state();
        }
      }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    } finally {
      zkClient.stopAndWait();
    }
  }

  @Test
  public void testServiceStopFailure() throws Exception {
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    try (ZKDiscoveryService discoveryService = new ZKDiscoveryService(zkClient)) {
      // Test the failure on stop case
      final TestBalancerService stopFailureService = new TestBalancerService("test", 4, zkClient, discoveryService,
                                                                              discoveryService, false, true);
      stopFailureService.startAndWait();

      // Should get four partitions
      Tasks.waitFor(ImmutableSet.of(0, 1, 2, 3), new Callable<Set<Integer>>() {
        @Override
        public Set<Integer> call() throws Exception {
          return stopFailureService.getPartitions();
        }
      }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // Register a new discoverable, this should trigger a partition change in the resource balancer service
      Cancellable cancellable = discoveryService.register(
        new Discoverable("test", new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234)));
      try {
        // When there is exception thrown by the underlying service during partition change,
        // the resource balancer service should fail.
        Tasks.waitFor(Service.State.FAILED, new Callable<Service.State>() {
          @Override
          public Service.State call() throws Exception {
            return stopFailureService.state();
          }
        }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      } finally {
        cancellable.cancel();
      }
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
      this.partitions = new AtomicReference<>(Collections.<Integer>emptySet());
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
