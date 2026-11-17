/*
 * Copyright © 2014-2024 Cask Data, Inc.
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

package io.cdap.cdap.common.zookeeper.coordination;

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.ZkClientModule;
import io.cdap.cdap.common.guice.ZkDiscoveryModule;
import io.cdap.cdap.common.utils.Tasks;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.internal.zookeeper.SettableOperationFuture;
import org.apache.twill.zookeeper.ForwardingZKClient;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link ResourceCoordinator} and {@link ResourceCoordinatorClient}.
 */
public class ResourceCoordinatorTest {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceCoordinatorTest.class);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static InMemoryZKServer zkServer;

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAsync().awaitRunning();
  }

  @AfterClass
  public static void finish() {
    zkServer.stopAsync().awaitTerminated();
  }

  @Test
  public void testAssignment() throws InterruptedException, ExecutionException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());

    String serviceName = "test-assignment";

    Injector injector = Guice.createInjector(new ConfigModule(cConf),
        new ZkClientModule(),
        new ZkDiscoveryModule());
    ZKClientService zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAsync().awaitRunning();
    DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);

    try {
      ResourceCoordinator coordinator = new ResourceCoordinator(zkClient,
          injector.getInstance(DiscoveryServiceClient.class),
          new BalancedAssignmentStrategy());
      coordinator.startAsync().awaitRunning();

      try {
        ResourceCoordinatorClient client = new ResourceCoordinatorClient(zkClient);
        client.startAsync().awaitRunning();

        try {
          ResourceRequirement requirement = ResourceRequirement.builder(serviceName)
              .addPartitions("p", 5, 1).build();
          client.submitRequirement(requirement).get();

          Assert.assertEquals(requirement, client.fetchRequirement(requirement.getName()).get());

          final Discoverable discoverable1 = createDiscoverable(serviceName, 10000);
          Cancellable cancelDiscoverable1 = discoveryService
              .register(ResolvingDiscoverable.of(discoverable1));

          final BlockingQueue<Collection<PartitionReplica>> assignmentQueue =
              new SynchronousQueue<>();
          final Semaphore finishSemaphore = new Semaphore(0);
          final Cancellable cancelSubscribe1 = subscribe(client, discoverable1, assignmentQueue,
              finishSemaphore);

          Collection<PartitionReplica> assigned = assignmentQueue.poll(30, TimeUnit.SECONDS);
          Assert.assertNotNull(assigned);
          Assert.assertEquals(5, assigned.size());

          cancelDiscoverable1.cancel();
          Assert.assertTrue(assignmentQueue.poll(30, TimeUnit.SECONDS).isEmpty());

          cancelDiscoverable1 = discoveryService.register(ResolvingDiscoverable.of(discoverable1));
          assigned = assignmentQueue.poll(30, TimeUnit.SECONDS);
          Assert.assertNotNull(assigned);
          Assert.assertEquals(5, assigned.size());

          final Discoverable discoverable2 = createDiscoverable(serviceName, 10001);
          final Cancellable cancelDiscoverable2 = discoveryService
              .register(ResolvingDiscoverable.of(discoverable2));

          assigned = assignmentQueue.poll(30, TimeUnit.SECONDS);
          Assert.assertNotNull(assigned);
          Assert.assertEquals(3, assigned.size());

          cancelDiscoverable1.cancel();
          Assert.assertTrue(assignmentQueue.poll(30, TimeUnit.SECONDS).isEmpty());

          cancelSubscribe1.cancel();
          Assert.assertTrue(finishSemaphore.tryAcquire(2, TimeUnit.SECONDS));

          final Cancellable cancelSubscribe2 = subscribe(client, discoverable2, assignmentQueue,
              finishSemaphore);
          assigned = assignmentQueue.poll(30, TimeUnit.SECONDS);
          Assert.assertNotNull(assigned);
          Assert.assertEquals(5, assigned.size());

          client.submitRequirement(ResourceRequirement.builder(serviceName).build());
          Assert.assertTrue(assignmentQueue.poll(30, TimeUnit.SECONDS).isEmpty());

          client.submitRequirement(
              ResourceRequirement.builder(serviceName).addPartitions("p", 1, 1).build());
          assigned = assignmentQueue.poll(30, TimeUnit.SECONDS);
          Assert.assertNotNull(assigned);
          Assert.assertEquals(1, assigned.size());

          client.deleteRequirement(requirement.getName());
          Assert.assertTrue(assignmentQueue.poll(30, TimeUnit.SECONDS).isEmpty());

          cancelSubscribe2.cancel();
          Assert.assertTrue(finishSemaphore.tryAcquire(2, TimeUnit.SECONDS));

          cancelDiscoverable2.cancel();
        } finally {
          client.stopAsync().awaitTerminated();
        }
      } finally {
        coordinator.stopAsync().awaitTerminated();
      }
    } finally {
      zkClient.stopAsync().awaitTerminated();
    }
  }

  @Test
  public void testSaveAssignmentFailureNotifiesFailed() throws Exception {
    ZKClientService rawZkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    rawZkClient.startAsync().awaitRunning();
    try {
      ZKClient namespaced = ZKClients.namespace(rawZkClient, "/save-fail-ns");
      ZKClient failingSaveZk = new ForwardingZKClient(namespaced) {
        @Override
        public OperationFuture<String> create(String path, byte[] data, CreateMode createMode,
            boolean createParent, Iterable<ACL> acl) {
          if (path.startsWith(CoordinationConstants.ASSIGNMENTS_PATH)) {
            SettableOperationFuture<String> failed = SettableOperationFuture.create(
                path, Threads.SAME_THREAD_EXECUTOR);
            failed.setException(new KeeperException.AuthFailedException());
            return failed;
          }
          return super.create(path, data, createMode, createParent, acl);
        }

        @Override
        public OperationFuture<Stat> setData(String path, byte[] data, int version) {
          if (path.startsWith(CoordinationConstants.ASSIGNMENTS_PATH)) {
            SettableOperationFuture<Stat> failed = SettableOperationFuture.create(
                path, Threads.SAME_THREAD_EXECUTOR);
            failed.setException(new KeeperException.AuthFailedException());
            return failed;
          }
          return super.setData(path, data, version);
        }
      };

      InMemoryDiscoveryService discovery = new InMemoryDiscoveryService();
      String serviceName = "save-fail-svc";
      Cancellable reg = discovery.register(createDiscoverable(serviceName, 12345));
      try {
        ResourceCoordinator coordinator = new ResourceCoordinator(
            failingSaveZk, discovery, new BalancedAssignmentStrategy());
        coordinator.startAsync().awaitRunning();

        ResourceCoordinatorClient client = new ResourceCoordinatorClient(namespaced);
        client.submitRequirement(
            ResourceRequirement.builder(serviceName).addPartitions("p", 2, 1).build()).get();

        Tasks.waitFor(Service.State.FAILED, coordinator::state,
            5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);
      } finally {
        reg.cancel();
      }
    } finally {
      rawZkClient.stopAsync().awaitTerminated();
    }
  }

  private Cancellable subscribe(ResourceCoordinatorClient client,
      final Discoverable discoverable,
      final BlockingQueue<Collection<PartitionReplica>> assignmentQueue,
      final Semaphore finishSemaphore) {
    return client.subscribe(discoverable.getName(), new ResourceHandler(discoverable) {
      @Override
      public void onChange(Collection<PartitionReplica> partitionReplicas) {
        try {
          LOG.debug("Discoverable {} Received: {}",
              discoverable.getSocketAddress().getPort(), partitionReplicas);
          assignmentQueue.put(partitionReplicas);
        } catch (InterruptedException e) {
          LOG.error("Interrupted.", e);
        }
      }

      @Override
      public void finished(Throwable failureCause) {
        LOG.debug("Finished on {}", discoverable.getSocketAddress().getPort());
        if (failureCause == null) {
          finishSemaphore.release();
        } else {
          LOG.error("Finished with failure for {}", discoverable.getSocketAddress().getPort(),
              failureCause);
        }
      }
    });
  }

  private Discoverable createDiscoverable(String serviceName, final int port) {
    InetSocketAddress address;
    try {
      address = new InetSocketAddress(InetAddress.getLocalHost(), port);
    } catch (UnknownHostException e) {
      address = new InetSocketAddress(port);
    }
    return new Discoverable(serviceName, address);
  }
}
