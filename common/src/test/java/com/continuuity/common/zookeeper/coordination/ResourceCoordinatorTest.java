/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Tests for {@link ResourceCoordinator} and {@link ResourceCoordinatorClient}.
 */
public class ResourceCoordinatorTest {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceCoordinatorTest.class);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static InMemoryZKServer zkServer;

  @Test
  public void testAssignment() throws InterruptedException, ExecutionException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());

    String serviceName = "test-assignment";

    Injector injector = Guice.createInjector(new ConfigModule(cConf),
                                             new ZKClientModule(),
                                             new DiscoveryRuntimeModule().getDistributedModules());
    ZKClientService zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();
    DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);

    try {
      ResourceCoordinator coordinator = new ResourceCoordinator(zkClient,
                                                                injector.getInstance(DiscoveryServiceClient.class),
                                                                new BalancedAssignmentStrategy());
      coordinator.startAndWait();

      try {
        ResourceCoordinatorClient client = new ResourceCoordinatorClient(zkClient);
        client.startAndWait();

        try {
          // Create a requirement
          ResourceRequirement requirement = ResourceRequirement.builder(serviceName).addPartitions("p", 5, 1).build();
          client.submitRequirement(requirement).get();

          // Fetch the requirement, just to verify it's the same as the one get submitted.
          Assert.assertEquals(requirement, client.fetchRequirement(requirement.getName()).get());

          // Register a discovery endpoint
          final Discoverable discoverable1 = createDiscoverable(serviceName, 10000);
          Cancellable cancelDiscoverable1 = discoveryService.register(discoverable1);

          // Add a change handler for this discoverable.
          final BlockingQueue<Collection<PartitionReplica>> assignmentQueue =
            new SynchronousQueue<Collection<PartitionReplica>>();
          final Semaphore finishSemaphore = new Semaphore(0);
          Cancellable cancelSubscribe1 = subscribe(client, discoverable1, assignmentQueue, finishSemaphore);

          // Assert that it received the changes.
          Collection<PartitionReplica> assigned = assignmentQueue.poll(5, TimeUnit.SECONDS);
          Assert.assertNotNull(assigned);
          Assert.assertEquals(5, assigned.size());

          // Unregister from discovery, the handler should receive a change with empty collection
          cancelDiscoverable1.cancel();
          Assert.assertTrue(assignmentQueue.poll(5, TimeUnit.SECONDS).isEmpty());

          // Register to discovery again, would receive changes.
          cancelDiscoverable1 = discoveryService.register(discoverable1);
          assigned = assignmentQueue.poll(5, TimeUnit.SECONDS);
          Assert.assertNotNull(assigned);
          Assert.assertEquals(5, assigned.size());

          // Register another discoverable
          final Discoverable discoverable2 = createDiscoverable(serviceName, 10001);
          Cancellable cancelDiscoverable2 = discoveryService.register(discoverable2);

          // Changes should be received by the handler, with only 3 resources,
          // as 2 out of 5 should get moved to the new discoverable.
          assigned = assignmentQueue.poll(5, TimeUnit.SECONDS);
          Assert.assertNotNull(assigned);
          Assert.assertEquals(3, assigned.size());

          // Cancel the first discoverable again, should expect empty result.
          // This also make sure the latest assignment get cached in the ResourceCoordinatorClient.
          // It is the the next test step.
          cancelDiscoverable1.cancel();
          Assert.assertTrue(assignmentQueue.poll(5, TimeUnit.SECONDS).isEmpty());

          // Cancel the handler.
          cancelSubscribe1.cancel();
          Assert.assertTrue(finishSemaphore.tryAcquire(2, TimeUnit.SECONDS));

          // Subscribe to changes for the second discoverable,
          // it should see the latest assignment, even though no new fetch from ZK is triggered.
          Cancellable cancelSubscribe2 = subscribe(client, discoverable2, assignmentQueue, finishSemaphore);
          assigned = assignmentQueue.poll(5, TimeUnit.SECONDS);
          Assert.assertNotNull(assigned);
          Assert.assertEquals(5, assigned.size());

          // Delete the requirement, the handler should receive a empty collection
          client.deleteRequirement(requirement.getName());
          Assert.assertTrue(assignmentQueue.poll(5, TimeUnit.SECONDS).isEmpty());

          // Cancel the second handler.
          cancelSubscribe2.cancel();
          Assert.assertTrue(finishSemaphore.tryAcquire(2, TimeUnit.SECONDS));

          cancelDiscoverable2.cancel();

        } finally {
          client.stopAndWait();
        }
      } finally {
        coordinator.stopAndWait();
      }

    } finally {
      zkClient.stopAndWait();
    }
  }

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();
  }

  @AfterClass
  public static void finish() {
    zkServer.stopAndWait();
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
          LOG.error("Finished with failure for {}", discoverable.getSocketAddress().getPort(), failureCause);
        }
      }
    });
  }

  private Discoverable createDiscoverable(final String serviceName, final int port) {
    InetSocketAddress address;
    try {
      address = new InetSocketAddress(InetAddress.getLocalHost(), port);
    } catch (UnknownHostException e) {
      address = new InetSocketAddress(port);
    }
    final InetSocketAddress finalAddress = address;

    return new Discoverable() {
      @Override
      public String getName() {
        return serviceName;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return finalAddress;
      }
    };
  }
}
