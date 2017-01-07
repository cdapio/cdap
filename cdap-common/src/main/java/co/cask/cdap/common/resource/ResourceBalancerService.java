/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.common.zookeeper.coordination.BalancedAssignmentStrategy;
import co.cask.cdap.common.zookeeper.coordination.PartitionReplica;
import co.cask.cdap.common.zookeeper.coordination.ResourceCoordinator;
import co.cask.cdap.common.zookeeper.coordination.ResourceCoordinatorClient;
import co.cask.cdap.common.zookeeper.coordination.ResourceHandler;
import co.cask.cdap.common.zookeeper.coordination.ResourceRequirement;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.Services;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Set;

/**
 * A service that automatically balances resource assignments between its instances.
 */
public abstract class ResourceBalancerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceBalancerService.class);

  private final String serviceName;
  private final int partitionCount;
  private final LeaderElection election;
  private final ResourceCoordinatorClient resourceClient;
  private final DiscoveryService discoveryService;

  private final SettableFuture<?> completion;

  private Cancellable cancelDiscoverable;
  private Cancellable cancelResourceHandler;

  /**
   * Creates instance of {@link ResourceBalancerService}.
   * @param serviceName name of the service
   * @param partitionCount number of partitions of the resource to balance
   * @param zkClient ZooKeeper place to keep metadata for sync; will be further namespaced with service name
   * @param discoveryService discovery service to register this service
   * @param discoveryServiceClient discovery service client to discover other instances of this service
   */
  protected ResourceBalancerService(String serviceName,
                                    int partitionCount,
                                    ZKClientService zkClient,
                                    DiscoveryService discoveryService,
                                    final DiscoveryServiceClient discoveryServiceClient) {
    this.serviceName = serviceName;
    this.partitionCount = partitionCount;
    this.discoveryService = discoveryService;

    final ZKClient zk = ZKClients.namespace(zkClient, "/" + serviceName);

    this.election =
      new LeaderElection(zk, serviceName, new ElectionHandler() {
        private ResourceCoordinator coordinator;

        @Override
        public void leader() {
          coordinator = new ResourceCoordinator(zk,
                                                discoveryServiceClient,
                                                new BalancedAssignmentStrategy());
          coordinator.startAndWait();
        }

        @Override
        public void follower() {
          if (coordinator != null) {
            coordinator.stopAndWait();
            coordinator = null;
          }
        }
      });

    this.resourceClient = new ResourceCoordinatorClient(zk);
    this.completion = SettableFuture.create();
  }

  /**
   * Creates an instance of {@link Service} that gets assigned the given partitions.
   * @param partitions partitions to process
   * @return instance of {@link Service}
   */
  protected abstract Service createService(Set<Integer> partitions);

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting ResourceBalancer {} service...", serviceName);

    // We first submit requirement before starting coordinator to make sure all needed paths in ZK are created
    ResourceRequirement requirement =
      ResourceRequirement.builder(serviceName).addPartitions("", partitionCount, 1).build();
    resourceClient.submitRequirement(requirement).get();

    Discoverable discoverable = createDiscoverable(serviceName);
    cancelDiscoverable = discoveryService.register(ResolvingDiscoverable.of(discoverable));

    election.start();
    resourceClient.startAndWait();

    cancelResourceHandler = resourceClient.subscribe(serviceName,
                                                     createResourceHandler(discoverable));
    LOG.info("Started ResourceBalancer {} service...", serviceName);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping ResourceBalancer {} service...", serviceName);
    Throwable throwable = null;
    try {
      Services.chainStop(election, resourceClient).get();
    } catch (Throwable th) {
      throwable = th;
      LOG.error("Exception while shutting down {}.", serviceName, th);
    }
    try {
      cancelResourceHandler.cancel();
    } catch (Throwable th) {
      throwable = th;
      LOG.error("Exception while shutting down {}.", serviceName, th);
    }
    try {
      cancelDiscoverable.cancel();
    } catch (Throwable th) {
      throwable = th;
      LOG.error("Exception while shutting down{}.", serviceName, th);
    }
    if (throwable != null) {
      throw Throwables.propagate(throwable);
    }
    LOG.info("Stopped ResourceBalancer {} service.", serviceName);
  }

  private ResourceHandler createResourceHandler(Discoverable discoverable) {
    return new ResourceHandler(discoverable) {
        private Service service;

        @Override
        public void onChange(Collection<PartitionReplica> partitionReplicas) {
          Set<Integer> partitions = Sets.newHashSet();
          for (PartitionReplica replica : partitionReplicas) {
            partitions.add(Integer.valueOf(replica.getName()));
          }

          LOG.info("Partitions changed {}, service: {}", partitions, serviceName);
          try {
            if (service != null) {
              service.stopAndWait();
            }
            if (partitions.isEmpty() || !election.isRunning()) {
              service = null;
            } else {
              service = createService(partitions);
              service.startAndWait();
            }
          } catch (Throwable t) {
            LOG.error("Failed to change partitions, service: {}.", serviceName, t);
            completion.setException(t);
          }
        }

        @Override
        public void finished(Throwable failureCause) {
          if (service != null) {
            service.stopAndWait();
            service = null;
          }
        }
      };
  }

  private Discoverable createDiscoverable(final String serviceName) {
    InetSocketAddress address;
    // NOTE: at this moment we are not using port anywhere
    int port = Networks.getRandomPort();
    try {
      address = new InetSocketAddress(InetAddress.getLocalHost(), port);
    } catch (UnknownHostException e) {
      address = new InetSocketAddress(port);
    }
    return new Discoverable(serviceName, address);
  }
}
