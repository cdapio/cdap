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
package co.cask.cdap.metrics.runtime;

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.common.zookeeper.coordination.BalancedAssignmentStrategy;
import co.cask.cdap.common.zookeeper.coordination.PartitionReplica;
import co.cask.cdap.common.zookeeper.coordination.ResourceCoordinator;
import co.cask.cdap.common.zookeeper.coordination.ResourceCoordinatorClient;
import co.cask.cdap.common.zookeeper.coordination.ResourceHandler;
import co.cask.cdap.common.zookeeper.coordination.ResourceRequirement;
import co.cask.cdap.metrics.process.KafkaMetricsProcessorServiceFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.Services;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

/**
 * Metrics processor service that processes events from Kafka.
 */
public final class KafkaMetricsProcessorService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsProcessorService.class);
  private static final String SERVICE_NAME = "metrics.processor.consumer";

  private final CConfiguration conf;
  private final LeaderElection election;
  private final ResourceCoordinatorClient resourceClient;
  private final DiscoveryService discoveryService;
  private final KafkaMetricsProcessorServiceFactory metricsProcessorFactory;

  private final SettableFuture<?> completion;

  private Cancellable cancelDiscoverable;
  private Cancellable cancelResourceHandler;

  @Nullable
  private MetricsContext metricsContext;

  @Inject
  public KafkaMetricsProcessorService(CConfiguration conf,
                                      final ZKClientService zkClient,
                                      DiscoveryService discoveryService,
                                      final DiscoveryServiceClient discoveryServiceClient,
                                      KafkaMetricsProcessorServiceFactory metricsProcessorFactory) {
    this.conf = conf;
    this.metricsProcessorFactory = metricsProcessorFactory;
    this.discoveryService = discoveryService;

    this.election =
      new LeaderElection(zkClient, SERVICE_NAME, new ElectionHandler() {
        private ResourceCoordinator coordinator;

        @Override
        public void leader() {
          coordinator = new ResourceCoordinator(zkClient,
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

    this.resourceClient = new ResourceCoordinatorClient(zkClient);
    this.completion = SettableFuture.create();
  }

  public void setMetricsContext(MetricsContext metricsContext) {
    this.metricsContext = metricsContext;
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
  protected void startUp() throws Exception {
    LOG.info("Starting Metrics Processor ...");

    Discoverable discoverable = createDiscoverable(SERVICE_NAME);
    cancelDiscoverable = discoveryService.register(ResolvingDiscoverable.of(discoverable));

    election.start();
    resourceClient.startAndWait();

    int partitionSize = conf.getInt(Constants.Metrics.KAFKA_PARTITION_SIZE,
                                    Constants.Metrics.DEFAULT_KAFKA_PARTITION_SIZE);
    ResourceRequirement requirement =
      // kafka partition id is the name of the partition
      ResourceRequirement.builder(SERVICE_NAME).addPartitions("", partitionSize, 1).build();
    resourceClient.submitRequirement(requirement).get();

    cancelResourceHandler =
      resourceClient.subscribe(SERVICE_NAME, createResourceHandler(metricsProcessorFactory, discoverable));
  }

  @Override
  protected void run() throws Exception {
    completion.get();
  }

  @Override
  protected void triggerShutdown() {
    completion.set(null);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Metrics Processor ...");
    Throwable throwable = null;
    try {
      Services.chainStop(election, resourceClient).get();
    } catch (Throwable th) {
      throwable = th;
      LOG.error("Exception while shutting down.", th);
    }
    try {
      cancelResourceHandler.cancel();
    } catch (Throwable th) {
      throwable = th;
      LOG.error("Exception while shutting down.", th);
    }
    try {
      cancelDiscoverable.cancel();
    } catch (Throwable th) {
      throwable = th;
      LOG.error("Exception while shutting down.", th);
    }
    if (throwable != null) {
      throw Throwables.propagate(throwable);
    }
  }

  private ResourceHandler createResourceHandler(final KafkaMetricsProcessorServiceFactory factory,
                                                Discoverable discoverable) {
    return new ResourceHandler(discoverable) {
        private co.cask.cdap.metrics.process.KafkaMetricsProcessorService service;

        @Override
        public void onChange(Collection<PartitionReplica> partitionReplicas) {
          Set<Integer> partitions = Sets.newHashSet();
          for (PartitionReplica replica : partitionReplicas) {
            // kafka partition id is the name of the partition
            partitions.add(Integer.valueOf(replica.getName()));
          }

          LOG.info("Metrics Kafka partition changed {}", partitions);
          try {
            if (service != null) {
              service.stopAndWait();
            }
            if (partitions.isEmpty() || !election.isRunning()) {
              service = null;
            } else {
              service = factory.create(partitions);
              service.setMetricsContext(metricsContext);
              service.startAndWait();
            }
          } catch (Throwable t) {
            LOG.error("Failed to change Kafka partition.", t);
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
