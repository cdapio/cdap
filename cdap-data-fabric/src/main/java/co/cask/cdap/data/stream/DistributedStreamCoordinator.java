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
package co.cask.cdap.data.stream;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.PropertyStore;
import co.cask.cdap.common.io.Codec;
import co.cask.cdap.common.zookeeper.coordination.BalancedAssignmentStrategy;
import co.cask.cdap.common.zookeeper.coordination.PartitionReplica;
import co.cask.cdap.common.zookeeper.coordination.ResourceCoordinator;
import co.cask.cdap.common.zookeeper.coordination.ResourceCoordinatorClient;
import co.cask.cdap.common.zookeeper.coordination.ResourceHandler;
import co.cask.cdap.common.zookeeper.coordination.ResourceModifier;
import co.cask.cdap.common.zookeeper.coordination.ResourceRequirement;
import co.cask.cdap.common.zookeeper.store.ZKPropertyStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.zookeeper.ZKClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A {@link StreamCoordinator} uses ZooKeeper to implementation coordination needed for stream. It also uses a
 * {@link ResourceCoordinator} to elect each handler as the leader of a set of streams.
 */
@Singleton
public final class DistributedStreamCoordinator extends AbstractStreamCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedStreamCoordinator.class);

  private static final String STREAMS_COORDINATOR = "streams.coordinator";

  private ZKClient zkClient;

  private final DiscoveryServiceClient discoveryServiceClient;
  private final StreamMetaStore streamMetaStore;
  private final ResourceCoordinatorClient resourceCoordinatorClient;

  private LeaderElection leaderElection;
  private ResourceCoordinator resourceCoordinator;
  private Discoverable handlerDiscoverable;
  private Cancellable handlerSubscription;

  @Inject
  public DistributedStreamCoordinator(StreamAdmin streamAdmin, ZKClient zkClient,
                                      DiscoveryServiceClient discoveryServiceClient,
                                      StreamMetaStore streamMetaStore) {
    super(streamAdmin);
    this.zkClient = zkClient;
    this.discoveryServiceClient = discoveryServiceClient;
    this.streamMetaStore = streamMetaStore;
    this.resourceCoordinatorClient = new ResourceCoordinatorClient(zkClient);
    this.handlerDiscoverable = null;
  }

  @Override
  protected void startUp() throws Exception {
    Preconditions.checkNotNull(handlerDiscoverable, "Stream Handler discoverable has not been set");
    resourceCoordinatorClient.startAndWait();
    handlerSubscription = resourceCoordinatorClient.subscribe(handlerDiscoverable.getName(),
                                                              new StreamsLeaderHandler());

    // Start the resource coordinator that will map Streams to Stream handlers
    leaderElection = new LeaderElection(
      zkClient, "/election/" + STREAMS_COORDINATOR, new ElectionHandler() {
      @Override
      public void leader() {
        LOG.info("Became Stream handler leader. Starting resource coordinator.");
        resourceCoordinator = new ResourceCoordinator(zkClient, discoveryServiceClient,
                                                      new BalancedAssignmentStrategy());
        resourceCoordinator.startAndWait();

        resourceCoordinatorClient.modifyRequirement(Constants.Service.STREAMS, new ResourceModifier() {
          @Nullable
          @Override
          public ResourceRequirement apply(@Nullable ResourceRequirement existingRequirement) {
            try {
              // Create one requirement for the resource coordinator for all the streams.
              // One stream is identified by one partition
              ResourceRequirement.Builder builder = ResourceRequirement.builder(Constants.Service.STREAMS);
              for (StreamSpecification spec : streamMetaStore.listStreams()) {
                LOG.debug("Adding {} stream as a resource to the coordinator to manager streams leaders.",
                          spec.getName());
                builder.addPartition(new ResourceRequirement.Partition(spec.getName(), 1));
              }
              return builder.build();
            } catch (Throwable e) {
              LOG.error("Could not create requirement for coordinator in Stream handler leader", e);
              Throwables.propagate(e);
              return null;
            }
          }
        });
      }

      @Override
      public void follower() {
        LOG.info("Became Stream handler follower.");
        if (resourceCoordinator != null) {
          resourceCoordinator.stopAndWait();
        }
      }
    });
  }

  @Override
  protected void doShutDown() throws Exception {
    // revoke subscription to resource coordinator
    if (leaderElection != null) {
      Uninterruptibles.getUninterruptibly(leaderElection.stop(), 5, TimeUnit.SECONDS);
    }

    if (handlerSubscription != null) {
      handlerSubscription.cancel();
    }

    if (resourceCoordinatorClient != null) {
      resourceCoordinatorClient.stopAndWait();
    }
  }

  @Override
  protected <T> PropertyStore<T> createPropertyStore(Codec<T> codec) {
    return ZKPropertyStore.create(zkClient, "/" + Constants.Service.STREAMS + "/properties", codec);
  }

  @Override
  public ListenableFuture<Void> streamCreated(final String streamName) {
    // modify the requirement to add the new stream as a new partition of the existing requirement
    ListenableFuture<ResourceRequirement> future = resourceCoordinatorClient.modifyRequirement(
      Constants.Service.STREAMS, new ResourceModifier() {
        @Nullable
        @Override
        public ResourceRequirement apply(@Nullable ResourceRequirement existingRequirement) {
          Set<ResourceRequirement.Partition> partitions;
          if (existingRequirement != null) {
            partitions = existingRequirement.getPartitions();
          } else {
            partitions = ImmutableSet.of();
          }

          ResourceRequirement.Partition newPartition = new ResourceRequirement.Partition(streamName, 1);
          if (partitions.contains(newPartition)) {
            return null;
          }

          ResourceRequirement.Builder builder = ResourceRequirement.builder(Constants.Service.STREAMS);
          builder.addPartition(newPartition);
          for (ResourceRequirement.Partition partition : partitions) {
            builder.addPartition(partition);
          }
          return builder.build();
        }
      });
    return Futures.transform(future, Functions.<Void>constant(null));
  }

  @Override
  public void setHandlerDiscoverable(Discoverable discoverable) {
    handlerDiscoverable = discoverable;
  }


  /**
   * Class that defines the behavior of a leader of a collection of Streams.
   */
  private final class StreamsLeaderHandler extends ResourceHandler {

    protected StreamsLeaderHandler() {
      super(handlerDiscoverable);
    }

    @Override
    public void onChange(Collection<PartitionReplica> partitionReplicas) {
      // TODO Use the names of the ParitionReplicas to retrieve the streams names and do something on them.
      // TODO Here, we are in the master for all the streams represented by the partitions replicas. We need to do
      // aggregation, etc.
    }

    @Override
    public void finished(Throwable failureCause) {
      if (failureCause != null) {
        LOG.error("Finished with failure for Stream handler instance {}", handlerDiscoverable.getName(), failureCause);
      }
    }
  }
}
