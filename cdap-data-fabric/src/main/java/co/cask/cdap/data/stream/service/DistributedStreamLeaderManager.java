/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.zookeeper.coordination.BalancedAssignmentStrategy;
import co.cask.cdap.common.zookeeper.coordination.PartitionReplica;
import co.cask.cdap.common.zookeeper.coordination.ResourceCoordinator;
import co.cask.cdap.common.zookeeper.coordination.ResourceCoordinatorClient;
import co.cask.cdap.common.zookeeper.coordination.ResourceHandler;
import co.cask.cdap.common.zookeeper.coordination.ResourceModifier;
import co.cask.cdap.common.zookeeper.coordination.ResourceRequirement;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
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
import javax.annotation.Nullable;

/**
 * Distributed implementation of the {@link StreamLeaderManager} that uses a {@link ResourceCoordinator} to
 * affect Streams fairly among Stream handlers. One Stream handler will be the leader of a collection of streams.
 * This ensures that not only one stream handler gets overloaded with this task.
 */
public class DistributedStreamLeaderManager extends AbstractIdleService implements StreamLeaderManager {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedStreamLeaderManager.class);

  public static final String STREAMS_COORDINATOR = "streams.coordinator";

  private static final String STREAMS_RESOURCE = "streams";

  private final CConfiguration cConf;
  private final ZKClient zkClient;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final StreamMetaStore streamMetaStore;
  private final ResourceCoordinatorClient streamsResourceCoordinatorClient;

  private LeaderElection leaderElection;
  private ResourceCoordinator streamsResourceCoordinator;
  private Discoverable handlerDiscoverable;
  private Cancellable handlerSubscription;

  @Inject
  public DistributedStreamLeaderManager(CConfiguration cConf, ZKClient zkClient,
                                        DiscoveryServiceClient discoveryServiceClient,
                                        StreamMetaStore streamMetaStore) {
    this.cConf = cConf;
    this.zkClient = zkClient;
    this.discoveryServiceClient = discoveryServiceClient;
    this.streamMetaStore = streamMetaStore;
    this.streamsResourceCoordinatorClient = new ResourceCoordinatorClient(zkClient);
    this.handlerDiscoverable = null;
  }

  @Override
  protected void startUp() throws Exception {
    Preconditions.checkNotNull(handlerDiscoverable, "Stream Handler discoverable has not been set");
    streamsResourceCoordinatorClient.startAndWait();

    // Start the resource coordinator that will map Streams to Stream handlers
    leaderElection = new LeaderElection(
      zkClient, "/election/" + STREAMS_COORDINATOR, new ElectionHandler() {
      @Override
      public void leader() {
        LOG.info("Became Stream handler leader. Starting resource coordinator.");
        streamsResourceCoordinator = new ResourceCoordinator(zkClient, discoveryServiceClient,
                                                             new BalancedAssignmentStrategy());
        streamsResourceCoordinator.startAndWait();

        try {
          // Create one requirement for the resource coordinator for all the streams.
          // One stream is identified by one partition
          ResourceRequirement.Builder builder = ResourceRequirement.builder(STREAMS_RESOURCE);
          for (StreamSpecification spec : streamMetaStore.listStreams()) {
            LOG.debug("Adding {} stream as a resource to the coordinator to manager streams leaders.", spec.getName());
            builder.addPartition(new ResourceRequirement.Partition(spec.getName(), 1));
          }
          streamsResourceCoordinatorClient.submitRequirement(builder.build()).get();
        } catch (Throwable e) {
          LOG.error("Could not create requirement for coordinator in Stream handler leader", e);
          Throwables.propagate(e);
        }
      }

      @Override
      public void follower() {
        LOG.info("Became Stream handler follower.");
        if (streamsResourceCoordinator != null) {
          streamsResourceCoordinator.stopAndWait();
        }
      }
    });

    handlerSubscription = streamsResourceCoordinatorClient.subscribe(handlerDiscoverable.getName(),
                                                                     new StreamsLeaderHandler());
  }

  @Override
  protected void shutDown() throws Exception {
    // revoke subscription to coordinator
    if (leaderElection != null) {
      leaderElection.stopAndWait();
    }

    if (handlerSubscription != null) {
      handlerSubscription.cancel();
    }

    if (streamsResourceCoordinatorClient != null) {
      streamsResourceCoordinatorClient.stopAndWait();
    }
  }

  @Override
  public void setHandlerDiscoverable(Discoverable discoverable) {
    handlerDiscoverable = discoverable;
  }

  @Override
  public ListenableFuture<Void> affectLeader(final String streamName) {
    // modify the requirement to add the new stream as a new partition of the existing requirement
    ListenableFuture<ResourceRequirement> future = streamsResourceCoordinatorClient.modifyRequirement(
      STREAMS_RESOURCE, new ResourceModifier() {
        @Nullable
        @Override
        public ResourceRequirement apply(@Nullable ResourceRequirement existingRequirement) {
          Set<ResourceRequirement.Partition> partitions = existingRequirement.getPartitions();
          ResourceRequirement.Partition newParition = new ResourceRequirement.Partition(streamName, 1);
          if (partitions.contains(newParition)) {
            return null;
          }

          ResourceRequirement.Builder builder = ResourceRequirement.builder(existingRequirement.getName());
          builder.addPartition(newParition);
          for (ResourceRequirement.Partition partition : partitions) {
            builder.addPartition(partition);
          }
          return builder.build();
        }
      });
    return Futures.transform(future, Functions.<Void>constant(null));
  }

  /**
   * Class that defines the bahavior of a leader of a collection of Streams.
   */
  private final class StreamsLeaderHandler extends ResourceHandler {

    protected StreamsLeaderHandler() {
      super(handlerDiscoverable);
    }

    @Override
    public void onChange(Collection<PartitionReplica> partitionReplicas) {
      // TODO use the names of the ParitionReplicas to retrieve the streams names and do something on them
      // TODO here, we are in the master for all the streams represented by the partitions replicas. We need to do
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
