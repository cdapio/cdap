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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.PropertyStore;
import co.cask.cdap.common.io.Codec;
import co.cask.cdap.common.zookeeper.coordination.ResourceCoordinator;
import co.cask.cdap.common.zookeeper.coordination.ResourceCoordinatorClient;
import co.cask.cdap.common.zookeeper.coordination.ResourceModifier;
import co.cask.cdap.common.zookeeper.coordination.ResourceRequirement;
import co.cask.cdap.common.zookeeper.store.ZKPropertyStore;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.internal.zookeeper.ReentrantDistributedLock;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;

/**
 * A {@link StreamCoordinatorClient} uses ZooKeeper to implementation coordination needed for stream. It also uses a
 * {@link ResourceCoordinator} to elect each handler as the leader of a set of streams.
 */
@Singleton
public final class DistributedStreamCoordinatorClient extends AbstractStreamCoordinatorClient {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedStreamCoordinatorClient.class);

  private final ResourceCoordinatorClient resourceCoordinatorClient;
  private final ZKClient zkClient;

  @Inject
  public DistributedStreamCoordinatorClient(ZKClient zkClient) {
    super();
    this.zkClient = zkClient;
    this.resourceCoordinatorClient = new ResourceCoordinatorClient(getCoordinatorZKClient());
  }

  @Override
  protected void doStartUp() throws Exception {
    resourceCoordinatorClient.startAndWait();
  }

  @Override
  protected void doShutDown() throws Exception {
    if (resourceCoordinatorClient != null) {
      resourceCoordinatorClient.stopAndWait();
    }
  }

  @Override
  protected <T> PropertyStore<T> createPropertyStore(Codec<T> codec) {
    return ZKPropertyStore.create(zkClient, "/" + Constants.Service.STREAMS + "/properties", codec);
  }

  @Override
  protected Lock getLock(Id.Stream streamId) {
    // It's ok to create new locks every time as it's backed by ZK for distributed lock
    ZKClient lockZKClient = ZKClients.namespace(zkClient, "/" + Constants.Service.STREAMS + "/locks");
    return new ReentrantDistributedLock(lockZKClient, streamId.toId());
  }

  @Override
  protected void streamCreated(final Id.Stream streamId) {
    resourceCoordinatorClient.modifyRequirement(
      Constants.Service.STREAMS, new ResourceModifier() {
        @Nullable
        @Override
        public ResourceRequirement apply(@Nullable ResourceRequirement existingRequirement) {
          LOG.debug("Modifying requirement to add stream {} as a resource", streamId);
          Set<ResourceRequirement.Partition> partitions;
          if (existingRequirement != null) {
            partitions = existingRequirement.getPartitions();
          } else {
            partitions = ImmutableSet.of();
          }

          ResourceRequirement.Partition newPartition = new ResourceRequirement.Partition(streamId.toId(), 1);
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
  }

  private ZKClient getCoordinatorZKClient() {
    return ZKClients.namespace(zkClient, Constants.Stream.STREAM_ZK_COORDINATION_NAMESPACE);
  }
}
