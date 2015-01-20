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
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.zookeeper.ZKClient;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * A {@link StreamCoordinatorClient} uses ZooKeeper to implementation coordination needed for stream. It also uses a
 * {@link ResourceCoordinator} to elect each handler as the leader of a set of streams.
 */
@Singleton
public final class DistributedStreamCoordinatorClient extends AbstractStreamCoordinatorClient {

  private final ResourceCoordinatorClient resourceCoordinatorClient;
  private final ZKClient zkClient;

  @Inject
  public DistributedStreamCoordinatorClient(StreamAdmin streamAdmin, ZKClient zkClient) {
    super(streamAdmin);
    this.zkClient = zkClient;
    this.resourceCoordinatorClient = new ResourceCoordinatorClient(zkClient);
  }

  @Override
  protected void startUp() throws Exception {
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
}
