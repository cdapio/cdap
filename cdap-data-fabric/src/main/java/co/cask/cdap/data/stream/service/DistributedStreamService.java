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

import co.cask.cdap.common.zookeeper.coordination.ResourceCoordinator;
import co.cask.cdap.common.zookeeper.coordination.ResourceCoordinatorClient;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamLeaderListener;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.zookeeper.ZKClient;

import java.util.Set;

/**
 *
 */
public class DistributedStreamService extends AbstractStreamService {

  private final ZKClient zkClient;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final StreamMetaStore streamMetaStore;
  private final ResourceCoordinatorClient resourceCoordinatorClient;
  private final Set<StreamLeaderListener> leaderListeners;

  private LeaderElection leaderElection;
  private ResourceCoordinator resourceCoordinator;
  private Discoverable handlerDiscoverable;
  private Cancellable handlerSubscription;


  @Inject
  public DistributedStreamService(StreamCoordinatorClient streamCoordinatorClient,
                                  StreamFileJanitorService janitorService,
                                  StreamWriterSizeManager sizeManager,
                                  ZKClient zkClient,
                                  DiscoveryServiceClient discoveryServiceClient,
                                  StreamMetaStore streamMetaStore) {
    super(streamCoordinatorClient, janitorService, sizeManager);
    this.zkClient = zkClient;
    this.discoveryServiceClient = discoveryServiceClient;
    this.streamMetaStore = streamMetaStore;
  }
}
