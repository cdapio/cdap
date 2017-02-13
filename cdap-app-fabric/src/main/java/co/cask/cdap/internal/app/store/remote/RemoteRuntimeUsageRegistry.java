/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store.remote;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.internal.remote.RemoteOpsClient;
import co.cask.cdap.data2.registry.DatasetUsageKey;
import co.cask.cdap.data2.registry.RuntimeUsageRegistry;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Implementation of RuntimeUsageRegistry, which uses an HTTP Client to execute the actual usage dataset updates in a
 * remote server.
 */
public class RemoteRuntimeUsageRegistry extends RemoteOpsClient implements RuntimeUsageRegistry {

  private final ConcurrentMap<DatasetUsageKey, Boolean> registered = new ConcurrentHashMap<>();

  @Inject
  RemoteRuntimeUsageRegistry(DiscoveryServiceClient discoveryClient) {
    super(discoveryClient, Constants.Service.REMOTE_SYSTEM_OPERATION);
  }

  @Override
  public void registerAll(Iterable<? extends EntityId> users, StreamId streamId) {
    for (EntityId user : users) {
      register(user, streamId);
    }
  }

  @Override
  public void register(EntityId user, StreamId streamId) {
    if (user instanceof ProgramId) {
      register((ProgramId) user, streamId);
    }
  }

  @Override
  public void registerAll(Iterable<? extends EntityId> users, DatasetId datasetId) {
    for (EntityId user : users) {
      register(user, datasetId);
    }
  }

  @Override
  public void register(EntityId user, DatasetId datasetId) {
    if (user instanceof ProgramId) {
      register((ProgramId) user, datasetId);
    }
  }

  @Override
  public void register(ProgramId programId, DatasetId datasetInstanceId) {
    if (alreadyRegistered(datasetInstanceId, programId)) {
      return;
    }
    executeRequest("registerDataset", programId, datasetInstanceId);
  }

  @Override
  public void register(ProgramId programId, StreamId streamId) {
    executeRequest("registerStream", programId, streamId);
  }

  private boolean alreadyRegistered(DatasetId dataset, ProgramId owner) {
    return registered.putIfAbsent(new DatasetUsageKey(dataset, owner), true) != null;
  }
}
