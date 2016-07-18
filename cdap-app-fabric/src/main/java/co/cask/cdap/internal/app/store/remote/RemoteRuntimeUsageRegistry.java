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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.internal.remote.RemoteOpsClient;
import co.cask.cdap.data2.registry.DatasetUsageKey;
import co.cask.cdap.data2.registry.RuntimeUsageRegistry;
import co.cask.cdap.proto.Id;
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
  RemoteRuntimeUsageRegistry(CConfiguration cConf, DiscoveryServiceClient discoveryClient) {
    super(cConf, discoveryClient);
  }

  @Override
  public void registerAll(Iterable<? extends Id> users, Id.Stream streamId) {
    for (Id user : users) {
      register(user, streamId);
    }
  }

  @Override
  public void register(Id user, Id.Stream streamId) {
    if (user instanceof Id.Program) {
      register((Id.Program) user, streamId);
    }
  }

  @Override
  public void registerAll(Iterable<? extends Id> users, Id.DatasetInstance datasetId) {
    for (Id user : users) {
      register(user, datasetId);
    }
  }

  @Override
  public void register(Id user, Id.DatasetInstance datasetId) {
    if (user instanceof Id.Program) {
      register((Id.Program) user, datasetId);
    }
  }

  @Override
  public void register(Id.Program programId, Id.DatasetInstance datasetInstanceId) {
    if (alreadyRegistered(datasetInstanceId, programId)) {
      return;
    }
    executeRequest("registerDataset", programId, datasetInstanceId);
  }

  @Override
  public void register(Id.Program programId, Id.Stream streamId) {
    executeRequest("registerStream", programId, streamId);
  }

  private boolean alreadyRegistered(Id.DatasetInstance dataset, Id.Program owner) {
    return registered.putIfAbsent(new DatasetUsageKey(dataset, owner), true) != null;
  }
}
