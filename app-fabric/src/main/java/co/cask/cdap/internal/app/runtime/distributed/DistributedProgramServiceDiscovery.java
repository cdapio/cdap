/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.ProgramServiceDiscovery;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;

/**
 * Distributed ProgramServiceDiscovery which implements the discovery of User Service.
 */
public class DistributedProgramServiceDiscovery implements ProgramServiceDiscovery {

  // A loading cache so that only one instance of DiscoveryServiceClient is created per service.
  // This is needed to minimize number of ZK watches.
  private final LoadingCache<Id.Program, DiscoveryServiceClient> discoveredCache;

  @Inject
  public DistributedProgramServiceDiscovery(CConfiguration cConf, ZKClient zkClient) {
    final ZKClient twillZKClient = ZKClients.namespace(zkClient, cConf.get(Constants.CFG_TWILL_ZK_NAMESPACE));
    this.discoveredCache = CacheBuilder.newBuilder().build(new CacheLoader<Id.Program, DiscoveryServiceClient>() {
      @Override
      public DiscoveryServiceClient load(Id.Program programId) throws Exception {
        // A twill service started by Reactor would have Twill applicationId = [type].[accountId].[appId].[programId]
        String namespace = String.format("/%s.%s.%s.%s",
                                         ProgramType.SERVICE.name().toLowerCase(),
                                         programId.getAccountId(), programId.getApplicationId(), programId.getId());
        return new ZKDiscoveryService(ZKClients.namespace(twillZKClient, namespace));
      }
    });
  }

  @Override
  public ServiceDiscovered discover(String accountId, String appId, String serviceId, String serviceName) {
    return discoveredCache.getUnchecked(Id.Program.from(accountId, appId, serviceId)).discover(serviceName);
  }
}
