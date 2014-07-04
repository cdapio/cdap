package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.Id;
import com.continuuity.app.program.Type;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.internal.app.runtime.ProgramServiceDiscovery;
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
                                         Type.SERVICE.name().toLowerCase(),
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
