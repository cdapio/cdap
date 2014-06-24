package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.internal.app.runtime.ProgramServiceDiscovery;
import com.google.inject.Inject;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClients;

/**
 * Distributed ProgramServiceDiscovery which implements the discovery of User Service.
 */
public class DistributedProgramServiceDiscovery implements ProgramServiceDiscovery {

  private CConfiguration cConf;
  private ZKClientService zkClientService;

  @Inject
  public DistributedProgramServiceDiscovery(CConfiguration cConf, ZKClientService zkClientService) {
    this.cConf = cConf;
    this.zkClientService = zkClientService;
  }

  @Override
  public ServiceDiscovered discover(String accountId, String appId, String serviceId, String serviceName) {
    String twillNamespace = cConf.get(Constants.CFG_TWILL_ZK_NAMESPACE);
    String zkNamespace = String.format("%s/service.%s.%s.%s", twillNamespace, accountId, appId, serviceId);
    ZKDiscoveryService zkDiscoveryService = new ZKDiscoveryService(ZKClients.namespace(zkClientService, zkNamespace));
    return zkDiscoveryService.discover(serviceName);
  }
}
