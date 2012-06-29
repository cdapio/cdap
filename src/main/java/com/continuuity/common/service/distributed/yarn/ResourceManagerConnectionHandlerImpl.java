package com.continuuity.common.service.distributed.yarn;

import com.continuuity.common.service.distributed.ResourceManagerConnectionHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 *
 *
 */
public class ResourceManagerConnectionHandlerImpl implements ResourceManagerConnectionHandler<AMRMProtocol> {
  private static Logger Log = LoggerFactory.getLogger(ResourceManagerConnectionHandlerImpl.class);

  private final Configuration configuration;
  private final YarnRPC rpc;

  public ResourceManagerConnectionHandlerImpl(Configuration configuration) {
    this.configuration = configuration;
    this.rpc = YarnRPC.create(configuration);
  }

  @Override
  public AMRMProtocol connect() {
    YarnConfiguration yarnConf = new YarnConfiguration(configuration);
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
      YarnConfiguration.RM_SCHEDULER_ADDRESS,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
    Log.info("Connecting to ResourceManager at " + rmAddress);
    return ((AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, configuration));
  }
}
