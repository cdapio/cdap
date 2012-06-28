package com.continuuity.common.distributedservice.yarn;

import com.continuuity.common.distributedservice.ContainerManagerConnectionHandler;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 *
 *
 */
public class CMConnectionHandler implements ContainerManagerConnectionHandler {
  private static Logger Log = LoggerFactory.getLogger(CMConnectionHandler.class);
  private final Configuration configuration;
  private final YarnRPC rpc;
  private final Map<String, ContainerManager> containerMgrs = Maps.newHashMap();

  public CMConnectionHandler(Configuration configuration) {
    this.configuration = configuration;
    this.rpc = YarnRPC.create(configuration);
  }

  @Override
  public synchronized ContainerManager connect(Container container) {
    NodeId nodeId = container.getNodeId();
    String containerIpAndPort = String.format("%s:%d", nodeId.getHost(), nodeId.getPort());
    if(!containerMgrs.containsKey(containerIpAndPort)) {
      Log.info("Connecting to container manager at {}", containerIpAndPort);
      InetSocketAddress addr = NetUtils.createSocketAddr(containerIpAndPort);
      ContainerManager containerManager = (ContainerManager) rpc.getProxy(ContainerManager.class, addr, configuration);
      containerMgrs.put(containerIpAndPort, containerManager);
      return containerManager;
    }
    return containerMgrs.get(containerIpAndPort);
  }

  @Override
  public ContainerManager get(String contanerIpAndPort) {
    return containerMgrs.get(contanerIpAndPort);
  }
}
