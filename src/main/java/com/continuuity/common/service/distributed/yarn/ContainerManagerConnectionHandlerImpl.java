package com.continuuity.common.service.distributed.yarn;

import com.continuuity.common.service.distributed.ContainerManagerConnectionHandler;
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
 * Connection handler for managing connections to containers allocated.
 */
public class ContainerManagerConnectionHandlerImpl implements ContainerManagerConnectionHandler {
  private static Logger Log = LoggerFactory.getLogger(ContainerManagerConnectionHandlerImpl.class);

  /**
   * Instance of configuration.
   */
  private final Configuration configuration;

  /**
   * Instance of YarnRPC.
   */
  private final YarnRPC rpc;

  /**
   * Maps of container ids to container.
   */
  private final Map<String, ContainerManager> containerMgrs = Maps.newHashMap();

  public ContainerManagerConnectionHandlerImpl(Configuration configuration) {
    this.configuration = configuration;
    this.rpc = YarnRPC.create(configuration);
  }

  /**
   * Connects to the container manager on which the container would be run.
   *
   * @param container for which we container manager is requested.
   * @return ContainerManager associated with the container.
   */
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
