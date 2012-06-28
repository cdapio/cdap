package com.continuuity.common.service.distributed;

import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.records.Container;

/**
 * Handles connecting to the container manager (M) that is responsible for a specific container (C) instance.
 */
public interface ContainerManagerConnectionHandler {
  ContainerManager connect(Container container);
  ContainerManager get(String contanerIpAndPort);
}
