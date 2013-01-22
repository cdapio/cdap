package com.continuuity.common.zookeeper;

import com.continuuity.common.utils.PortDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread safe, lazy instantiator idiom
 */
public class OnDemandInMemoryZookeeperServer {
  private static final Logger LOG =
    LoggerFactory.getLogger(OnDemandInMemoryZookeeperServer.class);

  private static class InMemoryZookeeperRefHolder {
    private static InMemoryZookeeper inMemoryZookeeper = OnDemandInMemoryZookeeperServer.create();
  }

  public static InMemoryZookeeper getSingletonInstance() {
    return InMemoryZookeeperRefHolder.inMemoryZookeeper;
  }

  private static InMemoryZookeeper create() {
    try {
      int port = PortDetector.findFreePort();
      return new InMemoryZookeeper(port);
    } catch (Exception e) {
      LOG.error("Failed to start zookeeper", e);
    }
    return null;
  }
}

