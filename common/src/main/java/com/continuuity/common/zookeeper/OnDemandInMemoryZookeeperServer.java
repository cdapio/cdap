package com.continuuity.common.zookeeper;

import com.continuuity.common.utils.PortDetector;

import java.io.IOException;

/**
 * Thread safe, lazy instantiator idiom.
 */
public final class OnDemandInMemoryZookeeperServer {
  private static volatile InMemoryZookeeper inMemoryZookeeper = null;

  public static InMemoryZookeeper getSingletonInstance() throws IOException, InterruptedException {
    InMemoryZookeeper result = inMemoryZookeeper;
    if (result == null) {
      synchronized (OnDemandInMemoryZookeeperServer.class) {
        result = inMemoryZookeeper;
        if (result == null) {
          inMemoryZookeeper = result = createInstance();
        }
      }
    }
    return result;
  }

  private static InMemoryZookeeper createInstance() throws IOException, InterruptedException {
    int port = PortDetector.findFreePort();
    return new InMemoryZookeeper(port);
  }
}

