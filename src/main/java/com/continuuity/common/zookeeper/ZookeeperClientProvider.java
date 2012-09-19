package com.continuuity.common.zookeeper;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.retry.RetryOneTime;

/**
 * A zookeeper client provider.
 */
public final class ZookeeperClientProvider {

    static {
      System.setProperty("curator-log-events", "false");
      System.setProperty("curator-dont-log-connection-problems", "true");
      System.setProperty("zookeeper.disableAutoWatchReset", "true");
    }

    public static CuratorFramework getClient(String connectionString,
                                      int n,
                                      int sleepMsBetweenRetries) {
      CuratorFramework framework
        = CuratorFrameworkFactory.newClient(
                    connectionString,
                    new RetryNTimes(n, sleepMsBetweenRetries)
          );
      framework.start();
      return framework;
    }

    public static CuratorFramework getClient(String connectionString,
                                      int sleepMsBetweenRetries) {
      return getClient(connectionString, 1, sleepMsBetweenRetries);
    }
}
