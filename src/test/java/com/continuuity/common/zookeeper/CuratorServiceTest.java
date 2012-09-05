package com.continuuity.common.zookeeper;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.details.JsonInstanceSerializer;
import com.netflix.curator.x.discovery.details.ServiceDiscoveryImpl;
import junit.framework.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;

/**
 * Tests curator service.
 */
public class CuratorServiceTest {

  @Test
  public void testBasicCuratorService() throws Exception {
    final int TIMEOUT = 10 * 1000;
    List<Closeable> closeables = Lists.newArrayList();
    InMemoryZookeeper server = new InMemoryZookeeper();
    closeables.add(server);

    try {
      CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectionString(), TIMEOUT, TIMEOUT,
        new RetryOneTime(1));
      closeables.add(client);
      client.start();

      ServiceInstance<String> instance = ServiceInstance.<String>builder()
        .payload("testBasicCuratorService")
        .name("awesomeservice")
        .port(1791)
        .build();


      ServiceDiscovery<String> discovery = new ServiceDiscoveryImpl<String>(client, "/awesomeservice",
        new JsonInstanceSerializer<String>(String.class), instance);
      closeables.add(discovery);
      discovery.start();
      Thread.sleep(10000); // This is stupidity. We should have an API to see if this started or no.
      Assert.assertEquals(discovery.queryForInstances("awesomeservice").size(), 1);
      KillZKSession.kill(client.getZookeeperClient().getZooKeeper(), server.getConnectionString());
      Assert.assertEquals(discovery.queryForInstances("awesomeservice").size(), 0);
    } finally {
      Collections.reverse(closeables);
      for(Closeable c : closeables) {
        Closeables.closeQuietly(c);
      }
    }
  }


}
