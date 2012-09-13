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
      CuratorFramework client = ZookeeperClientProvider.getClient(
        server.getConnectionString(),
        1, TIMEOUT
      );
      closeables.add(client);

      ServiceInstance<String> instance = ServiceInstance.<String>builder()
        .payload("testBasicCuratorService")
        .name("awesomeservice")
        .port(1791)
        .build();


      ServiceDiscovery<String> discovery =
          new ServiceDiscoveryImpl<String>(client, "/awesomeservice",
        new JsonInstanceSerializer<String>(String.class), instance);
      closeables.add(discovery);
      discovery.start();

      Assert.assertEquals(1,
          discovery.queryForInstances("awesomeservice").size());

      //ENG-538 has been opened to investigate this issue.
//      KillZKSession.kill(
//          client.getZookeeperClient().getZooKeeper(),
//          server.getConnectionString());
//      Thread.sleep(500); // without this the test fails intermittently
//      Assert.assertEquals(0,
//          discovery.queryForInstances("awesomeservice").size());

    } finally {
      Collections.reverse(closeables);
      for(Closeable c : closeables) {
        Closeables.closeQuietly(c);
      }
    }
  }

}
