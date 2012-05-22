package com.continuuity.common.discovery;

import com.continuuity.common.zookeeper.InMemoryZKBaseTest;
import com.google.common.io.Closeables;
import com.netflix.curator.x.discovery.ProviderStrategy;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.strategies.RandomStrategy;
import org.junit.Assert;
import org.junit.Test;

/**
 *  Testing of ServiceDiscoveryClient.
 */
public class ServiceDiscoveryClientTest extends InMemoryZKBaseTest {

  @Test
  public void testRegistration() throws Exception {
    ServiceDiscoveryClient client = new ServiceDiscoveryClient(server.getConnectionString());
    try {
      ServiceDiscoveryClient.ServicePayload payload =
        new ServiceDiscoveryClient.ServicePayload();
      payload.add("A1", "1");
      payload.add("A1", "2");

      client.register("flow-manager", 8080, payload);
      client.register("flow-manager", 8081, payload);
      client.register("flow-manager", 8082, payload);
      int count = client.getProviderCount("flow-manager");
      Assert.assertEquals(3, count);
      client.register("flow-manager", 8083, payload);
      Assert.assertEquals(4, client.getProviderCount("flow-manager"));

      // Same should not increase count, but it does, so we have to be careful.
      client.register("flow-manager", 8083, payload);
      Assert.assertEquals(5, client.getProviderCount("flow-manager"));
    } finally {
      client.close();
    }
  }

    @Test
    public void testRandom() throws Exception {
      ServiceDiscoveryClient client = null;
      try {
        client = new ServiceDiscoveryClient(server.getConnectionString());
        ServiceDiscoveryClient.ServicePayload payload = new ServiceDiscoveryClient.ServicePayload();
        payload.add("A1", "1");
        payload.add("A2", "2");
        client.register("flow-manager", 8080, payload);
        client.register("flow-manager", 8081, payload);
        int count = client.getProviderCount("flow-manager");
        Assert.assertTrue(count == 2);
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        ProviderStrategy<ServiceDiscoveryClient.ServicePayload> strategy =
          new RandomStrategy<ServiceDiscoveryClient.ServicePayload>();
        ServiceDiscoveryClient.ServiceProvider provider = client.getServiceProvider("flow-manager");
        int[] stats = new int[] { 0, 0};
        for(int i = 0; i < 100; ++i) {
          ServiceInstance<ServiceDiscoveryClient.ServicePayload> instance = strategy.getInstance(provider);
          int k = instance.getPort() - 8080;
          stats[k]++;
        }
        int sum = stats[0] + stats[1];
        int diff = Math.abs(stats[0] - stats[1]);
        Assert.assertTrue(diff > 0 && diff < 20);
        Assert.assertTrue(sum == 100);
      } finally {
        if(client != null) {
          Closeables.closeQuietly(client);
        }
      }
    }
}
