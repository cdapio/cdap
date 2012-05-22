package com.continuuity.common.discovery;

import com.continuuity.common.zookeeper.InMemoryZKBaseTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 *  Testing of ServiceDiscoveryClient.
 */
public class ServiceDiscoveryClientTest extends InMemoryZKBaseTest {

  @Test
  public void testRegistration() throws Exception {
    ServiceDiscoveryClient client = new ServiceDiscoveryClient(server.getConnectionString());
    try {
      client.start();

      Map<String, String> payload = Maps.newHashMap();
      payload.put("A1", "1");
      payload.put("A1", "2");

      client.register("flow-manager", 8080, ImmutableMap.copyOf(payload));
      client.register("flow-manager", 8081, ImmutableMap.copyOf(payload));
      client.register("flow-manager", 8082, ImmutableMap.copyOf(payload));
      int count = client.getProviderCount("flow-manager");
      Assert.assertEquals(3, count);
      client.register("flow-manager", 8083, ImmutableMap.copyOf(payload));
      Assert.assertEquals(4, client.getProviderCount("flow-manager"));

      // Same should not increase count, but it does, so we have to be careful.
      client.register("flow-manager", 8083, ImmutableMap.copyOf(payload));
      Assert.assertEquals(5, client.getProviderCount("flow-manager"));
    } finally {
      client.close();
    }
  }


  //  @Test
  //  public void testRandom() throws Exception {
  //    ServiceDiscoveryClient client = new ServiceDiscoveryClient(server.getConnectionString());
  //    ProviderStrategy<Map<String, String>> strategy = new RandomStrategy<Map<String, String>>();
  //    ServiceDiscoveryClient.ServiceProvider provider = new ServiceDiscoveryClient.ServiceProvider("flow-manager");
  //    try {
  //      client.start();
  //
  //    } finally {
  //      client.close();
  //    }
  //  }
}
