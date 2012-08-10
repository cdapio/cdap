package com.continuuity.common.discovery;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.common.zookeeper.InMemoryZKBaseTest;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceType;
import com.netflix.curator.x.discovery.server.entity.ServiceNames;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: nmotgi
 * Date: 8/8/12
 * Time: 5:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class ServiceDiscoveryServerTest extends InMemoryZKBaseTest {
  private static final Logger Log =
    LoggerFactory.getLogger(ServiceDiscoveryServerTest.class);

  @Test
  public void testRegisterService() throws Exception {
    ServicePayload payload = new ServicePayload();
    payload.add("A", "value-A");
    payload.add("B", "value-B");

    ServiceInstance<ServicePayload> service1
      = ServiceInstance.<ServicePayload>builder()
        .name("far")
        .port(12345)
        .payload(payload)
        .serviceType(ServiceType.STATIC)
      .build();

    ServiceInstance<ServicePayload> service2
      = ServiceInstance.<ServicePayload>builder()
      .name("far")
      .port(56789)
      .payload(payload)
      .serviceType(ServiceType.STATIC)
      .build();

    CConfiguration configuration = CConfiguration.create();
    configuration.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, getEnsemble());

    final ServiceDiscoveryServer server =
      new ServiceDiscoveryServer(configuration);
    final int port = PortDetector.findFreePort();

    server.start(port);
    Thread.sleep(2000);

    ClientConfig config = new DefaultClientConfig() {
      @Override
      public Set<Object> getSingletons() {
        Set<Object> singletons = Sets.newHashSet();
        singletons.add(server.getContext());
        singletons.add(server.getServiceNamesMarshaller());
        singletons.add(server.getServiceInstanceMarshaller());
        singletons.add(server.getServiceInstancesMarshaller());
        return singletons;
      }
    };

    Client client = Client.create(config);
    WebResource resource = client.resource("http://localhost:" + port);
    resource.path("/v1/service/far/" +
      service1.getId()).type(MediaType.APPLICATION_JSON_TYPE).put(service1);
    resource.path("/v1/service/far/" +
      service2.getId()).type(MediaType.APPLICATION_JSON_TYPE).put(service2);

    ServiceNames names = resource.path("/v1/service").get(ServiceNames.class);
    Assert.assertEquals(names.getNames(), Lists.newArrayList("far"));
    server.stop();
  }

}
