/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.distributed.remote;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.stream.StreamSupport;

/**
 * Unit test for {@link RemoteExecutionDiscoveryService}.
 */
public class RemoteExecutionDiscoveryServiceTest {

  @Test
  public void testDiscovery() {
    CConfiguration cConf = CConfiguration.create();
    RemoteExecutionDiscoveryService discoveryService = new RemoteExecutionDiscoveryService(cConf);

    // Without registering, the service discovered should contain one
    // entry with the host name the same as the service name
    ServiceDiscovered serviceDiscovered = discoveryService.discover("test");
    Assert.assertTrue(
      StreamSupport.stream(serviceDiscovered.spliterator(), false)
        .map(Discoverable::getSocketAddress)
        .allMatch(addr -> InetSocketAddress.createUnresolved("test", 0).equals(addr))
    );

    // Explicitly register an endpoint, this should override the previous one
    Cancellable cancellable = discoveryService.register(
      new Discoverable("test", InetSocketAddress.createUnresolved("xyz", 12345)));
    Assert.assertTrue(
      StreamSupport.stream(serviceDiscovered.spliterator(), false)
        .map(Discoverable::getSocketAddress)
        .allMatch(addr -> InetSocketAddress.createUnresolved("xyz", 12345).equals(addr))
    );

    // Cancel the registration,
    // it should resort back to having discoverable with address host the same as the discovery name
    cancellable.cancel();
    Assert.assertTrue(
      StreamSupport.stream(serviceDiscovered.spliterator(), false)
        .map(Discoverable::getSocketAddress)
        .allMatch(addr -> InetSocketAddress.createUnresolved("test", 0).equals(addr))
    );
  }

  @Test
  public void testExplicitConf() {
    CConfiguration cConf = CConfiguration.create();
    RemoteExecutionDiscoveryService discoveryService = new RemoteExecutionDiscoveryService(cConf);

    // Register, an entry should be added to the cConf
    discoveryService.register(new Discoverable("test", InetSocketAddress.createUnresolved("xyz", 12345)));
    Assert.assertNotNull(cConf.get(Constants.RuntimeMonitor.DISCOVERY_SERVICE_PREFIX + "test"));

    // Create a new discoveryService from the same config, discover() should return the address from the config
    ServiceDiscovered serviceDiscovered = new RemoteExecutionDiscoveryService(cConf).discover("test");
    Assert.assertTrue(
      StreamSupport.stream(serviceDiscovered.spliterator(), false)
        .map(Discoverable::getSocketAddress)
        .allMatch(addr -> InetSocketAddress.createUnresolved("xyz", 12345).equals(addr))
    );
  }
}
