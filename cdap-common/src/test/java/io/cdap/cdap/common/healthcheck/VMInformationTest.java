/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.healthcheck;

import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.internal.remote.NoOpInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.http.NettyHttpService;
import java.util.Set;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link VMInformation}.
 */
public class VMInformationTest {

  @Test
  public void testVMInformation() throws Exception {
    assertVMInformation(VMInformation.collect());
  }

  @Test
  public void testVMInformationHandler() throws Exception {
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();

    NettyHttpService httpService = NettyHttpService.builder("test")
      .setHttpHandlers(new VMInformationHandler(new ContextAccessEnforcer() {
        @Override
        public void enforce(EntityId entity, Set<? extends Permission> permissions) {
          // no-op
        }

        @Override
        public void enforceOnParent(EntityType entityType, EntityId parentId, Permission permission) {
          // no-op
        }

        @Override
        public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds) {
          return entityIds;
        }
      }))
      .build();

    httpService.start();
    try {
      discoveryService.register(
        ResolvingDiscoverable.of(URIScheme.HTTP.createDiscoverable("test", httpService.getBindAddress())));

      VMInformationFetcher fetcher = new VMInformationFetcher(new RemoteClientFactory(discoveryService,
                                                                                      new NoOpInternalAuthenticator()));
      assertVMInformation(fetcher.getVMInformation("test"));
    } finally {
      httpService.stop();
    }
  }

  private void assertVMInformation(VMInformation info) {
    Assert.assertNotNull(info);
    Assert.assertNotNull(info.getHeapMemoryUsage());
    Assert.assertNotNull(info.getNonHeapMemoryUsage());
    Assert.assertNotNull(info.getThreads());
  }
}
