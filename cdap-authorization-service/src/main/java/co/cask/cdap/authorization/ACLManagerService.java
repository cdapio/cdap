/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.authorization;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.common.authorization.ACLStore;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;

import java.net.InetSocketAddress;

/**
 * Manages authorization and exposes endpoints for clients to verify authorized access.
 */
public class ACLManagerService extends AbstractIdleService {

  public static final String DISCOVERABLE_NAME = "cdap.authorization.acl.manager";

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;

  private Cancellable cancelDiscovery;

  public ACLManagerService(CConfiguration configuration, DiscoveryService discoveryService, ACLStore aclStore) {
    this.discoveryService = discoveryService;
    this.httpService = new CommonNettyHttpServiceBuilder(configuration)
      .addHttpHandlers(ImmutableList.of(new ACLManagerHandler(aclStore)))
      .build();
  }

  @Override
  protected void startUp() throws Exception {
    httpService.startAndWait();
    cancelDiscovery = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return DISCOVERABLE_NAME;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    });
  }

  @Override
  protected void shutDown() throws Exception {
    cancelDiscovery.cancel();
    httpService.stopAndWait();
  }
}
