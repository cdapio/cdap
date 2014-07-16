/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.gateway.router;

import com.continuuity.http.HttpHandler;
import com.continuuity.http.NettyHttpService;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * A generic http service for testing router.
 */
public class MockHttpService extends AbstractIdleService {
  private static final Logger log = LoggerFactory.getLogger(MockHttpService.class);

  private final DiscoveryService discoveryService;
  private final String serviceName;
  private final Set<HttpHandler> httpHandlers;

  private NettyHttpService httpService;
  private Cancellable cancelDiscovery;

  public MockHttpService(DiscoveryService discoveryService,
                          String serviceName, HttpHandler... httpHandlers) {
    this.discoveryService = discoveryService;
    this.serviceName = serviceName;
    this.httpHandlers = ImmutableSet.copyOf(httpHandlers);
  }

  @Override
  protected void startUp() throws Exception {
    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(httpHandlers);
    builder.setHost("localhost");
    builder.setPort(0);
    httpService = builder.build();
    httpService.startAndWait();

    registerServer();

    log.info("Started test server on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    cancelDiscovery.cancel();
    httpService.stopAndWait();
  }

  public void registerServer() {
    // Register services of test server
    log.info("Registering service {}", serviceName);
    cancelDiscovery = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return serviceName;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    });
  }
}
