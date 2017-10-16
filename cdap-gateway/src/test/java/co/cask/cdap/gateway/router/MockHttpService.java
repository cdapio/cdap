/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * A generic http service for testing router.
 */
public class MockHttpService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(MockHttpService.class);

  private final DiscoveryService discoveryService;
  private final String serviceName;
  private final List<HttpHandler> httpHandlers;

  private NettyHttpService httpService;
  private Cancellable cancelDiscovery;

  public MockHttpService(DiscoveryService discoveryService,
                          String serviceName, HttpHandler... httpHandlers) {
    this.discoveryService = discoveryService;
    this.serviceName = serviceName;
    this.httpHandlers = Arrays.asList(httpHandlers);
  }

  @Override
  protected void startUp() throws Exception {
    NettyHttpService.Builder builder = NettyHttpService.builder(MockHttpService.class.getName());
    builder.setHttpHandlers(httpHandlers);
    builder.setHost("localhost");
    builder.setPort(0);
    httpService = builder.build();
    httpService.start();

    registerServer();

    LOG.info("Started test server on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    cancelDiscovery.cancel();
    httpService.stop();
  }

  public void registerServer() {
    // Register services of test server
    LOG.info("Registering service {}", serviceName);
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(new Discoverable(serviceName, httpService.getBindAddress())));
  }
}
