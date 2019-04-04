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

package io.cdap.cdap.internal.app.runtime.monitor.proxy;

import io.netty.channel.ChannelHandler;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * A {@link ChannelHandler} for handling SOCKS handshake requests for the {@link ServiceSocksProxy}.
 */
final class ServiceSocksServerHandler extends AbstractSocksServerHandler {

  private final DiscoveryServiceClient discoveryServiceClient;
  private final ServiceSocksProxyAuthenticator authenticator;

  ServiceSocksServerHandler(DiscoveryServiceClient discoveryServiceClient,
                            ServiceSocksProxyAuthenticator authenticator) {
    this.discoveryServiceClient = discoveryServiceClient;
    this.authenticator = authenticator;
  }

  @Override
  protected boolean requireAuthentication() {
    return true;
  }

  @Override
  protected boolean authenticate(String username, String password) {
    return authenticator.authenticate(username, password);
  }

  @Override
  protected ChannelHandler createSocks4ConnectHandler() {
    return null;
  }

  @Override
  protected ChannelHandler createSocks5ConnectHandler() {
    return new ServiceSocksServerConnectHandler(discoveryServiceClient);
  }
}
