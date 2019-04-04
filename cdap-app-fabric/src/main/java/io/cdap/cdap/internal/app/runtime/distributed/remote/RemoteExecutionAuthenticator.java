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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Networks;

import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A {@link Authenticator} for providing authentication information for the service socks proxy.
 */
public class RemoteExecutionAuthenticator extends Authenticator {

  private final CConfiguration cConf;
  private volatile String password;
  private volatile InetSocketAddress proxyAddress;

  @Inject
  RemoteExecutionAuthenticator(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Nullable
  @Override
  protected PasswordAuthentication getPasswordAuthentication() {
    if (!"SOCKS5".equals(getRequestingProtocol())) {
      return null;
    }
    InetSocketAddress proxyAddress = getProxyAddress();
    if (proxyAddress == null) {
      return null;
    }
    if (!Objects.equals(proxyAddress.getHostName(), getRequestingHost())) {
      return null;
    }
    if (proxyAddress.getPort() != getRequestingPort()) {
      return null;
    }
    String password = getPassword();
    if (password == null) {
      return null;
    }
    return new PasswordAuthentication(password, password.toCharArray());
  }

  @Nullable
  private InetSocketAddress getProxyAddress() {
    if (proxyAddress == null) {
      proxyAddress = Networks.getAddress(cConf, Constants.RuntimeMonitor.SERVICE_PROXY_ADDRESS);
    }
    return proxyAddress;
  }

  @Nullable
  private String getPassword() {
    if (password == null) {
      password = cConf.get(Constants.RuntimeMonitor.SERVICE_PROXY_PASSWORD);
    }
    return password;
  }
}
