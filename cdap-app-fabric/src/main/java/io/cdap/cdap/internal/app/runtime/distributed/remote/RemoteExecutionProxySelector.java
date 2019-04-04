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

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.utils.Networks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

/**
 * A {@link ProxySelector} to return proxy setting for {@link URI}.
 */
public class RemoteExecutionProxySelector extends ProxySelector {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionProxySelector.class);
  private static final Logger OUTAGE_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(TimeUnit.SECONDS.toMillis(30)));

  private final CConfiguration cConf;
  private volatile Proxy serviceProxy;

  @Inject
  RemoteExecutionProxySelector(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public List<Proxy> select(URI uri) {
    Proxy proxy = Proxy.NO_PROXY;

    // The port will be 0 for services that need to use service proxy
    if (uri.getPort() == 0) {
      if (serviceProxy == null) {
        serviceProxy = readServiceProxy(cConf);
      }
      proxy = serviceProxy;
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Proxy for {} is {}", uri, proxy);
    }

    return Collections.singletonList(proxy);
  }

  @Override
  public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
    OUTAGE_LOG.warn("Failed to connect to {} via proxy {}", uri, sa, ioe);

    // If connect failed, set the proxy to null so that it will be re-read next time
    this.serviceProxy = null;
  }

  private Proxy readServiceProxy(CConfiguration cConf) {
    InetSocketAddress addr = Networks.getAddress(cConf, Constants.RuntimeMonitor.SERVICE_PROXY_ADDRESS);
    Proxy proxy = addr == null ? Proxy.NO_PROXY : new Proxy(Proxy.Type.SOCKS, addr);

    LOG.debug("Service proxy is {}", proxy);
    return proxy;
  }
}
