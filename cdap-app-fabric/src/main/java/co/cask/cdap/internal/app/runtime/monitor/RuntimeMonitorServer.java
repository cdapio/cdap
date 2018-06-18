/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.monitor;

import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.common.HttpExceptionHandler;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.tools.HttpsEnabler;
import co.cask.http.NettyHttpService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.concurrent.CountDownLatch;

/**
 * Runtime Server which starts netty-http service to expose metadata to {@link RuntimeMonitor}
 */
public class RuntimeMonitorServer extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitorServer.class);

  private final CConfiguration cConf;
  private final MessageFetcher messageFetcher;
  private final CountDownLatch shutdownLatch;
  private final KeyStore keyStore;
  private final KeyStore trustStore;
  private NettyHttpService httpService;

  @Inject
  RuntimeMonitorServer(CConfiguration cConf, MessagingService messagingService,
                       @Constants.AppFabric.KeyStore KeyStore keyStore,
                       @Constants.AppFabric.TrustStore KeyStore trustStore) {
    this.cConf = cConf;
    this.messageFetcher = new MultiThreadMessagingContext(messagingService).getMessageFetcher();
    this.shutdownLatch = new CountDownLatch(1);
    this.keyStore = keyStore;
    this.trustStore = trustStore;
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.RUNTIME_HTTP));
    InetSocketAddress address = getServerSocketAddress(cConf);

    // Enable SSL for communication.
    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.RUNTIME_HTTP)
      .setHttpHandlers(new RuntimeHandler(cConf, messageFetcher, () -> {
        shutdownLatch.countDown();
        stop();
      }))
      .setExceptionHandler(new HttpExceptionHandler())
      .setHost(address.getHostName())
      .setPort(address.getPort());

    httpService = new HttpsEnabler()
      .setKeyStore(keyStore, ""::toCharArray)
      .setTrustStore(trustStore)
      .enable(builder)
      .build();

    httpService.start();
    LOG.info("Runtime monitor server started on {}", httpService.getBindAddress());
  }

  @VisibleForTesting
  public InetSocketAddress getBindAddress() {
    return httpService.getBindAddress();
  }

  @Override
  protected void shutDown() throws Exception {
    // Wait for the shutdown signal from the runtime monitor before shutting off the http server.
    // This allows the runtime monitor still able to talk to this service until all data are fetched.
    Uninterruptibles.awaitUninterruptibly(shutdownLatch);
    httpService.stop();
    LOG.info("Runtime monitor server stopped");
  }

  /**
   * Returns the {@link InetSocketAddress} for the http service to bind to.
   */
  private InetSocketAddress getServerSocketAddress(CConfiguration cConf) {
    String host = cConf.get(Constants.RuntimeMonitor.SERVER_HOST);
    if (host == null) {
      host = InetAddress.getLoopbackAddress().getCanonicalHostName();
    }
    int port = cConf.getInt(Constants.RuntimeMonitor.SERVER_PORT);
    return new InetSocketAddress(host, port);
  }
}
