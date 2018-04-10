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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.common.HttpExceptionHandler;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.internal.app.runtime.handler.RuntimeHandler;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitor;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.http.NettyHttpService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * Runtime Server which starts netty-http service to expose metadata to {@link RuntimeMonitor}
 */
public class RuntimeServer extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeServer.class);

  private final InetAddress hostname;
  private final CConfiguration cConf;
  private final MessageFetcher messageFetcher;
  private NettyHttpService httpService;

  public RuntimeServer(CConfiguration cConf, InetAddress hostname, MessageFetcher messageFetcher) {
    this.hostname = hostname;
    this.messageFetcher = messageFetcher;
    this.cConf = cConf;
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.RUNTIME_HTTP));
    httpService = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.RUNTIME_HTTP)
      .setHost(hostname.getCanonicalHostName())
      .setHttpHandlers(new RuntimeHandler(cConf, messageFetcher, this::stop))
      .setPort(cConf.getInt(Constants.RuntimeHandler.SERVER_PORT))
      .setExceptionHandler(new HttpExceptionHandler()).build();

    httpService.start();
    LOG.info("Runtime HTTP server started on {}", httpService.getBindAddress());
  }

  @VisibleForTesting
  public NettyHttpService getHttpService() {
    return httpService;
  }

  @Override
  protected void shutDown() throws Exception {
    httpService.stop();
    LOG.info("Runtime HTTP server stopped");
  }
}
