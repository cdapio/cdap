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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Namespace server. Could be renamed and extended to include other services in future
 */
public class NamespaceHttpService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(NamespaceHttpService.class);

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancellable;

  @Inject
  public NamespaceHttpService(CConfiguration cConf, DiscoveryService discoveryService, @Named("namespaces")
                              Set<HttpHandler> handlers) {
    this.discoveryService = discoveryService;
    this.httpService = new CommonNettyHttpServiceBuilder(cConf)
      .addHttpHandlers(handlers)
      //.setHost(Constants.Namespace.ADDRESS)
      //.setWorkerThreadPoolSize(cConf.getInt(Constants.Namespace.WORKER_THREADS, 10))
      //.setExecThreadPoolSize(0)
      //.setConnectionBacklog(2000)
      //.setChannelConfig("child.bufferFactory",
      //                 HeapChannelBufferFactory.getInstance()) // ChannelBufferFactory that always creates new Buffer
      .build();
  }

  @Override
  protected void startUp() throws Exception {
    System.out.println("################################### Starting HTTP Server for Namespaces");
    LOG.warn("Starting HTTP Server for Namespaces");
    //TODO: Constantify
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       "namespaces"));
    httpService.startAndWait();

    cancellable = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        // TODO: Constantify
        return "namespaces";
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    });

    System.out.println("################################### Started HTTP Server for Namespaces???????");
  }

  @Override
  protected void shutDown() throws Exception {
    System.out.println("#################################### Shutting HTTP Server for Namespaces");
    LOG.warn("Shutting down HTTP Server for Namespaces");
    try {
      if (cancellable != null) {
        cancellable.cancel();
      }
    } finally {
      httpService.stopAndWait();
    }
  }
}
