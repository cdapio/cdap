/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers.meta;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.MetricsReporterHook;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.authorization.PrivilegesFetcherProxyService;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
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
 * Provides REST endpoints to execute System Operations such as writing to Store, Lineage etc remotely.
 */
public class RemoteSystemOperationsService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteSystemOperationsService.class);

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private final PrivilegesFetcherProxyService privilegesFetcherProxyService;

  private Cancellable cancellable;

  @Inject
  RemoteSystemOperationsService(CConfiguration cConf, DiscoveryService discoveryService,
                                MetricsCollectionService metricsCollectionService,
                                @Named(Constants.RemoteSystemOpService.HANDLERS_NAME) Set<HttpHandler> handlers,
                                PrivilegesFetcherProxyService privilegesFetcherProxyService) {
    this.discoveryService = discoveryService;
    this.privilegesFetcherProxyService = privilegesFetcherProxyService;

    int workerThreads = cConf.getInt(Constants.RemoteSystemOpService.WORKER_THREADS);
    int execThreads = cConf.getInt(Constants.RemoteSystemOpService.EXEC_THREADS);

    this.httpService = new CommonNettyHttpServiceBuilder(cConf)
      .addHttpHandlers(handlers)
      .setHost(cConf.get(Constants.RemoteSystemOpService.SERVICE_BIND_ADDRESS))
      .setHandlerHooks(ImmutableList.of(
        new MetricsReporterHook(metricsCollectionService, Constants.Service.REMOTE_SYSTEM_OPERATION)))
      .setWorkerThreadPoolSize(workerThreads)
      .setExecThreadPoolSize(execThreads)
      .setConnectionBacklog(20000)
      .build();
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.REMOTE_SYSTEM_OPERATION));
    LOG.info("Starting RemoteSystemOperationService...");

    httpService.startAndWait();
    cancellable = discoveryService.register(ResolvingDiscoverable.of(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.REMOTE_SYSTEM_OPERATION;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    }));

    privilegesFetcherProxyService.startAndWait();

    LOG.info("RemoteSystemOperationService started successfully on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping RemoteSystemOperationService...");

    privilegesFetcherProxyService.stopAndWait();

    try {
      if (cancellable != null) {
        cancellable.cancel();
      }
    } finally {
      httpService.stopAndWait();
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("bindAddress", httpService.getBindAddress())
      .toString();
  }

  public NettyHttpService getHttpService() {
    return httpService;
  }
}
