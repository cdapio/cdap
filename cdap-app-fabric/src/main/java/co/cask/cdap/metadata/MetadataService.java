/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.metrics.MetricsReporterHook;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Service to manage metadata in CDAP. This service serves the HTTP endpoints defined in {@link MetadataHttpHandler}.
 */
public class MetadataService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataService.class);

  private final CConfiguration cConf;
  private final MetricsCollectionService metricsCollectionService;
  private final DiscoveryService discoveryService;
  private final Set<HttpHandler> handlers;
  private final Integer instanceId;
  private final MetadataMigrator metadataMigrator;

  private NettyHttpService httpService;
  private Cancellable cancelDiscovery;

  @Inject
  MetadataService(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                  DiscoveryService discoveryService,
                  @Named(Constants.Metadata.HANDLERS_NAME) Set<HttpHandler> handlers,
                  DatasetFramework dsFramework, TransactionSystemClient txClient,
                  @Named(Constants.DatasetOpsExecutor.INSTANCE_ID) Integer instanceId) {
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
    this.discoveryService = discoveryService;
    this.handlers = handlers;
    this.instanceId = instanceId;
    this.metadataMigrator = new MetadataMigrator(cConf, dsFramework, txClient);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Metadata Service");
    httpService = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.METADATA_SERVICE)
      .setHttpHandlers(handlers)
      .setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                Constants.Service.METADATA_SERVICE)))
      .setHost(cConf.get(Constants.Metadata.SERVICE_BIND_ADDRESS))
      .setPort(cConf.getInt(Constants.Metadata.SERVICE_BIND_PORT))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.Metadata.SERVICE_WORKER_THREADS))
      .setExecThreadPoolSize(cConf.getInt(Constants.Metadata.SERVICE_EXEC_THREADS))
      .setConnectionBacklog(20000)
      .build();

    httpService.start();

    // Start migration only on the first instance of Dataset Ops Executor.
    if (instanceId == 0) {
      metadataMigrator.start();
    }

    InetSocketAddress socketAddress = httpService.getBindAddress();
    LOG.info("Metadata service running at {}", socketAddress);
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(new Discoverable(Constants.Service.METADATA_SERVICE, socketAddress)));

  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down Metadata Service");
    cancelDiscovery.cancel();
    httpService.stop();
    if (metadataMigrator.isRunning()) {
      metadataMigrator.stop();
    }
    LOG.info("Metadata HTTP service stopped");
  }
}
