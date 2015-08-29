/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.metadata.service;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.metrics.MetricsReporterHook;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Service to manage metadata in CDAP
 */
public class MetadataService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataService.class);

  private final CConfiguration cConf;
  private final MetricsCollectionService metricsCollectionService;
  private final DiscoveryService discoveryService;

  private NettyHttpService httpService;

  @Inject
  MetadataService(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                  DiscoveryService discoveryService) {
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
    this.discoveryService = discoveryService;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting Metadata Service");
    httpService = new CommonNettyHttpServiceBuilder(cConf)
      .addHttpHandlers(ImmutableList.of(new MetadataHttpHandler()))
      .setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                Constants.Service.DATASET_MANAGER)))
      .setHost(cConf.get("metadata.service.bind.address"))
      .build();
    httpService.addListener(new ServiceListenerAdapter() {
      private Cancellable cancellable;

      @Override
      public void running() {
        final InetSocketAddress socketAddress = httpService.getBindAddress();
        LOG.debug("Metadata service running at {}", socketAddress);
        cancellable = discoveryService.register(ResolvingDiscoverable.of(new Discoverable() {
          @Override
          public String getName() {
            return Constants.Service.METADATA_SERVICE;
          }

          @Override
          public InetSocketAddress getSocketAddress() {
            return socketAddress;
          }
        }));
      }

      @Override
      public void terminated(State from) {
        LOG.debug("Metadata HTTP service stopped");
        cancellable.cancel();
      }

      @Override
      public void failed(State from, Throwable failure) {
        LOG.debug("Metadata HTTP service stopped with failure.", failure);
        cancellable.cancel();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    httpService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down Metadata Service");
    httpService.stopAndWait();
  }
}
