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

package co.cask.cdap.search;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.proto.Id;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Elastic search service
 */
public class SearchService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(SearchService.class);

  private CommonNettyHttpServiceBuilder nettyHttpServiceBuilder;
  private NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private Cancellable cancelDiscovery;
  private Node node;

  @Inject
  public SearchService(CConfiguration cConf, DiscoveryService discoveryService) {
    // netty http server config
    
    String address = cConf.get(Constants.Search.ADDRESS);
    int backlogcnxs = cConf.getInt(Constants.Metrics.BACKLOG_CONNECTIONS, 20000);
    int execthreads = cConf.getInt(Constants.Metrics.EXEC_THREADS, 20);
    int bossthreads = cConf.getInt(Constants.Metrics.BOSS_THREADS, 1);
    int workerthreads = cConf.getInt(Constants.Metrics.WORKER_THREADS, 10);

    nettyHttpServiceBuilder = new CommonNettyHttpServiceBuilder(cConf);
    nettyHttpServiceBuilder.setHost(address);

    nettyHttpServiceBuilder.setConnectionBacklog(backlogcnxs);
    nettyHttpServiceBuilder.setExecThreadPoolSize(execthreads);
    nettyHttpServiceBuilder.setBossThreadPoolSize(bossthreads);
    nettyHttpServiceBuilder.setWorkerThreadPoolSize(workerthreads);

    this.discoveryService = discoveryService;

    LOG.info("Configuring SearchService " +
               ", address: " + address +
               ", backlog connections: " + backlogcnxs +
               ", execthreads: " + execthreads +
               ", bossthreads: " + bossthreads +
               ", workerthreads: " + workerthreads);
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Id.Namespace.SYSTEM.getId(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.SEARCH));

    node = NodeBuilder.nodeBuilder().build();
    node.start();
    Client client = node.client();

    nettyHttpServiceBuilder.addHttpHandlers(ImmutableList.of(new SearchHttpHandler(client)));
    httpService = nettyHttpServiceBuilder.build();
    httpService.startAndWait();

    // Register the service
    cancelDiscovery = discoveryService.register(ResolvingDiscoverable.of(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.SEARCH;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    }));

    LOG.info("Search Service client started successfully on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Search Service...");

    // Unregister the service
    cancelDiscovery.cancel();
    httpService.stopAndWait();
    node.stop();
  }
}
