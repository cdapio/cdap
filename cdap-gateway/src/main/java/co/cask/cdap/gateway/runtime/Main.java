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

package co.cask.cdap.gateway.runtime;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.runtime.DaemonMain;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.gateway.collector.NettyFlumeCollector;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.twill.common.Services;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main is a simple class that allows us to launch the Gateway as a standalone
 * program. This is also where we do our runtime injection.
 * <p/>
 */
public class Main extends DaemonMain {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  private ZKClientService zkClientService;
  private KafkaClientService kafkaClientService;
  private MetricsCollectionService metricsCollectionService;
  private NettyFlumeCollector flumeCollector;

  public static void main(String[] args) throws Exception {
    new Main().doMain(args);
  }

  @Override
  public void init(String[] args) {
    // Load our configuration from our resource files
    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = HBaseConfiguration.create(new HdfsConfiguration());

    // Set the HTTP keep alive max connection property to allow more keep-alive connections
    if (System.getProperty("http.maxConnections") == null) {
      System.setProperty("http.maxConnections", cConf.get(Constants.Gateway.STREAM_FLUME_THREADS));
    }

    String zookeeper = cConf.get(Constants.Zookeeper.QUORUM);
    if (zookeeper == null) {
      LOG.error("No zookeeper quorum provided.");
      throw new IllegalStateException("No zookeeper quorum provided.");
    }

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new AuthModule(),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new GatewayModule().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModule(),
      new LoggingModules().getDistributedModules()
    );

    zkClientService = injector.getInstance(ZKClientService.class);
    kafkaClientService = injector.getInstance(KafkaClientService.class);

    // Get the metrics collection service
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);

    // Get our fully wired Flume Collector
    flumeCollector = injector.getInstance(NettyFlumeCollector.class);

  }

  @Override
  public void start() {
    LOG.info("Starting Gateway...");
    Futures.getUnchecked(Services.chainStart(zkClientService, kafkaClientService, metricsCollectionService,
                                             flumeCollector));
  }

  @Override
  public void stop() {
    LOG.info("Stopping Gateway...");
    Futures.getUnchecked(Services.chainStop(flumeCollector, metricsCollectionService, kafkaClientService,
                                            zkClientService));
  }

  @Override
  public void destroy() {
    // no-op
  }
}

