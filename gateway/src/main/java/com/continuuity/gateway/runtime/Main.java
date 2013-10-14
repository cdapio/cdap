package com.continuuity.gateway.runtime;

import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.conf.KafkaConstants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.collector.NettyFlumeCollector;
import com.continuuity.gateway.v2.runtime.GatewayModules;
import com.continuuity.internal.app.store.MDTBasedStoreFactory;
import com.continuuity.internal.kafka.client.ZKKafkaClientService;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.weave.common.Services;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.util.concurrent.Futures;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

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

    String zookeeper = cConf.get(Constants.Zookeeper.QUORUM);
    if (zookeeper == null) {
      LOG.error("No zookeeper quorum provided.");
      throw new IllegalStateException("No zookeeper quorum provided.");
    }

    zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(zookeeper).build(),
            RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
          )
        ));

    String kafkaZKNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    kafkaClientService = new ZKKafkaClientService(
      kafkaZKNamespace == null
        ? zkClientService
        : ZKClients.namespace(zkClientService, "/" + kafkaZKNamespace)
    );

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
      new MetricsClientRuntimeModule(kafkaClientService).getDistributedModules(),
      new GatewayModules().getDistributedModules(),
      new DataFabricModules(cConf, hConf).getDistributedModules(),
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule(zkClientService).getDistributedModules(),
      new LoggingModules().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // It's a bit hacky to add it here. Need to refactor these bindings out as it overlaps with
          // AppFabricServiceModule
          bind(StoreFactory.class).to(MDTBasedStoreFactory.class);
        }
      }
    );

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

