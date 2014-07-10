package com.continuuity.gateway.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.KafkaClientModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.gateway.collector.NettyFlumeCollector;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
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

