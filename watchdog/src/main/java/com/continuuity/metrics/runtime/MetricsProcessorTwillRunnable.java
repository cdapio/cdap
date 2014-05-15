package com.continuuity.metrics.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.KafkaClientModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.twill.AbstractReactorTwillRunnable;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.internal.migrate.MetricsTableMigrator20to21;
import com.continuuity.internal.migrate.TableMigrator;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.data.DefaultMetricsTableFactory;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.guice.MetricsProcessorModule;
import com.continuuity.metrics.process.KafkaConsumerMetaTable;
import com.continuuity.metrics.process.KafkaMetricsProcessorServiceFactory;
import com.continuuity.metrics.process.MessageCallbackFactory;
import com.continuuity.metrics.process.MetricsMessageCallbackFactory;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillContext;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Twill Runnable to run MetricsProcessor in YARN.
 */
public final class MetricsProcessorTwillRunnable extends AbstractReactorTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsProcessorTwillRunnable.class);

  private KafkaMetricsProcessorService kafkaMetricsProcessorService;
  private ZKClientService zkClientService;
  private KafkaClientService kafkaClientService;

  public MetricsProcessorTwillRunnable(String name, String cConfName, String hConfName) {
    super(name, cConfName, hConfName);
  }

  @Override
  protected void doInit(TwillContext context) {
    LOG.info("Initializing runnable {}", name);
    try {
      // Set the hostname of the machine so that cConf can be used to start internal services
      LOG.info("{} Setting host name to {}", name, context.getHost().getCanonicalHostName());
      getCConfiguration().set(Constants.Metrics.ADDRESS, context.getHost().getCanonicalHostName());

      Injector injector = createGuiceInjector(getCConfiguration(), getConfiguration());
      zkClientService = injector.getInstance(ZKClientService.class);
      kafkaClientService = injector.getInstance(KafkaClientService.class);
      kafkaMetricsProcessorService = injector.getInstance(KafkaMetricsProcessorService.class);
      LOG.info("Runnable initialized {}", name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void getServices(List<? super Service> services) {
    services.add(zkClientService);
    services.add(kafkaClientService);
    services.add(kafkaMetricsProcessorService);
  }

  public static Injector createGuiceInjector(CConfiguration cConf, Configuration hConf) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new LocationRuntimeModule().getDistributedModules(),
      new DataFabricModules(cConf, hConf).getDistributedModules(),
      new KafkaMetricsProcessorModule()
     );
  }

  static final class KafkaMetricsProcessorModule extends PrivateModule {
   @Override
    protected void configure() {
      install(new MetricsProcessorModule());
      bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class)
        .in(Scopes.SINGLETON);
      bind(MessageCallbackFactory.class).to(MetricsMessageCallbackFactory.class);
      bind(TableMigrator.class).to(MetricsTableMigrator20to21.class);
      install(new FactoryModuleBuilder()
                .build(KafkaMetricsProcessorServiceFactory.class));

      expose(TableMigrator.class);
      expose(KafkaMetricsProcessorServiceFactory.class);
    }
    @Provides
    @Named(MetricsConstants.ConfigKeys.KAFKA_CONSUMER_PERSIST_THRESHOLD)
    public int providesConsumerPersistThreshold(CConfiguration cConf) {
      return cConf.getInt(MetricsConstants.ConfigKeys.KAFKA_CONSUMER_PERSIST_THRESHOLD,
                          MetricsConstants.DEFAULT_KAFKA_CONSUMER_PERSIST_THRESHOLD);
    }

    @Provides
    @Named(MetricsConstants.ConfigKeys.KAFKA_TOPIC_PREFIX)
    public String providesKafkaTopicPrefix(CConfiguration cConf) {
      return cConf.get(MetricsConstants.ConfigKeys.KAFKA_TOPIC_PREFIX,
                       MetricsConstants.DEFAULT_KAFKA_TOPIC_PREFIX);
    }

    @Provides
    @Singleton
    public KafkaConsumerMetaTable providesKafkaConsumerMetaTable(MetricsTableFactory
                                                                   tableFactory) {
      return tableFactory.createKafkaConsumerMeta("default");
    }
  }
}
