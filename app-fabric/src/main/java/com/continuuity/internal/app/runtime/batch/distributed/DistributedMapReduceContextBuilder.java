package com.continuuity.internal.app.runtime.batch.distributed;

import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.DistributedDataSetAccessor;
import com.continuuity.data2.datafabric.dataset.RemoteDatasetFramework;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeClassLoaderFactory;
import com.continuuity.data2.datafabric.dataset.type.DistributedDatasetTypeClassLoaderFactory;
import com.continuuity.data2.dataset2.DatasetDefinitionRegistryFactory;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DefaultDatasetDefinitionRegistry;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.internal.app.runtime.ProgramServiceDiscovery;
import com.continuuity.internal.app.runtime.batch.AbstractMapReduceContextBuilder;
import com.continuuity.internal.app.runtime.distributed.DistributedProgramServiceDiscovery;
import com.continuuity.logging.appender.LogAppender;
import com.continuuity.logging.appender.kafka.KafkaLogAppender;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.twill.zookeeper.ZKClientService;

/**
 * Builds an instance of {@link com.continuuity.internal.app.runtime.batch.BasicMapReduceContext} good for
 * distributed environment. The context is to be used in remote worker (e.g. Mapper task started in YARN container)
 */
public class DistributedMapReduceContextBuilder extends AbstractMapReduceContextBuilder {
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final TaskAttemptContext taskContext;
  private ZKClientService zkClientService;

  public DistributedMapReduceContextBuilder(CConfiguration cConf, Configuration hConf, TaskAttemptContext taskContext) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.taskContext = taskContext;
  }

  @Override
  protected Injector prepare() {
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getDistributedModules(),
      new IOModule(),
      new AuthModule(),
      new ZKClientModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new MetricsClientRuntimeModule().getMapReduceModules(taskContext),
      new AbstractModule() {
        @Override
        protected void configure() {

          // Data-fabric bindings
          bind(HBaseTableUtil.class).toProvider(HBaseTableUtilFactory.class);

          // txds2
          bind(DataSetAccessor.class).to(DistributedDataSetAccessor.class).in(Singleton.class);

          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));
          bind(DatasetTypeClassLoaderFactory.class).to(DistributedDatasetTypeClassLoaderFactory.class);
          bind(DatasetFramework.class).to(RemoteDatasetFramework.class);

          // For log publishing
          bind(LogAppender.class).to(KafkaLogAppender.class);

          //install discovery service modules
          bind(ProgramServiceDiscovery.class).to(DistributedProgramServiceDiscovery.class).in(Scopes.SINGLETON);
        }
      }
    );

    zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.start();

    return injector;
  }

  @Override
  protected void finish() {
    zkClientService.stop();
  }
}
