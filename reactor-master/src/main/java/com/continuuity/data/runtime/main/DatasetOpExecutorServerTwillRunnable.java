package com.continuuity.data.runtime.main;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.KafkaClientModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.ServiceLoggingContext;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.twill.AbstractReactorTwillRunnable;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetServiceModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillContext;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.List;

/**
 * Executes user code on behalf of a particular user inside
 * a container running as that user. For security.
 */
public class DatasetOpExecutorServerTwillRunnable extends AbstractReactorTwillRunnable {

  // TODO: remove
  public static final String DEFAULT_USER = "bob";

  private Injector injector;
  private String user;

  public DatasetOpExecutorServerTwillRunnable(String name, String cConfName, String hConfName) {
    super(name, cConfName, hConfName);
  }

  @Override
  protected void doInit(TwillContext context) {
    this.user = DEFAULT_USER;

    CConfiguration cConf = getCConfiguration();
    Configuration hConf = getConfiguration();

    // Set the host name to the one provided by Twill
    cConf.set(Constants.Dataset.Executor.ADDRESS, context.getHost().getHostName());

    injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(), new ZKClientModule(),
      new KafkaClientModule(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new LocationRuntimeModule().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModule(),
      new DataSetServiceModules().getDistributedModule(),
      new LoggingModules().getDistributedModules(),
      new AuthModule());

    injector.getInstance(LogAppenderInitializer.class).initialize();
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.DATASET_EXECUTOR));
  }

  @Override
  protected void getServices(List<? super Service> services) {
    services.add(injector.getInstance(ZKClientService.class));
    services.add(injector.getInstance(KafkaClientService.class));
    services.add(injector.getInstance(MetricsCollectionService.class));
    services.add(injector.getInstance(DatasetOpExecutorService.class));
  }
}
