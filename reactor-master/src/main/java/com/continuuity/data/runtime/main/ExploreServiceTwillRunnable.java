package com.continuuity.data.runtime.main;

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
import com.continuuity.explore.service.ExploreService;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.hive.guice.HiveRuntimeModule;
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
 *
 */
public class ExploreServiceTwillRunnable extends AbstractReactorTwillRunnable {

  private Injector injector;

  public ExploreServiceTwillRunnable(String name, String cConfName, String hConfName) {
    super(name, cConfName, hConfName);
  }

  @Override
  protected void doInit(TwillContext context) {
    CConfiguration cConf = getCConfiguration();
    Configuration hConf = getConfiguration();

    // Set the host name to the one provided by Twill
    // TODO move the constant
    cConf.set(Constants.Hive.SERVER_ADDRESS, context.getHost().getHostName());

    injector = Guice.createInjector(
        new ConfigModule(cConf, hConf),
        new IOModule(), new ZKClientModule(),
        new KafkaClientModule(),
        new MetricsClientRuntimeModule().getDistributedModules(),
        new DiscoveryRuntimeModule().getDistributedModules(),
        new LocationRuntimeModule().getDistributedModules(),
        new DataFabricModules().getDistributedModules(),
        new HiveRuntimeModule().getDistributedModules(),
        new AuthModule());
  }

  @Override
  protected void getServices(List<? super Service> services) {
    services.add(injector.getInstance(ZKClientService.class));
    services.add(injector.getInstance(KafkaClientService.class));
    services.add(injector.getInstance(ExploreService.class));
  }
}
