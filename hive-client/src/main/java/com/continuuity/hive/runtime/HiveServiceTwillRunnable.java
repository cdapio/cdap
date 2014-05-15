package com.continuuity.hive.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.twill.AbstractReactorTwillRunnable;
import com.continuuity.hive.HiveServer;
import com.continuuity.hive.guice.DistributedHiveModule;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillContext;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HiveServiceTwillRunnable extends AbstractReactorTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(HiveServiceTwillRunnable.class);

  private ZKClientService zkClient;
  private HiveServer hiveServer;

  public HiveServiceTwillRunnable(String name, String cConfName, String hConfName) {
    super(name, cConfName, hConfName);
    // todo save hive-site.xml to the local disk somewhere
  }

  @Override
  public void initialize(TwillContext context) {
    super.initialize(context);
    LOG.info("Initializing runnable {}", name);
    try {
      // Set the hostname of the machine so that cConf can be used to start internal services
      LOG.info("{} Setting host name to {}", name, context.getHost().getCanonicalHostName());
      getCConfiguration().set(Constants.Transaction.Container.ADDRESS, context.getHost().getCanonicalHostName());

      Injector injector = createGuiceInjector(getCConfiguration(), getConfiguration());

      // Get Zookeeper Client Instance
      zkClient = injector.getInstance(ZKClientService.class);

      // Get Hive Server Instance
      hiveServer = injector.getInstance(HiveServer.class);

      LOG.info("Runnable initialized {}", name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void getServices(List<? super Service> services) {
    services.add(zkClient);
    services.add(hiveServer);
  }

  static Injector createGuiceInjector(CConfiguration cConf, Configuration hConf) {
    return Guice.createInjector(
        new ConfigModule(cConf, hConf),
        new IOModule(),
        new ZKClientModule(),
        new DistributedHiveModule(),
        new LocationRuntimeModule().getDistributedModules(),
        new DiscoveryRuntimeModule().getDistributedModules()
    );
  }
}
