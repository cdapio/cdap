package com.continuuity.hive.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.twill.AbstractReactorTwillRunnable;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.hive.HiveServer;
import com.continuuity.hive.guice.HiveRuntimeModule;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.twill.api.TwillContext;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * TwillRunnable to run Hive through twill.
 */
public class HiveServiceTwillRunnable extends AbstractReactorTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(HiveServiceTwillRunnable.class);

  private ZKClientService zkClient;
  private HiveServer hiveServer;

  private String hiveConfName;
  private HiveConf hiveConf;

  public HiveServiceTwillRunnable(String name, String cConfName, String hConfName, String hiveConfName) {
    super(name, cConfName, hConfName);
    this.hiveConfName = hiveConfName;
  }

  @Override
  protected Map<String, String> addExtraConfs() {
    return ImmutableMap.of(hiveConfName, hiveConfName);
  }

  @Override
  public void doInit(TwillContext context) {
    LOG.info("Initializing runnable {}", name);
    // Hive needs this parameter to be an absolute path
    System.setProperty("java.io.tmpdir", new File(System.getProperty("java.io.tmpdir")).getAbsolutePath());
    try {
      // Set the hostname of the machine so that cConf can be used to start internal services
      LOG.info("{} Setting host name to {}", name, context.getHost().getCanonicalHostName());
      getCConfiguration().set(Constants.Hive.Container.SERVER_ADDRESS, context.getHost().getCanonicalHostName());

      Map<String, String> configs = context.getSpecification().getConfigs();
      hiveConf = new HiveConf();
      hiveConf.clear();
      hiveConf.addResource(new File(configs.get("hive-site.xml")).toURI().toURL());

      int hiveServerPort = PortDetector.findFreePort();
      LOG.info("Setting hive server port to {}...", hiveServerPort);

      // The port number is a parameter that is directly read from the hiveConf passed to hive server,
      // Contrary to most parameters which need to be in hive-site.xml in the classpath.
      hiveConf.setInt("hive.server2.thrift.port", hiveServerPort);

      Injector injector = createGuiceInjector(getCConfiguration(), getConfiguration(), hiveConf);

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

  static Injector createGuiceInjector(CConfiguration cConf, Configuration hConf, final HiveConf hiveConf) {
    return Guice.createInjector(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(HiveConf.class).toInstance(hiveConf);
          }
        },
        new ConfigModule(cConf, hConf),
        new IOModule(),
        new ZKClientModule(),
        new HiveRuntimeModule().getDistributedModules(),
        new LocationRuntimeModule().getDistributedModules(),
        new DiscoveryRuntimeModule().getDistributedModules()
    );
  }
}
