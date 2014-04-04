package com.continuuity.gateway.run;

import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.KafkaClientModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.MetricsService;
import com.continuuity.gateway.auth.GatewayAuthModule;
import com.continuuity.gateway.runtime.GatewayModule;
import com.continuuity.gateway.runtime.MetricsModule;
import com.continuuity.internal.app.store.MDTBasedStoreFactory;
import com.continuuity.logging.gateway.handlers.LogHandlerModule;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.metrics.guice.MetricsHandlerModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Services;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class MetricsTwillRunnable extends AbstractTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsTwillRunnable.class);

  private String name;
  private String cConfName;
  private String hConfName;
  private CountDownLatch runLatch;

  private MetricsService metrics;

  public MetricsTwillRunnable(String name, String cConfName, String hConfName) {
    this.name = name;
    this.cConfName = cConfName;
    this.hConfName = hConfName;
  }

  @Override
  public TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(name)
      .withConfigs(ImmutableMap.of("cConf", cConfName, "hConf", hConfName))
      .build();
  }

  @Override
  public void initialize(TwillContext context) {
    super.initialize(context);

    runLatch = new CountDownLatch(1);
    name = context.getSpecification().getName();
    Map<String, String> configs = context.getSpecification().getConfigs();

    LOG.info("Initializing runnable " + name);
    try {
      // Load configuration
      Configuration hConf = new Configuration();
      hConf.clear();
      hConf.addResource(new File(configs.get("hConf")).toURI().toURL());

      UserGroupInformation.setConfiguration(hConf);

      CConfiguration cConf = CConfiguration.create();
      cConf.clear();
      cConf.addResource(new File(configs.get("cConf")).toURI().toURL());

      LOG.info("Setting host name to " + context.getHost().getCanonicalHostName());
      cConf.set(Constants.Metrics.ADDRESS, context.getHost().getCanonicalHostName());

      // Set Gateway port to 0, so that it binds to any free port.
      cConf.setInt(Constants.Metrics.PORT, 0);

      LOG.info("Continuuity conf {}", cConf);
      LOG.info("HBase conf {}", hConf);

      Injector injector = createGuiceInjector(cConf, hConf);

      // Get the Metric Services
      metrics = injector.getInstance(MetricsService.class);

      LOG.info("Runnable initialized " + name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void run() {
    LOG.info("Starting runnable " + name);
    Futures.getUnchecked(Services.chainStart(metrics));
    LOG.info("Runnable started " + name);

    try {
      runLatch.await();
    } catch (InterruptedException e) {
      LOG.error("Waiting on latch interrupted");
      Thread.currentThread().interrupt();
    }

    LOG.info("Runnable stopped " + name);
  }

  @Override
  public void stop() {
    LOG.info("Stopping runnable " + name);
    Futures.getUnchecked(Services.chainStop(metrics));
    runLatch.countDown();
  }

  static Injector createGuiceInjector(CConfiguration cConf, Configuration hConf) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new DataFabricModules(cConf, hConf).getDistributedModules(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new LoggingModules().getDistributedModules(),
      new GatewayAuthModule(),
      new MetricsHandlerModule(),
      new MetricsModule().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // It's a bit hacky to add it here. Need to refactor these bindings out as it overlaps with
          // AppFabricServiceModule
          bind(StoreFactory.class).to(MDTBasedStoreFactory.class);
      }
    }
    );
  }
}
