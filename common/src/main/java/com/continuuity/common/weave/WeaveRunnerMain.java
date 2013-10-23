package com.continuuity.common.weave;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.common.zookeeper.election.ElectionHandler;
import com.continuuity.common.zookeeper.election.LeaderElection;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.common.ServiceListenerAdapter;
import com.continuuity.weave.filesystem.LocationFactories;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Generic wrapper class to run weave applications.
 */
public abstract class WeaveRunnerMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(WeaveRunnerMain.class);
  private static final long MAX_BACKOFF_TIME_MS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
  private static final long SUCCESSFUL_RUN_DURATON_MS = TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES);

  protected final CConfiguration cConf;
  protected final Configuration hConf;

  private final AtomicBoolean isLeader = new AtomicBoolean(false);

  private ZKClientService zkClientService;
  private LeaderElection leaderElection;
  private volatile Injector injector;
  private volatile WeaveRunnerService weaveRunnerService;
  private volatile WeaveController weaveController;

  private String yarnUser;

  private String serviceName;
  private WeaveApplication weaveApplication;

  private long lastRunTimeMs = System.currentTimeMillis();
  private int currentRun = 0;

  private boolean stopFlag = false;

  protected WeaveRunnerMain(CConfiguration cConf, Configuration hConf) {
    this.cConf = cConf;
    this.hConf = hConf;
  }

  protected abstract WeaveApplication createWeaveApplication();

  protected WeavePreparer prepare(WeavePreparer preparer) {
    return preparer;
  }

  @Override
  public void init(String[] args) {
    weaveApplication = createWeaveApplication();
    if (weaveApplication == null) {
      throw new IllegalArgumentException("WeaveApplication cannot be null");
    }

    serviceName = weaveApplication.configure().getName();

    // Initialize ZK client
    String zookeeper = cConf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE);
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

    injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(WeaveRunnerService.class).to(YarnWeaveRunnerService.class);
        }

        @Singleton
        @Provides
        private YarnWeaveRunnerService provideYarnWeaveRunnerService(CConfiguration configuration,
                                                                     YarnConfiguration yarnConfiguration,
                                                                     LocationFactory locationFactory) {
          String zkNamespace = configuration.get(Constants.CFG_WEAVE_ZK_NAMESPACE, "/weave");

          YarnConfiguration yarnConfig = new YarnConfiguration(yarnConfiguration);
          yarnConfig.set(Constants.CFG_WEAVE_RESERVED_MEMORY_MB,
                         configuration.get(Constants.CFG_WEAVE_RESERVED_MEMORY_MB));

          YarnWeaveRunnerService runner =
            new YarnWeaveRunnerService(yarnConfig,
                                       configuration.get(Constants.Zookeeper.QUORUM) + zkNamespace,
                                       LocationFactories.namespace(locationFactory, "weave"));
          runner.setJVMOptions(configuration.get(Constants.CFG_WEAVE_JVM_GC_OPTS));

          return runner;
        }
      }
    );

    yarnUser = cConf.get(Constants.CFG_YARN_USER, System.getProperty("user.name"));
  }

  @Override
  public void start() {
    zkClientService.startAndWait();

    leaderElection = new LeaderElection(zkClientService, "/election/" + serviceName, new ElectionHandler() {
      @Override
      public void leader() {
        LOG.info("Became leader.");
        weaveRunnerService = injector.getInstance(WeaveRunnerService.class);
        weaveRunnerService.startAndWait();
        run();
        isLeader.set(true);
      }

      @Override
      public void follower() {
        LOG.info("Became follower.");
        if (weaveRunnerService != null && weaveRunnerService.isRunning()) {
          weaveRunnerService.stopAndWait();
        }
        isLeader.set(false);
      }
    });
  }

  @Override
  public void stop() {
    LOG.info("Stopping {}", serviceName);
    stopFlag = true;

    if (isLeader.get() && weaveController != null && weaveController.isRunning()) {
      weaveController.stopAndWait();
    }

    leaderElection.cancel();
    zkClientService.stopAndWait();
  }

  @Override
  public void destroy() {
    LOG.info("Destroying {}", serviceName);
    if (weaveRunnerService != null && weaveRunnerService.isRunning()) {
      weaveRunnerService.stopAndWait();
    }
  }

  private void run() {
    // If service is already running, return handle to that instance
    Iterable<WeaveController> weaveControllers = lookupService();
    Iterator<WeaveController> iterator = weaveControllers.iterator();

    if (iterator.hasNext()) {
      LOG.info("{} application is already running", serviceName);
      weaveController = iterator.next();

      if (iterator.hasNext()) {
        LOG.warn("Found more than one instance of {} running. Stopping the others...", serviceName);
        for (; iterator.hasNext(); ) {
          WeaveController controller = iterator.next();
          LOG.warn("Stopping one extra instance of {}", serviceName);
          controller.stopAndWait();
        }
        LOG.warn("Done stopping extra instances of {}", serviceName);
      }
    } else {
      LOG.info("Starting {} application", serviceName);
      WeavePreparer weavePreparer = getPreparer();
      weaveController = weavePreparer.start();

      weaveController.addListener(new ServiceListenerAdapter() {
        @Override
        public void failed(Service.State from, Throwable failure) {
          LOG.error("{} failed with exception... restarting with back-off.", serviceName, failure);
          backOffRun();
        }

        @Override
        public void terminated(Service.State from) {
          LOG.warn("{} got terminated... restarting with back-off", serviceName);
          backOffRun();
        }
      }, MoreExecutors.sameThreadExecutor());
    }
  }

  private void backOffRun() {
    if (stopFlag) {
      LOG.warn("Not starting a new run when stopFlag is true");
      return;
    }

    if (System.currentTimeMillis() - lastRunTimeMs > SUCCESSFUL_RUN_DURATON_MS) {
      currentRun = 0;
    }

    try {

      long sleepMs = Math.min(500 * (long) Math.pow(2, currentRun), MAX_BACKOFF_TIME_MS);
      LOG.info("Current restart run = {}. Backing off for {} ms...", currentRun, sleepMs);
      TimeUnit.MILLISECONDS.sleep(sleepMs);

    } catch (InterruptedException e) {
      LOG.warn("Got interrupted exception: ", e);
      Thread.currentThread().interrupt();
    }

    run();
    ++currentRun;
    lastRunTimeMs = System.currentTimeMillis();
  }

  protected File getSavedHConf() throws IOException {
    File hConfFile = saveHConf(hConf, File.createTempFile("hConf", ".xml"));
    hConfFile.deleteOnExit();
    return hConfFile;
  }

  protected File getSavedCConf() throws IOException {
    File cConfFile = saveCConf(cConf, File.createTempFile("cConf", ".xml"));
    cConfFile.deleteOnExit();
    return cConfFile;
  }

  private WeavePreparer getPreparer() {
    return prepare(weaveRunnerService.prepare(weaveApplication)
                     .setUser(yarnUser)
                     .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
    );
  }

  private static File saveHConf(Configuration conf, File file) throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
    return file;
  }

  private File saveCConf(CConfiguration conf, File file) throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
    return file;
  }

  /**
   * Wait for sometime while looking up service in weave.
   */
  private Iterable<WeaveController> lookupService() {
    int count = 100;
    Iterable<WeaveController> iterable = weaveRunnerService.lookup(serviceName);

    try {

      for (int i = 0; i < count; ++i) {
        if (iterable.iterator().hasNext()) {
          return iterable;
        }

        TimeUnit.MILLISECONDS.sleep(20);
      }
    } catch (InterruptedException e) {
      LOG.warn("Got interrupted exception: ", e);
      Thread.currentThread().interrupt();
    }

    return iterable;
  }
}
