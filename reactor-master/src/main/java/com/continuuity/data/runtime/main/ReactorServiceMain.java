package com.continuuity.data.runtime.main;

import com.continuuity.app.guice.AppFabricServiceRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.KafkaClientModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.TwillModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.common.zookeeper.election.ElectionHandler;
import com.continuuity.common.zookeeper.election.LeaderElection;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetServiceModules;
import com.continuuity.data.security.HBaseSecureStoreUpdater;
import com.continuuity.data.security.HBaseTokenUtils;
import com.continuuity.data2.datafabric.dataset.service.DatasetManagerService;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.hive.guice.HiveRuntimeModule;
import com.continuuity.hive.server.HiveServer;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.Credentials;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Services;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.yarn.YarnSecureStore;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Driver class for starting all reactor master services.
 * AppFabricHttpService
 * TwillRunnables: MetricsProcessor, MetricsHttp, LogSaver, TransactionService, StreamHandler.
 */
public class ReactorServiceMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(ReactorServiceMain.class);

  private static final long MAX_BACKOFF_TIME_MS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
  private static final long SUCCESSFUL_RUN_DURATON_MS = TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES);

  public ReactorServiceMain(CConfiguration cConf, Configuration hConf) {
    this.cConf = cConf;
    this.hConf = hConf;
  }
  private boolean stopFlag = false;

  protected final CConfiguration cConf;
  protected final Configuration hConf;

  private final AtomicBoolean isLeader = new AtomicBoolean(false);

  private Injector baseInjector;
  private ZKClientService zkClientService;
  private LeaderElection leaderElection;
  private volatile TwillRunnerService twillRunnerService;
  private volatile TwillController twillController;
  private AppFabricServer appFabricServer;
  private HiveServer hiveServer;
  private KafkaClientService kafkaClientService;
  private MetricsCollectionService metricsCollectionService;
  private DatasetManagerService dsService;

  private String serviceName;
  private TwillApplication twillApplication;
  private long lastRunTimeMs = System.currentTimeMillis();
  private int currentRun = 0;

  public static void main(final String[] args) throws Exception {
    LOG.info("Starting Reactor Service Main...");
    new ReactorServiceMain(CConfiguration.create(), HBaseConfiguration.create()).doMain(args);
  }

  @Override
  public void init(String[] args) {
    twillApplication = createTwillApplication();
    if (twillApplication == null) {
      throw new IllegalArgumentException("TwillApplication cannot be null");
    }

    serviceName = twillApplication.configure().getName();

    cConf.set(Constants.Dataset.Manager.ADDRESS, getLocalHost().getCanonicalHostName());

    baseInjector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new IOModule(),
      new AuthModule(),
      new KafkaClientModule(),
      new TwillModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new DataSetServiceModules().getDistributedModule(),
      new DataFabricModules(cConf, hConf).getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new HiveRuntimeModule().getDistributedModules()
    );
    // Initialize ZK client
    zkClientService = baseInjector.getInstance(ZKClientService.class);
    kafkaClientService = baseInjector.getInstance(KafkaClientService.class);
    metricsCollectionService = baseInjector.getInstance(MetricsCollectionService.class);
    dsService = baseInjector.getInstance(DatasetManagerService.class);
  }

  @Override
  public void start() {
    Services.chainStart(zkClientService, kafkaClientService, metricsCollectionService);

    leaderElection = new LeaderElection(zkClientService, "/election/" + serviceName, new ElectionHandler() {
      @Override
      public void leader() {
        LOG.info("Became leader.");
        Injector injector = baseInjector.createChildInjector();

        hiveServer = injector.getInstance(HiveServer.class);
        hiveServer.startAndWait();

        twillRunnerService = injector.getInstance(TwillRunnerService.class);
        twillRunnerService.startAndWait();
        // app fabric uses twillRunnerService for reporting some AM container metrics and getting live-info for apps,
        // make sure its started after twill runner is started.
        appFabricServer = injector.getInstance(AppFabricServer.class);
        appFabricServer.startAndWait();
        scheduleSecureStoreUpdate(twillRunnerService);
        runTwillApps();

        isLeader.set(true);
      }

      @Override
      public void follower() {
        LOG.info("Became follower.");

        dsService.stopAndWait();
        if (twillRunnerService != null) {
          twillRunnerService.stopAndWait();
        }
        if (appFabricServer != null) {
          appFabricServer.stopAndWait();
        }
        if (hiveServer != null) {
          hiveServer.stopAndWait();
        }
        isLeader.set(false);
      }
    });
    leaderElection.start();
  }

  @Override
  public void stop() {
    LOG.info("Stopping {}", serviceName);
    stopFlag = true;

    dsService.stopAndWait();
    if (isLeader.get() && twillController != null) {
      twillController.stopAndWait();
    }

    leaderElection.stopAndWait();
    Services.chainStop(metricsCollectionService, kafkaClientService, zkClientService);
  }

  @Override
  public void destroy() {
  }

  private InetAddress getLocalHost() {
    try {
      return InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      LOG.error("Error obtaining localhost address", e);
      throw Throwables.propagate(e);
    }
  }

  private TwillApplication createTwillApplication() {
    try {
      return new ReactorTwillApplication(cConf, getSavedCConf(), getSavedHConf());
    } catch (Exception e) {
      throw  Throwables.propagate(e);
    }
  }

  private void scheduleSecureStoreUpdate(TwillRunner twillRunner) {
    if (User.isHBaseSecurityEnabled(hConf)) {
      HBaseSecureStoreUpdater updater = new HBaseSecureStoreUpdater(hConf);
      twillRunner.scheduleSecureStoreUpdate(updater, 30000L, updater.getUpdateInterval(), TimeUnit.MILLISECONDS);
    }
  }


  private TwillPreparer prepare(TwillPreparer preparer) {
    return preparer.withDependencies(new HBaseTableUtilFactory().get().getClass())
      .addSecureStore(YarnSecureStore.create(HBaseTokenUtils.obtainToken(hConf, new Credentials())));
  }

  private void runTwillApps() {
    // If service is already running, return handle to that instance
    Iterable<TwillController> twillControllers = lookupService();
    Iterator<TwillController> iterator = twillControllers.iterator();

    if (iterator.hasNext()) {
      LOG.info("{} application is already running", serviceName);
      twillController = iterator.next();

      if (iterator.hasNext()) {
        LOG.warn("Found more than one instance of {} running. Stopping the others...", serviceName);
        for (; iterator.hasNext(); ) {
          TwillController controller = iterator.next();
          LOG.warn("Stopping one extra instance of {}", serviceName);
          controller.stopAndWait();
        }
        LOG.warn("Done stopping extra instances of {}", serviceName);
      }
    } else {
      LOG.info("Starting {} application", serviceName);
      TwillPreparer twillPreparer = getPreparer();
      twillController = twillPreparer.start();

      twillController.addListener(new ServiceListenerAdapter() {

        @Override
        public void running() {
          if (!dsService.isRunning()) {
            LOG.info("Starting dataset service");
            dsService.startAndWait();
          }
        }

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

  private File getSavedHConf() throws IOException {
    File hConfFile = saveHConf(hConf, File.createTempFile("hConf", ".xml"));
    hConfFile.deleteOnExit();
    return hConfFile;
  }

  private File getSavedCConf() throws IOException {
    File cConfFile = saveCConf(cConf, File.createTempFile("cConf", ".xml"));
    cConfFile.deleteOnExit();
    return cConfFile;
  }

  /**
   * Wait for sometime while looking up service in twill.
   */
  private Iterable<TwillController> lookupService() {
    int count = 100;
    Iterable<TwillController> iterable = twillRunnerService.lookup(serviceName);

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

  private TwillPreparer getPreparer() {
    return prepare(twillRunnerService.prepare(twillApplication)
                     .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
    );
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

    runTwillApps();
    ++currentRun;
    lastRunTimeMs = System.currentTimeMillis();
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
}
