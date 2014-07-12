package com.continuuity.data.runtime.main;

import com.continuuity.app.guice.AppFabricServiceRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.app.guice.ServiceStoreModules;
import com.continuuity.app.store.ServiceStore;
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
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data.security.HBaseSecureStoreUpdater;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.util.hbase.ConfigurationTable;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.explore.service.ExploreServiceUtils;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Services;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
  private KafkaClientService kafkaClientService;
  private MetricsCollectionService metricsCollectionService;
  private DatasetService dsService;
  private ServiceStore serviceStore;
  private HBaseSecureStoreUpdater secureStoreUpdater;

  private String serviceName;
  private TwillApplication twillApplication;
  private long lastRunTimeMs = System.currentTimeMillis();
  private int currentRun = 0;
  private boolean isExploreEnabled;

  public static void main(final String[] args) throws Exception {
    LOG.info("Starting Reactor Service Main...");
    new ReactorServiceMain(CConfiguration.create(), HBaseConfiguration.create()).doMain(args);
  }

  @Override
  public void init(String[] args) {
    isExploreEnabled = cConf.getBoolean(Constants.Explore.CFG_EXPLORE_ENABLED);
    serviceName = Constants.Service.REACTOR_SERVICES;
    cConf.set(Constants.Dataset.Manager.ADDRESS, getLocalHost().getCanonicalHostName());

    baseInjector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new LoggingModules().getDistributedModules(),
      new IOModule(),
      new AuthModule(),
      new KafkaClientModule(),
      new TwillModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new DataSetServiceModules().getDistributedModule(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModule(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new ServiceStoreModules().getDistributedModule()
    );

    // Initialize ZK client
    zkClientService = baseInjector.getInstance(ZKClientService.class);
    kafkaClientService = baseInjector.getInstance(KafkaClientService.class);
    metricsCollectionService = baseInjector.getInstance(MetricsCollectionService.class);
    dsService = baseInjector.getInstance(DatasetService.class);
    serviceStore = baseInjector.getInstance(ServiceStore.class);

    secureStoreUpdater = baseInjector.getInstance(HBaseSecureStoreUpdater.class);

    checkTransactionRequirements();
    checkExploreRequirements();
  }

  /**
   * The transaction coprocessors (0.94 and 0.96 versions of {@code ReactorTransactionDataJanitor}) need access
   * to CConfiguration values in order to load transaction snapshots for data cleanup.
   */
  private void checkTransactionRequirements() {
    try {
      new ConfigurationTable(hConf).write(ConfigurationTable.Type.DEFAULT, cConf);
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }

  /**
   * Check that if Explore is enabled, the correct jars are present on reactor-master node,
   * and that the distribution of Hive is supported.
   */
  private void checkExploreRequirements() {
    if (!isExploreEnabled) {
      return;
    }

    // This checking will throw an exception if Hive is not present or if its distribution is unsupported
    ExploreServiceUtils.checkHiveSupportWithSecurity(hConf);
  }

  @Override
  public void start() {
    Services.chainStart(zkClientService, kafkaClientService, metricsCollectionService);

    leaderElection = new LeaderElection(zkClientService, "/election/" + serviceName, new ElectionHandler() {
      @Override
      public void leader() {
        Map<String, Integer> instanceCount = getSystemServiceInstances();
        twillApplication = createTwillApplication(instanceCount);
        if (twillApplication == null) {
          throw new IllegalArgumentException("TwillApplication cannot be null");
        }

        LOG.info("Became leader.");
        Injector injector = baseInjector.createChildInjector();

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

    if (leaderElection != null) {
      leaderElection.stopAndWait();
    }
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

  /**
   * Returns a map from system service name to a map from property to configuration key.
   */
  private Map<String, Map<String, String>> getConfigKeys() {
    Map<String, Map<String, String>> configKeys = Maps.newHashMap();

    configKeys.put(Constants.Service.LOGSAVER,
                   ImmutableMap.of("default", Constants.LogSaver.NUM_INSTANCES,
                                   "max", Constants.LogSaver.MAX_INSTANCES));
    configKeys.put(Constants.Service.TRANSACTION,
                   ImmutableMap.of("default", Constants.Transaction.Container.NUM_INSTANCES,
                                   "max", Constants.Transaction.Container.MAX_INSTANCES));
    configKeys.put(Constants.Service.METRICS_PROCESSOR,
                   ImmutableMap.of("default", Constants.MetricsProcessor.NUM_INSTANCES,
                                   "max", Constants.MetricsProcessor.MAX_INSTANCES));
    configKeys.put(Constants.Service.METRICS,
                   ImmutableMap.of("default", Constants.Metrics.NUM_INSTANCES,
                                   "max", Constants.Metrics.MAX_INSTANCES));
    configKeys.put(Constants.Service.STREAMS,
                   ImmutableMap.of("default", Constants.Stream.CONTAINER_INSTANCES,
                                   "max", Constants.Stream.MAX_INSTANCES));
    configKeys.put(Constants.Service.DATASET_EXECUTOR,
                   ImmutableMap.of("default", Constants.Dataset.Executor.CONTAINER_INSTANCES,
                                   "max", Constants.Dataset.Executor.MAX_INSTANCES));
    configKeys.put(Constants.Service.EXPLORE_HTTP_USER_SERVICE,
                   ImmutableMap.of("default", Constants.Explore.CONTAINER_INSTANCES,
                                   "max", Constants.Explore.MAX_INSTANCES));
    return configKeys;
  }

  private Map<String, Integer> getSystemServiceInstances() {
    Map<String, Integer> instanceCountMap = new HashMap<String, Integer>();
    for (Map.Entry<String, Map<String, String>> entry : getConfigKeys().entrySet()) {
      String service = entry.getKey();
      Map<String, String> configKeys = entry.getValue();
      try {
        int maxCount = cConf.getInt(configKeys.get("max"));

        Integer savedCount = serviceStore.getServiceInstance(service);
        if (savedCount == null || savedCount == 0) {
          savedCount = Math.min(maxCount, cConf.getInt(configKeys.get("default")));
        } else {
          // If the max value is smaller than the saved instance count, update the store to the max value.
          if (savedCount > maxCount) {
            savedCount = maxCount;
          }
        }

        serviceStore.setServiceInstance(service, savedCount);
        instanceCountMap.put(service, savedCount);
        LOG.info("Setting instance count of {} Service to {}", service, savedCount);
      } catch (Exception e) {
        LOG.error("Couldn't retrieve instance count {} : {}", service, e.getMessage(), e);
      }
    }
    return instanceCountMap;
  }

  private TwillApplication createTwillApplication(final Map<String, Integer> instanceCountMap) {
    try {
      return new ReactorTwillApplication(cConf, getSavedCConf(), getSavedHConf(), isExploreEnabled, instanceCountMap);
    } catch (Exception e) {
      throw  Throwables.propagate(e);
    }
  }

  private void scheduleSecureStoreUpdate(TwillRunner twillRunner) {
    if (User.isHBaseSecurityEnabled(hConf)) {
      twillRunner.scheduleSecureStoreUpdate(secureStoreUpdater, 30000L, secureStoreUpdater.getUpdateInterval(),
                                            TimeUnit.MILLISECONDS);
    }
  }


  private TwillPreparer prepare(TwillPreparer preparer) {
    return preparer.withDependencies(new HBaseTableUtilFactory().get().getClass())
      .addSecureStore(secureStoreUpdater.update(null, null)); // HBaseSecureStoreUpdater.update() ignores parameters
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

  /**
   * Prepare the specs of the twill application for the Explore twill runnable.
   * Add jars needed by the Explore module in the classpath of the containers, and
   * add conf files (hive_site.xml, etc) as resources available for the Explore twill
   * runnable.
   */
  private TwillPreparer prepareExploreContainer(TwillPreparer preparer) {
    if (!isExploreEnabled) {
      return preparer;
    }

    try {
      // Put jars needed by Hive in the containers classpath. Those jars are localized in the Explore
      // container by ReactorTwillApplication, so they are available for ExploreServiceTwillRunnable
      Set<File> jars = ExploreServiceUtils.traceExploreDependencies();
      for (File jarFile : jars) {
        LOG.trace("Adding jar file to classpath: {}", jarFile.getName());
        preparer = preparer.withClassPaths(jarFile.getName());
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to trace Explore dependencies", e);
    }

    // EXPLORE_CONF_FILES will be defined in startup scripts if Hive is installed.
    String hiveConfFiles = System.getProperty(Constants.Explore.EXPLORE_CONF_FILES);
    LOG.debug("Hive conf files = {}", hiveConfFiles);
    if (hiveConfFiles == null) {
      throw new RuntimeException("System property " + Constants.Explore.EXPLORE_CONF_FILES + " is not set.");
    }

    // Add all the conf files needed by hive as resources available to containers
    Iterable<File> hiveConfFilesFiles = ExploreServiceUtils.getClassPathJarsFiles(hiveConfFiles);
    for (File file : hiveConfFilesFiles) {
      if (file.getName().matches(".*\\.xml")) {
        preparer = preparer.withResources(file.toURI());
      }
    }

    return preparer;
  }

  private TwillPreparer getPreparer() {
    TwillPreparer preparer = twillRunnerService.prepare(twillApplication)
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)));

    // Add system logback file to the preparer
    URL logbackUrl = getClass().getResource("/logback.xml");
    if (logbackUrl == null) {
      LOG.warn("Cannot find logback.xml to pass onto Twill Runnables!");
    } else {
      try {
        preparer.withResources(logbackUrl.toURI());
      } catch (URISyntaxException e) {
        LOG.error("Got exception while getting URI for logback.xml - {}", logbackUrl);
      }
    }
    preparer = prepareExploreContainer(preparer);
    return prepare(preparer);
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
