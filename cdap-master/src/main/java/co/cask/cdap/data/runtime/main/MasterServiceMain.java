/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data.runtime.main;

import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.app.store.ServiceStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.TwillModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.runtime.DaemonMain;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.security.HBaseSecureStoreUpdater;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.util.hbase.ConfigurationTable;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.explore.service.ExploreServiceUtils;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Services;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.URI;
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
 * Driver class for starting all master services.
 * AppFabricHttpService
 * TwillRunnables: MetricsProcessor, MetricsHttp, LogSaver, TransactionService, StreamHandler.
 */
public class MasterServiceMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(MasterServiceMain.class);

  private static final long MAX_BACKOFF_TIME_MS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
  private static final long SUCCESSFUL_RUN_DURATON_MS = TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES);

  public MasterServiceMain(CConfiguration cConf, Configuration hConf) {
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
  private ExploreClient exploreClient;

  private String serviceName;
  private TwillApplication twillApplication;
  private long lastRunTimeMs = System.currentTimeMillis();
  private int currentRun = 0;
  private boolean isExploreEnabled;

  public static void main(final String[] args) throws Exception {
    LOG.info("Starting {}", MasterServiceMain.class.getSimpleName());
    new MasterServiceMain(CConfiguration.create(), HBaseConfiguration.create()).doMain(args);
  }

  @Override
  public void init(String[] args) {
    isExploreEnabled = cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED);
    serviceName = Constants.Service.MASTER_SERVICES;
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
      new ServiceStoreModules().getDistributedModule(),
      new ExploreClientModule()
    );

    // Initialize ZK client
    zkClientService = baseInjector.getInstance(ZKClientService.class);
    kafkaClientService = baseInjector.getInstance(KafkaClientService.class);
    metricsCollectionService = baseInjector.getInstance(MetricsCollectionService.class);
    dsService = baseInjector.getInstance(DatasetService.class);
    exploreClient = baseInjector.getInstance(ExploreClient.class);
    secureStoreUpdater = baseInjector.getInstance(HBaseSecureStoreUpdater.class);
    serviceStore = baseInjector.getInstance(ServiceStore.class);

    checkTransactionRequirements();
    checkExploreRequirements();
  }

  /**
   * The transaction coprocessors (0.94 and 0.96 versions of {@code DefaultTransactionProcessor}) need access
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
   * Check that if Explore is enabled, the correct jars are present on master node,
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
    LogAppenderInitializer logAppenderInitializer = baseInjector.getInstance(LogAppenderInitializer.class);
    logAppenderInitializer.initialize();

    Futures.getUnchecked(Services.chainStart(zkClientService, kafkaClientService, metricsCollectionService,
                                             serviceStore));
    leaderElection = new LeaderElection(zkClientService, "/election/" + serviceName, new ElectionHandler() {
      @Override
      public void leader() {
        Map<String, Integer> instanceCount = getSystemServiceInstances();
        twillApplication = createTwillApplication(instanceCount);
        if (twillApplication == null) {
          throw new IllegalArgumentException("TwillApplication cannot be null");
        }

        LOG.info("Became leader");
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
        LOG.info("Became follower");

        dsService.stopAndWait();
        if (twillRunnerService != null) {
          // this shuts down the twill runner service but not the twill services themselves
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
    Futures.getUnchecked(Services.chainStop(serviceStore, metricsCollectionService, kafkaClientService,
                                            zkClientService));

    try {
      exploreClient.close();
    } catch (IOException e) {
      LOG.error("Could not close Explore client", e);
      throw Throwables.propagate(e);
    }
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
        LOG.error("Couldn't retrieve instance count {}: {}", service, e.getMessage(), e);
      }
    }
    return instanceCountMap;
  }

  private TwillApplication createTwillApplication(final Map<String, Integer> instanceCountMap) {
    try {
      return new MasterTwillApplication(cConf, getSavedCConf(), getSavedHConf(), isExploreEnabled, instanceCountMap);
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
        LOG.warn("Found more than one instance of {} running; stopping the others", serviceName);
        for (; iterator.hasNext(); ) {
          TwillController controller = iterator.next();
          LOG.warn("Stopping one extra instance of {}", serviceName);
          controller.stopAndWait();
        }
        LOG.warn("Stopped extra instances of {}", serviceName);
      }

      // we have to start the dataset service. Because twill services are already running,
      // it will not be started by the service listener callback below.
      if (!dsService.isRunning()) {
        // we need a new dataset service (the old one may have run before and can't be restarted)
        dsService = baseInjector.getInstance(DatasetService.class); // not a singleton
        LOG.info("Starting Dataset service");
        dsService.startAndWait();
      }

    } else {
      LOG.info("Starting {} application", serviceName);
      TwillPreparer twillPreparer = getPreparer();
      twillController = twillPreparer.start();

      twillController.addListener(new ServiceListenerAdapter() {

        @Override
        public void running() {
          if (!dsService.isRunning()) {
            // we need a new dataset service (the old one may have run before and can't be restarted)
            dsService = baseInjector.getInstance(DatasetService.class); // not a singleton
            LOG.info("Starting Dataset service");
            dsService.startAndWait();
          }
        }

        @Override
        public void failed(Service.State from, Throwable failure) {
          LOG.error("{} failed with exception; restarting with back-off", serviceName, failure);
          backOffRun();
        }

        @Override
        public void terminated(Service.State from) {
          LOG.warn("{} was terminated; restarting with back-off", serviceName);
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
      LOG.warn("Caught interrupted exception", e);
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
      // container by MasterTwillApplication, so they are available for ExploreServiceTwillRunnable
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
      throw new RuntimeException("System property " + Constants.Explore.EXPLORE_CONF_FILES + " is not set");
    }

    // Add all the conf files needed by hive as resources available to containers
    Iterable<File> hiveConfFilesFiles = ExploreServiceUtils.getClassPathJarsFiles(hiveConfFiles);
    Set<String> addedFiles = Sets.newHashSet();
    for (File file : hiveConfFilesFiles) {
      if (file.getName().matches(".*\\.xml") && !file.getName().equals("logback.xml")) {
        if (addedFiles.add(file.getName())) {
          LOG.debug("Adding config file: {}", file.getAbsolutePath());
          preparer = preparer.withResources(ExploreServiceUtils.hijackHiveConfFile(file).toURI());
        } else {
          LOG.warn("Ignoring duplicate config file: {}", file.getAbsolutePath());
        }
      }
    }

    return preparer;
  }

  private TwillPreparer getPreparer() {
    TwillPreparer preparer = twillRunnerService.prepare(twillApplication)
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)));

    // Add yarn queue name if defined
    String queueName = cConf.get(Constants.Service.SCHEDULER_QUEUE);
    if (queueName != null) {
      LOG.info("Setting scheduler queue to {} for master services", queueName);
      preparer.setSchedulerQueue(queueName);
    }

    URL containerLogbackURL = getClass().getResource("/logback-container.xml");
    if (containerLogbackURL != null) {
      try {
        File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        File file = new File(tempDir.getPath(), "logback.xml");

        Files.copy(new File(containerLogbackURL.toURI()), file);
        URI copiedLogbackURI = file.toURI();
        preparer.withResources(copiedLogbackURI);
      } catch (IOException e) {
        LOG.error("Got exception while copying logback-container.xml", e);
      } catch (URISyntaxException e) {
        LOG.error("Got exception while getting URI for logback-container.xml - {}", containerLogbackURL, e);
      }
    } else {
      // Default to system logback if the container logback is not found.
      LOG.debug("Could not load logback specific for containers. Defaulting to system logback.");

      containerLogbackURL = getClass().getResource("/logback.xml");
      if (containerLogbackURL == null) {
        LOG.warn("Cannot find logback.xml to pass onto Twill Runnables!");
      } else {
        try {
          preparer.withResources(containerLogbackURL.toURI());
        } catch (URISyntaxException e) {
          LOG.error("Got exception while getting URI for logback.xml - {}", containerLogbackURL, e);
        }
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
      LOG.info("Current restart run = {}; backing off for {} ms", currentRun, sleepMs);
      TimeUnit.MILLISECONDS.sleep(sleepMs);

    } catch (InterruptedException e) {
      LOG.warn("Caught interrupted exception", e);
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
