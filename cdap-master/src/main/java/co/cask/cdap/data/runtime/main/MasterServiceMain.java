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

import co.cask.cdap.api.metrics.MetricsCollectionService;
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
import co.cask.cdap.common.io.URLConnections;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.common.runtime.DaemonMain;
import co.cask.cdap.common.service.RetryOnStartFailureService;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.util.hbase.ConfigurationTable;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.explore.service.ExploreServiceUtils;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Driver class for starting all master services.
 * AppFabricHttpService
 * TwillRunnables: MetricsProcessor, MetricsHttp, LogSaver, TransactionService, StreamHandler.
 */
public class MasterServiceMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(MasterServiceMain.class);

  private static final long MAX_BACKOFF_TIME_MS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
  private static final long SUCCESSFUL_RUN_DURATON_MS = TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES);

  // Maximum time to try looking up the existing twill application
  private static final long LOOKUP_ATTEMPT_TIMEOUT_MS = 2000;

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final Injector baseInjector;
  private final ZKClientService zkClient;
  private final TwillRunnerService twillRunner;
  private final KafkaClientService kafkaClient;
  private final MetricsCollectionService metricsCollectionService;
  private final ServiceStore serviceStore;
  private final LeaderElection leaderElection;
  private final TokenSecureStoreUpdater secureStoreUpdater;

  private volatile boolean stopped;

  public static void main(final String[] args) throws Exception {
    LOG.info("Starting {}", MasterServiceMain.class.getSimpleName());
    new MasterServiceMain().doMain(args);
  }

  public MasterServiceMain() {
    this.cConf = CConfiguration.create();
    this.cConf.set(Constants.Dataset.Manager.ADDRESS, getLocalHost().getCanonicalHostName());
    this.hConf = HBaseConfiguration.create();

    Injector injector = createBaseInjector(cConf, hConf);
    this.baseInjector = injector;
    this.zkClient = injector.getInstance(ZKClientService.class);
    this.twillRunner = injector.getInstance(TwillRunnerService.class);
    this.kafkaClient = injector.getInstance(KafkaClientService.class);
    this.metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    this.serviceStore = injector.getInstance(ServiceStore.class);
    this.secureStoreUpdater = baseInjector.getInstance(TokenSecureStoreUpdater.class);
    this.leaderElection = createLeaderElection();
  }

  @Override
  public void init(String[] args) {
    cleanupTempDir();

    checkExploreRequirements();
    login();
  }

  @Override
  public void start() {
    try {
      // Workaround for release of file descriptors opened by URLClassLoader - https://issues.cask.co/browse/CDAP-2841
      URLConnections.setDefaultUseCaches(false);
    } catch (IOException e) {
      LOG.error("Could not disable caching of URLJarFiles. This may lead to 'too many open files` exception.", e);
    }

    createSystemHBaseNamespace();
    updateConfigurationTable();

    LogAppenderInitializer logAppenderInitializer = baseInjector.getInstance(LogAppenderInitializer.class);
    logAppenderInitializer.initialize();

    zkClient.startAndWait();
    twillRunner.start();

    // Tries to create the ZK root node (which can be namespaced through the zk connection string)
    Futures.getUnchecked(ZKOperations.ignoreError(zkClient.create("/", null, CreateMode.PERSISTENT),
                                                  KeeperException.NodeExistsException.class, null));
    kafkaClient.startAndWait();
    metricsCollectionService.startAndWait();
    serviceStore.startAndWait();
    leaderElection.startAndWait();
  }

  @Override
  public void stop() {
    LOG.info("Stopping {}", Constants.Service.MASTER_SERVICES);
    stopped = true;

    stopQuietly(leaderElection);
    stopQuietly(serviceStore);
    stopQuietly(metricsCollectionService);
    stopQuietly(kafkaClient);
    stopQuietly(twillRunner);
    stopQuietly(zkClient);

    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      Closeables.closeQuietly(baseInjector.getInstance(ExploreClient.class));
    }
  }

  @Override
  public void destroy() {
  }

  /**
   * Stops a guava {@link Service}. No exception will be thrown even stopping failed.
   */
  private void stopQuietly(Service service) {
    try {
      service.stopAndWait();
    } catch (Exception e) {
      LOG.warn("Exception when stopping service {}", service, e);
    }
  }

  /**
   * Stops a guava {@link Service}. No exception will be thrown even stopping failed.
   */
  private void stopQuietly(TwillRunnerService service) {
    try {
      service.stop();
    } catch (Exception e) {
      LOG.warn("Exception when stopping service {}", service, e);
    }
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
    Map<String, Integer> instanceCountMap = new HashMap<>();
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

  private Injector createBaseInjector(CConfiguration cConf, Configuration hConf) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new LoggingModules().getDistributedModules(),
      new IOModule(),
      new KafkaClientModule(),
      new TwillModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataSetServiceModules().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new ServiceStoreModules().getDistributedModules(),
      new ExploreClientModule(),
      new NotificationFeedServiceRuntimeModule().getDistributedModules(),
      new NotificationServiceRuntimeModule().getDistributedModules(),
      new StreamAdminModules().getDistributedModules()
    );
  }

  /**
   * Creates an unstarted {@link LeaderElection} for the master service.
   */
  private LeaderElection createLeaderElection() {
    String electionPath = "/election/" + Constants.Service.MASTER_SERVICES;
    return new LeaderElection(zkClient, electionPath, new ElectionHandler() {

      private final AtomicReference<TwillController> controller = new AtomicReference<>();
      private final List<Service> services = new ArrayList<>();
      private Cancellable secureStoreUpdateCancellable;
      // Executor for re-running master twill app if it gets terminated.
      private ScheduledExecutorService executor;

      @Override
      public void leader() {
        LOG.info("Became leader for master services");

        final Injector injector = baseInjector.createChildInjector(
          new AppFabricServiceRuntimeModule().getDistributedModules(),
          new ProgramRunnerRuntimeModule().getDistributedModules()
        );

        // Schedule secure store update.
        if (User.isHBaseSecurityEnabled(hConf) || UserGroupInformation.isSecurityEnabled()) {
          secureStoreUpdateCancellable = twillRunner.scheduleSecureStoreUpdate(secureStoreUpdater, 30000L,
                                                                               secureStoreUpdater.getUpdateInterval(),
                                                                               TimeUnit.MILLISECONDS);
        }

        // Create app-fabric and dataset services
        services.add(new RetryOnStartFailureService(new Supplier<Service>() {
          @Override
          public Service get() {
            return injector.getInstance(DatasetService.class);
          }
        }, RetryStrategies.exponentialDelay(200, 5000, TimeUnit.MILLISECONDS)));
        services.add(injector.getInstance(AppFabricServer.class));

        executor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("master-runner"));

        // Start monitoring twill application
        monitorTwillApplication(executor, 0, controller);

        // Start app-fabric and dataset services
        for (Service service : services) {
          LOG.info("Starting service in master: {}", service);
          service.startAndWait();
        }
      }

      @Override
      public void follower() {
        LOG.info("Became follower for master services");
        // Shutdown the retry executor so that no re-run of the twill app will be attempted
        if (executor != null) {
          executor.shutdownNow();
        }
        // Stop secure store update
        if (secureStoreUpdateCancellable != null) {
          secureStoreUpdateCancellable.cancel();
        }
        // If the master process has been explcitly stopped, stop the twill application as well.
        if (stopped) {
          LOG.info("Stopping master twill application");
          TwillController twillController = controller.get();
          if (twillController != null) {
            try {
              twillController.terminate();
              twillController.awaitTerminated();
            } catch (ExecutionException e) {
              LOG.error("Exception while stopping master ", e);
            }
          }
        }
        // Stop local services last since DatasetService is running locally
        // and remote services need it to preserve states.
        for (Service service : Lists.reverse(services)) {
          LOG.info("Stopping service in master: {}", service);
          stopQuietly(service);
        }
        services.clear();
      }
    });
  }

  /**
   * Cleanup the cdap system temp directory.
   */
  private void cleanupTempDir() {
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    try {
      DirUtils.deleteDirectoryContents(tmpDir, true);
    } catch (IOException e) {
      // It's ok not able to cleanup temp directory.
      LOG.debug("Failed to cleanup temp directory {}", tmpDir, e);
    }
  }

  /**
   * Check that if Explore is enabled, the correct jars are present on master node,
   * and that the distribution of Hive is supported.
   */
  private void checkExploreRequirements() {
    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      // This check will throw an exception if Hive is not present or if it's distribution is unsupported
      ExploreServiceUtils.checkHiveSupport();
    }
  }

  /**
   * Performs kerbose login if security is enabled.
   */
  private void login() {
    try {
      SecurityUtil.loginForMasterService(cConf);
    } catch (Exception e) {
      LOG.error("Failed to login as CDAP user", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates HBase namespace for the cdap system namespace.
   */
  private void createSystemHBaseNamespace() {
    HBaseTableUtil tableUtil = baseInjector.getInstance(HBaseTableUtil.class);
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      tableUtil.createNamespaceIfNotExists(admin, Constants.SYSTEM_NAMESPACE_ID);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * The transaction coprocessors (0.94 and 0.96 versions of {@code DefaultTransactionProcessor}) need access
   * to CConfiguration values in order to load transaction snapshots for data cleanup.
   */
  private void updateConfigurationTable() {
    try {
      new ConfigurationTable(hConf).write(ConfigurationTable.Type.DEFAULT, cConf);
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }

  /**
   * Monitors the twill application for master services running through Twill.
   *
   * @param executor executor for re-running the application if it gets terminated
   * @param failures number of failures in starting the application
   * @param serviceController the reference to be updated with the active {@link TwillController}
   */
  private void monitorTwillApplication(final ScheduledExecutorService executor, final int failures,
                                       final AtomicReference<TwillController> serviceController) {
    if (executor.isShutdown()) {
      return;
    }

    // Determines if the application is running. If not, starts a new one.
    final long startTime;
    TwillController controller = getCurrentTwillController();
    if (controller != null) {
      startTime = 0L;
    } else {
      controller = startTwillApplication();
      startTime = System.currentTimeMillis();
    }

    // Monitor the application
    serviceController.set(controller);
    controller.onTerminated(new Runnable() {
      @Override
      public void run() {
        if (executor.isShutdown()) {
          return;
        }
        LOG.warn("{} was terminated; restarting with back-off", Constants.Service.MASTER_SERVICES);
        backoffRun();
      }

      private void backoffRun() {
        if (System.currentTimeMillis() - startTime > SUCCESSFUL_RUN_DURATON_MS) {
          // Restart immediately
          executor.execute(new Runnable() {
            @Override
            public void run() {
              monitorTwillApplication(executor, 0, serviceController);
            }
          });
          return;
        }

        long nextRunTime = Math.min(500 * (long) Math.pow(2, failures + 1), MAX_BACKOFF_TIME_MS);
        executor.schedule(new Runnable() {
          @Override
          public void run() {
            monitorTwillApplication(executor, failures + 1, serviceController);
          }
        }, nextRunTime, TimeUnit.MILLISECONDS);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  /**
   * Returns the {@link TwillController} for the current master service or {@code null} if none is running.
   */
  @Nullable
  private TwillController getCurrentTwillController() {
    int count = 100;
    long sleepMs = LOOKUP_ATTEMPT_TIMEOUT_MS / count;

    // Try to lookup the existing twill application
    for (int i = 0; i < count; i++) {
      TwillController result = null;
      for (TwillController controller : twillRunner.lookup(Constants.Service.MASTER_SERVICES)) {
        if (result != null) {
          LOG.warn("Stopping one extra instance of {}", Constants.Service.MASTER_SERVICES);
          try {
            controller.terminate();
            controller.awaitTerminated();
          } catch (ExecutionException e) {
            LOG.warn("Exception while Stopping one extra instance of {} - {}", Constants.Service.MASTER_SERVICES, e);
          }
        } else {
          result = controller;
        }
      }
      if (result != null) {
        return result;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(sleepMs);
      } catch (InterruptedException e) {
        break;
      }
    }
    return null;
  }

  /**
   * Starts the {@link TwillApplication} for the master services.
   *
   * @return The {@link TwillController} for the application.
   */
  private TwillController startTwillApplication() {
    try {
      // Create a temp dir for the run to hold temporary files created to run the application
      Path tempPath = Files.createDirectories(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                       cConf.get(Constants.AppFabric.TEMP_DIR)).toPath());
      final Path runDir = Files.createTempDirectory(tempPath, "master");
      try {
        Path cConfFile = saveCConf(cConf, runDir.resolve("cConf.xml"));
        Path hConfFile = saveHConf(hConf, runDir.resolve("hConf.xml"));
        Path logbackFile = saveLogbackConf(runDir.resolve("logback.xml"));

        TwillPreparer preparer = twillRunner.prepare(new MasterTwillApplication(cConf, cConfFile.toFile(),
                                                                                hConfFile.toFile(),
                                                                                getSystemServiceInstances()))
          .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)));
        // Add logback xml
        if (logbackFile.toFile().isFile()) {
          preparer.withResources().withResources(logbackFile.toUri());
        }

        // Add yarn queue name if defined
        String queueName = cConf.get(Constants.Service.SCHEDULER_QUEUE);
        if (queueName != null) {
          LOG.info("Setting scheduler queue to {} for master services", queueName);
          preparer.setSchedulerQueue(queueName);
        }

        // Add HBase dependencies
        preparer.withDependencies(baseInjector.getInstance(HBaseTableUtil.class).getClass());

        // Add secure tokens
        if (User.isHBaseSecurityEnabled(hConf) || UserGroupInformation.isSecurityEnabled()) {
          // TokenSecureStoreUpdater.update() ignores parameters
          preparer.addSecureStore(secureStoreUpdater.update(null, null));
        }

        // Add explore dependencies
        if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
          prepareExploreContainer(preparer);
        }

        // Add a listener to delete temp files when application started/terminated.
        TwillController controller = preparer.start();
        Runnable cleanup = new Runnable() {
          @Override
          public void run() {
            try {
              File dir = runDir.toFile();
              if (dir.isDirectory()) {
                DirUtils.deleteDirectoryContents(dir);
              }
            } catch (IOException e) {
              LOG.warn("Failed to cleanup directory {}", runDir, e);
            }
          }
        };
        controller.onRunning(cleanup, Threads.SAME_THREAD_EXECUTOR);
        controller.onTerminated(cleanup, Threads.SAME_THREAD_EXECUTOR);
        return controller;
      } catch (Exception e) {
        try {
          DirUtils.deleteDirectoryContents(runDir.toFile());
        } catch (IOException ex) {
          LOG.warn("Failed to cleanup directory {}", runDir, ex);
          e.addSuppressed(ex);
        }
        throw e;
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Prepare the specs of the twill application for the Explore twill runnable.
   * Add jars needed by the Explore module in the classpath of the containers, and
   * add conf files (hive_site.xml, etc) as resources available for the Explore twill
   * runnable.
   */
  private TwillPreparer prepareExploreContainer(TwillPreparer preparer) {
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

  private Path saveCConf(CConfiguration conf, Path file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file, Charsets.UTF_8)) {
      conf.writeXml(writer);
    }
    return file;
  }

  private Path saveHConf(Configuration conf, Path file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file, Charsets.UTF_8)) {
      conf.writeXml(writer);
    }
    return file;
  }

  private Path saveLogbackConf(Path file) throws IOException {
    // Default to system logback if the container logback is not found.
    URL logbackResource = getClass().getResource("/logback-container.xml");
    if (logbackResource == null) {
      logbackResource = getClass().getResource("/logback.xml");
    }
    if (logbackResource != null) {
      try (InputStream input = logbackResource.openStream()) {
        Files.copy(input, file);
      }
    } else {
      LOG.warn("Cannot find logback.xml.");
    }

    return file;
  }
}
