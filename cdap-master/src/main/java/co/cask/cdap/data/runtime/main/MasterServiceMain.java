/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.app.guice.AuthorizationModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.app.guice.TwillModule;
import co.cask.cdap.app.store.ServiceStore;
import co.cask.cdap.common.MasterUtils;
import co.cask.cdap.common.app.MainClassLoader;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.FileContextProvider;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.io.URLConnections;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.runtime.DaemonMain;
import co.cask.cdap.common.service.RetryOnStartFailureService;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.Services;
import co.cask.cdap.common.twill.HadoopClassExcluder;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.util.hbase.ConfigurationTable;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.explore.service.ExploreServiceUtils;
import co.cask.cdap.hive.ExploreUtils;
import co.cask.cdap.internal.app.runtime.batch.distributed.MapReduceContainerHelper;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.master.startup.ServiceResourceKeys;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsStoreModule;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.operations.OperationalStatsLoader;
import co.cask.cdap.operations.OperationalStatsService;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.TokenSecureStoreUpdater;
import co.cask.cdap.security.authorization.AuthorizationBootstrapper;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationEnforcementService;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
  private final ZKClientService zkClient;
  private final LeaderElection leaderElection;
  private final LogAppenderInitializer logAppenderInitializer;

  private volatile boolean stopped;

  static {
    try {
      // Workaround for release of file descriptors opened by URLClassLoader - https://issues.cask.co/browse/CDAP-2841
      URLConnections.setDefaultUseCaches(false);
    } catch (IOException e) {
      LOG.error("Could not disable caching of URLJarFiles. This may lead to 'too many open files` exception.", e);
    }
  }

  public static void main(final String[] args) throws Exception {
    ClassLoader classLoader = MainClassLoader.createFromContext();
    if (classLoader == null) {
      LOG.warn("Failed to create CDAP system ClassLoader. AuthEnforce annotation will not be rewritten");
      new MasterServiceMain().doMain(args);
    } else {
      Thread.currentThread().setContextClassLoader(classLoader);
      Class<?> cls = classLoader.loadClass(MasterServiceMain.class.getName());
      // since doMain is in the DaemonMain super class
      Method method = cls.getSuperclass().getDeclaredMethod("doMain", String[].class);
      method.setAccessible(true);
      method.invoke(cls.newInstance(), new Object[]{args});
    }
  }

  public MasterServiceMain() {
    CConfiguration cConf = CConfiguration.create();

    // Note: login has to happen before any objects that need Kerberos credentials are instantiated.
    login(cConf);

    Configuration hConf = HBaseConfiguration.create();

    Injector injector = createProcessInjector(cConf, hConf);
    this.cConf = injector.getInstance(CConfiguration.class);
    this.hConf = injector.getInstance(Configuration.class);
    this.logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    this.zkClient = injector.getInstance(ZKClientService.class);
    this.leaderElection = createLeaderElection();

    // leader election will normally stay running. Will only stop if there was some issue starting up.
    this.leaderElection.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        if (!stopped) {
          LOG.error("CDAP Master failed to start");
          System.exit(1);
        }
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        if (!stopped) {
          LOG.error("CDAP Master failed to start");
          System.exit(1);
        }
      }
    }, MoreExecutors.sameThreadExecutor());
  }

  @Override
  public void init(String[] args) {
    cleanupTempDir();
    checkExploreRequirements();
  }

  @Override
  public void start() throws Exception {
    logAppenderInitializer.initialize();
    createDirectory("twill");
    createDirectory(cConf.get(QueueConstants.ConfigKeys.QUEUE_TABLE_COPROCESSOR_DIR,
                              QueueConstants.DEFAULT_QUEUE_TABLE_COPROCESSOR_DIR));
    createSystemHBaseNamespace();
    updateConfigurationTable();
    Services.startAndWait(zkClient, cConf.getLong(Constants.Zookeeper.CLIENT_STARTUP_TIMEOUT_MILLIS),
                          TimeUnit.MILLISECONDS,
                          String.format("Connection timed out while trying to start ZooKeeper client. Please " +
                                          "verify that the ZooKeeper quorum settings are correct in cdap-site.xml. " +
                                          "Currently configured as: %s", cConf.get(Constants.Zookeeper.QUORUM)));
    // Tries to create the ZK root node (which can be namespaced through the zk connection string)
    Futures.getUnchecked(ZKOperations.ignoreError(zkClient.create("/", null, CreateMode.PERSISTENT),
                                                  KeeperException.NodeExistsException.class, null));

    leaderElection.startAndWait();
  }

  /**
   * CDAP-6644 for secure impersonation to work,
   * we want other users to be able to write to the "path" directory,
   * currently only cdap.user has read-write permissions
   * while other users can only read the "{hdfs.namespace}/{path}" dir,
   * we want to let others to be able to write to "path" directory, till we have a better solution.
   */
  private void createDirectory(String path) {
    String hdfsNamespace = cConf.get(Constants.CFG_HDFS_NAMESPACE);
    String pathPrefix = hdfsNamespace.startsWith("/") ? hdfsNamespace : "/" + hdfsNamespace;
    String namespacedPath = String.format("%s/%s", pathPrefix, path);
    createDirectory(new FileContextProvider(cConf, hConf).get(), namespacedPath);
  }

  private void createDirectory(FileContext fileContext, String path) {
    try {
      org.apache.hadoop.fs.Path fPath = new org.apache.hadoop.fs.Path(path);
      boolean dirExists = checkDirectoryExists(fileContext, fPath);
      if (!dirExists) {
        FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
        // file context does ( permission AND  (NOT of umask) ) and uses that as permission, by default umask is 022,
        // if we want 777 permission, we have to set umask to 000
        fileContext.setUMask(new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));
        fileContext.mkdir(fPath, permission, false);
      }
    } catch (FileAlreadyExistsException e) {
      // should not happen as we create only if dir exists
    } catch (AccessControlException | ParentNotDirectoryException | FileNotFoundException e) {
      // just log the exception
      LOG.error("Exception while trying to create directory at {}", path, e);
    }  catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private boolean checkDirectoryExists(FileContext fileContext, org.apache.hadoop.fs.Path path) throws IOException {
    if (fileContext.util().exists(path)) {
      // dir exists
      // check permissions
      FsAction action =
        fileContext.getFileStatus(path).getPermission().getOtherAction();
      if (!action.implies(FsAction.WRITE)) {
        LOG.error("Directory {} is not writable for others, If you are using secure impersonation, " +
                    "make this directory writable for others, else you can ignore this message.", path);
      }
      return true;
    }
    return false;
  }

  @Override
  public void stop() {
    LOG.info("Stopping {}", Constants.Service.MASTER_SERVICES);
    stopped = true;

    // if leader election failed to start, its listener will stop the master.
    // In that case, we don't want to try stopping it again, as it will log confusing exceptions
    if (leaderElection.isRunning()) {
      stopQuietly(leaderElection);
    }
    stopQuietly(zkClient);
  }

  @Override
  public void destroy() {
    // no-op
  }

  /**
   * Gets an instance of the given {@link Service} class from the given {@link Injector}, start the service and
   * returns it.
   */
  private <T extends Service> T getAndStart(Injector injector, Class<T> cls) {
    T service = injector.getInstance(cls);
    LOG.info("Starting service in master {}", service);
    service.startAndWait();
    return service;
  }

  /**
   * Stops a guava {@link Service}. No exception will be thrown even stopping failed.
   */
  private void stopQuietly(@Nullable Service service) {
    try {
      if (service != null) {
        LOG.info("Stopping service in master: {}", service);
        service.stopAndWait();
      }
    } catch (Exception e) {
      LOG.warn("Exception when stopping service {}", service, e);
    }
  }

  /**
   * Stops a guava {@link Service}. No exception will be thrown even stopping failed.
   */
  private void stopQuietly(@Nullable TwillRunnerService service) {
    try {
      if (service != null) {
        service.stop();
      }
    } catch (Exception e) {
      LOG.warn("Exception when stopping service {}", service, e);
    }
  }

  private Map<String, Integer> getSystemServiceInstances(ServiceStore serviceStore) {
    Map<String, Integer> instanceCountMap = new HashMap<>();
    Set<ServiceResourceKeys> serviceResourceKeysSet = MasterUtils.createSystemServicesResourceKeysSet(cConf);
    for (ServiceResourceKeys serviceResourceKeys : serviceResourceKeysSet) {
      String service = serviceResourceKeys.getServiceName();
      try {
        int maxCount = serviceResourceKeys.getMaxInstances();

        Integer savedCount = serviceStore.getServiceInstance(service);
        if (savedCount == null || savedCount == 0) {
          savedCount = Math.min(maxCount, serviceResourceKeys.getInstances());
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


  /**
   * Creates an unstarted {@link LeaderElection} for the master service.
   */
  private LeaderElection createLeaderElection() {
    String electionPath = "/election/" + Constants.Service.MASTER_SERVICES;
    return new LeaderElection(zkClient, electionPath, new MasterLeaderElectionHandler(cConf, hConf, zkClient));
  }

  /**
   * Cleanup the cdap system temp directory.
   */
  private void cleanupTempDir() {
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();

    if (!tmpDir.isDirectory()) {
      return;
    }

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
  private void login(CConfiguration cConf) {
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
    HBaseTableUtil tableUtil = new HBaseTableUtilFactory(cConf).get();
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      tableUtil.createNamespaceIfNotExists(admin, tableUtil.getHBaseNamespace(NamespaceId.SYSTEM));
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
   * Creates a guice {@link Injector} used by this master service process.
   */
  @VisibleForTesting
  static Injector createProcessInjector(CConfiguration cConf, Configuration hConf) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new LoggingModules().getDistributedModules()
    );
  }

  /**
   * Creates a guice {@link Injector} to be used when this master service becomes leader.
   */
  @VisibleForTesting
  static Injector createLeaderInjector(CConfiguration cConf, Configuration hConf,
                                       final ZKClientService zkClientService) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new AbstractModule() {
        @Override
        protected void configure() {
          // Instead of using ZKClientModule that will create new instance of ZKClient, we create instance
          // binding to reuse the same ZKClient used for leader election
          bind(ZKClient.class).toInstance(zkClientService);
          bind(ZKClientService.class).toInstance(zkClientService);
        }
      },
      new LoggingModules().getDistributedModules(),
      new LocationRuntimeModule().getDistributedModules(),
      new IOModule(),
      new KafkaClientModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataSetServiceModules().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new MetricsStoreModule(),
      new ExploreClientModule(),
      new NotificationFeedServiceRuntimeModule().getDistributedModules(),
      new NotificationServiceRuntimeModule().getDistributedModules(),
      new ViewAdminModules().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new NamespaceStoreModule().getDistributedModules(),
      new AuditModule().getDistributedModules(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      new TwillModule(),
      new ServiceStoreModules().getDistributedModules(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new SecureStoreModules().getDistributedModules(),
      new PrivateModule() {
        @Override
        protected void configure() {
          bind(OperationalStatsLoader.class).in(Scopes.SINGLETON);
          bind(OperationalStatsService.class).in(Scopes.SINGLETON);
          expose(OperationalStatsService.class);
        }
      }
    );
  }

  /**
   * The {@link ElectionHandler} for handling leader election lifecycle for the master process.
   */
  private final class MasterLeaderElectionHandler implements ElectionHandler {

    private final CConfiguration cConf;
    private final Configuration hConf;
    private final ZKClientService zkClient;
    private final AtomicReference<TwillController> controller = new AtomicReference<>();
    private final List<Service> services = new ArrayList<>();

    private Injector injector;
    private Cancellable secureStoreUpdateCancellable;
    // Executor for re-running master twill app if it gets terminated.
    private ScheduledExecutorService executor;
    private AuthorizerInstantiator authorizerInstantiator;
    private TwillRunnerService twillRunner;
    private ServiceStore serviceStore;
    private TokenSecureStoreUpdater secureStoreUpdater;
    private ExploreClient exploreClient;

    private MasterLeaderElectionHandler(CConfiguration cConf, Configuration hConf, ZKClientService zkClient) {
      this.cConf = cConf;
      this.hConf = hConf;
      this.zkClient = zkClient;
    }

    @Override
    public void leader() {
      LOG.info("Became leader for master services");

      // We need to create a new injector each time becoming leader so that new instances of singleton Services
      // will be created
      injector = createLeaderInjector(cConf, hConf, zkClient);

      if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
        exploreClient = injector.getInstance(ExploreClient.class);
      }

      authorizerInstantiator = injector.getInstance(AuthorizerInstantiator.class);
      // Authorization bootstrapping is a blocking call, because CDAP will not start successfully if it does not
      // succeed on an authorization-enabled cluster
      injector.getInstance(AuthorizationBootstrapper.class).run();
      services.add(getAndStart(injector, KafkaClientService.class));
      services.add(getAndStart(injector, MetricsCollectionService.class));
      services.add(getAndStart(injector, AuthorizationEnforcementService.class));
      services.add(getAndStart(injector, OperationalStatsService.class));
      serviceStore = getAndStart(injector, ServiceStore.class);
      services.add(serviceStore);

      twillRunner = injector.getInstance(TwillRunnerService.class);
      twillRunner.start();

      secureStoreUpdater = injector.getInstance(TokenSecureStoreUpdater.class);

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
      monitorTwillApplication(executor, 0, controller, twillRunner, serviceStore, secureStoreUpdater);

      // Starts all services.
      for (Service service : services) {
        if (service.isRunning()) {
          // Some services already started (e.g. MetricsCollectionService, KafkaClientService)
          continue;
        }
        LOG.info("Starting service in master: {}", service);
        try {
          service.startAndWait();
        } catch (Throwable t) {
          // shut down the executor and stop the twill app,
          // then throw an exception to cause the leader election service to stop
          // leaderelection's listener will then shutdown the master
          stop(true);
          throw new RuntimeException(String.format("Unable to start service %s: %s", service, t.getMessage()));
        }
      }
      LOG.info("CDAP Master started successfully.");
    }

    @Override
    public void follower() {
      LOG.info("Became follower for master services");
      stop(stopped);
    }

    private void stop(boolean shouldTerminateApp) {
      // Shutdown the retry executor so that no re-run of the twill app will be attempted
      if (executor != null) {
        executor.shutdownNow();
      }
      // Stop secure store update
      if (secureStoreUpdateCancellable != null) {
        secureStoreUpdateCancellable.cancel();
      }
      // If the master process has been explcitly stopped, stop the twill application as well.
      if (shouldTerminateApp) {
        LOG.info("Stopping master twill application");
        TwillController twillController = controller.get();
        if (twillController != null) {
          Futures.getUnchecked(twillController.terminate());
        }
      }
      // Stop local services last since DatasetService is running locally
      // and remote services need it to preserve states.
      for (Service service : Lists.reverse(services)) {
        stopQuietly(service);
      }
      services.clear();
      stopQuietly(twillRunner);
      Closeables.closeQuietly(authorizerInstantiator);
      Closeables.closeQuietly(exploreClient);
    }


    /**
     * Monitors the twill application for master services running through Twill.
     *
     * @param executor executor for re-running the application if it gets terminated
     * @param failures number of failures in starting the application
     * @param serviceController the reference to be updated with the active {@link TwillController}
     */
    private void monitorTwillApplication(final ScheduledExecutorService executor, final int failures,
                                         final AtomicReference<TwillController> serviceController,
                                         final TwillRunnerService twillRunner, final ServiceStore serviceStore,
                                         final TokenSecureStoreUpdater secureStoreUpdater) {
      if (executor.isShutdown()) {
        return;
      }

      // Determines if the application is running. If not, starts a new one.
      final long startTime;
      TwillController controller = getCurrentTwillController(twillRunner);
      if (controller != null) {
        startTime = 0L;
      } else {
        try {
          controller = startTwillApplication(twillRunner, serviceStore, secureStoreUpdater);
        } catch (Exception e) {
          LOG.error("Failed to start master twill application", e);
          throw e;
        }
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
                monitorTwillApplication(executor, 0, serviceController, twillRunner, serviceStore, secureStoreUpdater);
              }
            });
            return;
          }

          long nextRunTime = Math.min(500 * (long) Math.pow(2, failures + 1), MAX_BACKOFF_TIME_MS);
          executor.schedule(new Runnable() {
            @Override
            public void run() {
              monitorTwillApplication(executor, failures + 1, serviceController,
                                      twillRunner, serviceStore, secureStoreUpdater);
            }
          }, nextRunTime, TimeUnit.MILLISECONDS);
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    }

    /**
     * Returns the {@link TwillController} for the current master service or {@code null} if none is running.
     */
    @Nullable
    private TwillController getCurrentTwillController(TwillRunnerService twillRunner) {
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
    private TwillController startTwillApplication(TwillRunnerService twillRunner,
                                                  ServiceStore serviceStore,
                                                  TokenSecureStoreUpdater secureStoreUpdater) {
      try {
        // Create a temp dir for the run to hold temporary files created to run the application
        Path tempPath = Files.createDirectories(Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                          cConf.get(Constants.AppFabric.TEMP_DIR)).toAbsolutePath());
        final Path runDir = Files.createTempDirectory(tempPath, "master");
        try {
          Path cConfFile = saveCConf(cConf, runDir.resolve("cConf.xml"));
          Path hConfFile = saveHConf(hConf, runDir.resolve("hConf.xml"));
          Path logbackFile = saveLogbackConf(runDir.resolve("logback.xml"));

          boolean exploreEnabled = cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED);
          Map<String, LocalizeResource> exploreResources = Collections.emptyMap();
          List<String> exploreExtraClassPaths = new ArrayList<>();
          if (exploreEnabled) {
            exploreResources = getExploreDependencies(runDir, exploreExtraClassPaths);
          }

          TwillPreparer preparer = twillRunner.prepare(
            new MasterTwillApplication(cConf, cConfFile.toFile(), hConfFile.toFile(),
                                       getSystemServiceInstances(serviceStore), exploreResources));

          if (cConf.getBoolean(Constants.COLLECT_CONTAINER_LOGS)) {
            if (LOG instanceof ch.qos.logback.classic.Logger) {
              preparer.addLogHandler(new LogHandler() {
                @Override
                public void onLog(LogEntry entry) {
                  ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LOG;
                  logger.callAppenders(new TwillLogEntryAdapter(entry));
                }
              });
            } else {
              LOG.warn("Unsupported logger binding ({}) for container log collection. Falling back to System.out.",
                       LOG.getClass().getName());
              preparer.addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)));
            }
          } else {
            preparer.addJVMOptions("-Dtwill.disable.kafka=true");
          }

          // Add logback xml
          if (Files.exists(logbackFile)) {
            preparer
              .withResources(logbackFile.toUri())
              .withEnv(Collections.singletonMap("CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR));
          }

          // Add yarn queue name if defined
          String queueName = cConf.get(Constants.Service.SCHEDULER_QUEUE);
          if (queueName != null) {
            LOG.info("Setting scheduler queue to {} for master services", queueName);
            preparer.setSchedulerQueue(queueName);
          }

          // Add HBase dependencies
          preparer.withDependencies(injector.getInstance(HBaseTableUtil.class).getClass());

          // Add secure tokens
          if (User.isHBaseSecurityEnabled(hConf) || UserGroupInformation.isSecurityEnabled()) {
            preparer.addSecureStore(secureStoreUpdater.update());
          }

          // add hadoop classpath to application classpath and exclude hadoop classes from bundle jar.
          List<String> yarnAppClassPath = Arrays.asList(
            hConf.getTrimmedStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));

          preparer.withApplicationClassPaths(yarnAppClassPath)
            .withBundlerClassAcceptor(new HadoopClassExcluder());

          // Add explore dependencies
          if (exploreEnabled) {
            prepareExploreContainer(preparer, exploreExtraClassPaths, yarnAppClassPath);
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
    private TwillPreparer prepareExploreContainer(TwillPreparer preparer,
                                                  Iterable<String> exploreExtraClassPaths,
                                                  Iterable<String> yarnAppClassPath) throws IOException {
      // Adding all hive jar files to the container classpath.
      // Ideally we only want it for the explore runnable container, but Twill doesn't support it per runnable.
      // However, setting these extra classpath for non-explore container is fine because those hive will be
      // missing from the container (localization is done by the MasterTwillApplication for the explore runnable only).

      boolean yarnFirst = cConf.getBoolean(Constants.Explore.CONTAINER_YARN_APP_CLASSPATH_FIRST);
      if (yarnFirst) {
        // It's ok to have yarn application classpath set here even it can affect non-explore container,
        // since we anyway have yarn application classpath in the "withApplicationClassPaths".
        preparer = preparer.withClassPaths(Iterables.concat(yarnAppClassPath, exploreExtraClassPaths));
      } else {
        preparer = preparer.withClassPaths(exploreExtraClassPaths);
      }

      // Add all the conf files needed by hive as resources. They will be available in the explore container classpath
      Set<String> addedFiles = Sets.newHashSet();
      for (File file : ExploreUtils.getExploreConfFiles()) {
        String name = file.getName();
        if (name.equals("logback.xml") || !name.endsWith(".xml")) {
          continue;
        }
        if (addedFiles.add(name)) {
          LOG.debug("Adding config file: {}", file.getAbsolutePath());
          preparer = preparer.withResources(file.toURI());
        } else {
          LOG.warn("Ignoring duplicate config file: {}", file);
        }
      }

      // Setup SPARK_HOME environment variable as well if spark is configured
      String sparkHome = System.getenv(Constants.SPARK_HOME);
      if (sparkHome != null) {
        preparer.withEnv(Constants.Service.EXPLORE_HTTP_USER_SERVICE,
                         Collections.singletonMap(Constants.SPARK_HOME, sparkHome));
      }

      return preparer;
    }

    private Path saveCConf(CConfiguration cConf, Path file) throws IOException {
      CConfiguration copied = CConfiguration.copy(cConf);
      // Set the CFG_LOCAL_DATA_DIR to a relative path as the data directory for the container should be relative to the
      // container directory
      copied.set(Constants.CFG_LOCAL_DATA_DIR, "data");
      try (Writer writer = Files.newBufferedWriter(file, Charsets.UTF_8)) {
        copied.writeXml(writer);
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

  private Map<String, LocalizeResource> getExploreDependencies(Path tempDir,
                                                               List<String> extraClassPaths) throws IOException {
    // Collect the set of jar files in the master process classloader
    final Set<File> masterJars = new HashSet<>();
    for (URL url : ClassLoaders.getClassLoaderURLs(getClass().getClassLoader(), new HashSet<URL>())) {
      String path = url.getPath();
      // Only interested in local jar files
      if (!"file".equals(url.getProtocol()) || !path.endsWith(".jar")) {
        continue;
      }
      try {
        masterJars.add(new File(url.toURI()));
      } catch (URISyntaxException e) {
        // Should happen. Ignore the file and keep proceeding.
        LOG.warn("Failed to convert local file url to File", e);
      }
    }

    // Filter out jar files that are already in the master classpath as those will get localized by twill automatically,
    // hence no need to localize again.
    Iterable<File> exploreJars = Iterables.filter(ExploreUtils.getExploreClasspathJarFiles(), new Predicate<File>() {
      @Override
      public boolean apply(File file) {
        return !masterJars.contains(file);
      }
    });

    Map<String, LocalizeResource> resources = new HashMap<>();
    for (File exploreJar : exploreJars) {
      File targetJar = tempDir.resolve(System.currentTimeMillis() + "-" + exploreJar.getName()).toFile();
      File resultFile = ExploreServiceUtils.rewriteHiveAuthFactory(exploreJar, targetJar);
      if (resultFile == targetJar) {
        LOG.info("Rewritten HiveAuthFactory from jar file {} to jar file {}", exploreJar, resultFile);
      }

      resources.put(resultFile.getName(), new LocalizeResource(resultFile));
      extraClassPaths.add(resultFile.getName());
    }

    // Explore also depends on MR, hence adding MR jars to the classpath.
    // Depending on how the cluster is configured, we might need to localize the MR framework tgz as well.
    extraClassPaths.addAll(MapReduceContainerHelper.localizeFramework(hConf, resources));
    return resources;
  }
}
