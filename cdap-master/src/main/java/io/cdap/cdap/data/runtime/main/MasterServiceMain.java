/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.data.runtime.main;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedMap;
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
import com.google.inject.Key;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.MonitorHandlerModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.guice.RuntimeServerModule;
import io.cdap.cdap.app.guice.TwillModule;
import io.cdap.cdap.app.store.ServiceStore;
import io.cdap.cdap.common.MasterUtils;
import io.cdap.cdap.common.app.MainClassLoader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.FileContextProvider;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.io.URLConnections;
import io.cdap.cdap.common.logging.LoggerLogHandler;
import io.cdap.cdap.common.runtime.DaemonMain;
import io.cdap.cdap.common.service.RetryOnStartFailureService;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.common.twill.HadoopClassExcluder;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.zookeeper.election.LeaderElectionInfoService;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.metadata.writer.DefaultMetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.MessagingMetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.util.hbase.ConfigurationReader;
import io.cdap.cdap.data2.util.hbase.ConfigurationWriter;
import io.cdap.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtilFactory;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.explore.service.ExploreServiceUtils;
import io.cdap.cdap.hive.ExploreUtils;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeServer;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.guice.KafkaLogAppenderModule;
import io.cdap.cdap.master.startup.ServiceResourceKeys;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.metrics.guice.MetricsStoreModule;
import io.cdap.cdap.operations.OperationalStatsService;
import io.cdap.cdap.operations.guice.OperationalStatsModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.TokenSecureStoreRenewer;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizerInstantiator;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.store.SecureStoreService;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutor;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.Configs;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.internal.zookeeper.ReentrantDistributedLock;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.yarn.YarnSecureStore;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;

/**
 * Driver class for starting all master services.
 */
public class MasterServiceMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(MasterServiceMain.class);

  private static final long MAX_BACKOFF_TIME_MS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
  private static final long SUCCESSFUL_RUN_DURATION_MS = TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES);

  // Maximum time to try looking up the existing twill application
  private static final long LOOKUP_ATTEMPT_TIMEOUT_MS = 2000;

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final ZKClientService zkClient;
  private final Lock shutdownLock;
  private final LeaderElection leaderElection;
  private final LeaderElectionInfoService electionInfoService;
  private final LogAppenderInitializer logAppenderInitializer;
  private final MasterLeaderElectionHandler electionHandler;

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
      LOG.warn("Failed to create CDAP system ClassLoader. AuthEnforce annotation will not be rewritten.");
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

  @SuppressWarnings("WeakerAccess")
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
    this.shutdownLock = new ReentrantDistributedLock(zkClient, "/lock/" + Constants.Service.MASTER_SERVICES);

    String electionPath = "/election/" + Constants.Service.MASTER_SERVICES;
    this.electionInfoService = new LeaderElectionInfoService(zkClient, electionPath);
    this.electionHandler = new MasterLeaderElectionHandler(cConf, hConf, zkClient, electionInfoService);
    this.leaderElection = new LeaderElection(zkClient, electionPath, electionHandler);

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
    resetShutdownTime();
    cleanupTempDir();
    checkExploreRequirements();
  }

  @Override
  public void start() throws Exception {
    logAppenderInitializer.initialize();
    resetShutdownTime();
    createDirectory("twill");
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
    electionInfoService.startAndWait();
    leaderElection.startAndWait();
  }

  @Override
  public void stop() {
    LOG.info("Stopping {}", Constants.Service.MASTER_SERVICES);
    stopped = true;

    // When shutting down, we want to have the leader process not to stop the YARN application if there are
    // followers running (CDAP-8565).
    // During shutdown, we use a lock to ensure the leader and followers have a consistent view
    // about the leader-election participants. This is mainly to deal with the case when multiple master
    // processes get shutting down simultaneously. When multiple masters are stopping at about the same time,
    // we want to perform a rolling style shutdown sequence such that the last master getting stopped
    // would become the last leader and stopping the YARN application as well.

    // The first lock block below guarantees that there is always an active leader until the last leader withdrawn

    // If the process currently is a leader, the following sequence of events will happen
    // 1. Acquire the shutdown lock
    // 2. Become follower with the electionHandler.follower() method get invoked
    // 3. Leader election stopped with the leader-election ephemeral node removed from ZK
    // 4. Release shutdown lock with the lock ephemeral node removed from ZK

    // For any other master process, before it was able to acquire the
    // lock (after step 4 above is performed by the original leader),
    // it must have received the leader-election children change event, hence the transition to leader must
    // be triggered for the next follower in line. Here is the sequence of events:
    // 5. ElectionHandler.leader() get triggered and lock acquired (the order doesn't matter)
    // 6. LeaderElection.stopAndWait gets called, and it will block until ElectionHandler.leader() completed
    // 7. ElectionHandler.leader() completed with the control of the yarn app
    // 8. Then step 2-4 above will happen in the new leader process

    shutdownLock.lock();
    try {
      // if leader election failed to start, its listener will stop the master.
      // In that case, we don't want to try stopping it again, as it will log confusing exceptions
      if (leaderElection.isRunning()) {
        stopQuietly(leaderElection);
      }
    } finally {
      shutdownLock.unlock();
    }

    // This second lock block is to guarantee the following conditions after the lock is acquired
    // 1. All other master processes that are still running must received the leader-election children change event
    // 2. For leader-election participants that this process can see by fetching from ZK while holding the lock,
    //    those must NOT have gone through the first lock block above.

    // Because of the conditions above, inside the electionHandler.postStop() method, if there are
    // non-zero participants, we don't need to stop the yarn app if this process was the leader.
    shutdownLock.lock();
    try {
      electionHandler.postStop();
    } finally {
      shutdownLock.unlock();
    }
    stopQuietly(electionInfoService);
    stopQuietly(zkClient);
    Closeables.closeQuietly(logAppenderInitializer);
  }

  @Override
  public void destroy() {
    saveShutdownTime(System.currentTimeMillis());
  }

  /**
   * CDAP-6644 for secure impersonation to work,
   * we want other users to be able to write to the "path" directory,
   * currently only cdap.user has read-write permissions
   * while other users can only read the "{hdfs.namespace}/{path}" dir,
   * we want to let others to be able to write to "path" directory, until we have a better solution.
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
        fileContext.mkdir(fPath, permission, true);
      }
    } catch (FileAlreadyExistsException e) {
      // should not happen as we create only if dir exists
    } catch (AccessControlException | ParentNotDirectoryException | FileNotFoundException e) {
      // just log the exception
      LOG.error("Exception while trying to create directory at {}", path, e);
    } catch (IOException e) {
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

  /**
   * Gets an instance of the given {@link Service} class from the given {@link Injector}, start the service and
   * returns it.
   */
  private static <T extends Service> T getAndStart(Injector injector, Class<T> cls) {
    T service = injector.getInstance(cls);
    LOG.debug("Starting service in master {}", service);
    service.startAndWait();
    LOG.info("Service {} started in master", service);
    return service;
  }

  /**
   * Stops a guava {@link Service}. No exception will be thrown even stopping failed.
   */
  private static void stopQuietly(@Nullable Service service) {
    try {
      if (service != null) {
        LOG.debug("Stopping service in master: {}", service);
        service.stopAndWait();
        LOG.info("Service {} stopped in master", service);
      }
    } catch (Exception e) {
      LOG.warn("Exception when stopping service {}", service, e);
    }
  }

  /**
   * Stops a {@link TwillRunnerService}. No exception will be thrown even stopping failed.
   */
  private static void stopQuietly(@Nullable TwillRunnerService service) {
    try {
      if (service != null) {
        LOG.debug("Stopping twill runner service");
        service.stop();
        LOG.info("Twill runner service stopped in master");
      }
    } catch (Exception e) {
      LOG.warn("Exception when stopping service {}", service, e);
    }
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
      ExploreServiceUtils.checkHiveSupport(cConf);
    }
  }

  /**
   * Performs Kerberos login if security is enabled.
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
    try (HBaseDDLExecutor ddlExecutor = new HBaseDDLExecutorFactory(cConf, hConf).get()) {
      ddlExecutor.createNamespaceIfNotExists(tableUtil.getHBaseNamespace(NamespaceId.SYSTEM));
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
      new ConfigurationWriter(hConf, cConf).write(ConfigurationReader.Type.DEFAULT, cConf);
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }

  /**
   * The replication Status tool will use CDAP shutdown time to determine last CDAP related writes to HBase.
   */
  private void saveShutdownTime(long timestamp) {
    File shutdownTimeFile = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                     Constants.Replication.CDAP_SHUTDOWN_TIME_FILENAME).getAbsoluteFile();
    if (!DirUtils.mkdirs(shutdownTimeFile.getParentFile())) {
      LOG.error("Failed to create parent directory for writing shutdown time {} to file {}",
                timestamp, shutdownTimeFile);
      return;
    }

    try (BufferedWriter bw = new BufferedWriter(new FileWriter(shutdownTimeFile))) {
      //Write shutdown time
      bw.write(Long.toString(timestamp));
      LOG.info("Saved CDAP shutdown time {} at file {}", timestamp, shutdownTimeFile);
    } catch (IOException e) {
      LOG.error("Failed to save CDAP shutdown time {} to file {}", timestamp, shutdownTimeFile, e);
    }
  }

  private void resetShutdownTime() {
    File shutdownTimeFile = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                     Constants.Replication.CDAP_SHUTDOWN_TIME_FILENAME).getAbsoluteFile();
    if (shutdownTimeFile.exists() && !shutdownTimeFile.delete()) {
      LOG.error("Failed to reset shutdown time file {}", shutdownTimeFile);
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
      new KafkaLogAppenderModule()
    );
  }

  /**
   * Creates a guice {@link Injector} to be used when this master service becomes leader.
   */
  @VisibleForTesting
  static Injector createLeaderInjector(CConfiguration cConf, Configuration hConf,
                                       final ZKClientService zkClientService,
                                       final LeaderElectionInfoService electionInfoService) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new AbstractModule() {
        @Override
        protected void configure() {
          // Instead of using ZKClientModule that will create new instance of ZKClient, we create instance
          // binding to reuse the same ZKClient used for leader election
          bind(ZKClient.class).toInstance(zkClientService);
          bind(ZKClientService.class).toInstance(zkClientService);
          bind(LeaderElectionInfoService.class).toInstance(electionInfoService);
        }
      },
      new KafkaLogAppenderModule(),
      new DFSLocationModule(),
      new IOModule(),
      new ZKDiscoveryModule(),
      new KafkaClientModule(),
      new DataSetServiceModules().getDistributedModules(),
      new DataFabricModules("cdap.master").getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new MetricsStoreModule(),
      new MessagingClientModule(),
      new ExploreClientModule(),
      new AuditModule(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      new TwillModule(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new MonitorHandlerModule(true),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new SecureStoreServerModule(),
      new RuntimeServerModule(),
      new OperationalStatsModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // TODO (CDAP-14677): find a better way to inject metadata publisher
          bind(MetadataPublisher.class).to(MessagingMetadataPublisher.class);
          bind(MetadataServiceClient.class).to(DefaultMetadataServiceClient.class);
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
    private final LeaderElectionInfoService electionInfoService;
    private final AtomicReference<TwillController> controller;
    private final List<Service> services;

    private Injector injector;
    private Cancellable secureStoreUpdateCancellable;
    // Executor for re-running master twill app if it gets terminated.
    private ScheduledExecutorService executor;
    private AuthorizerInstantiator authorizerInstantiator;
    private TwillRunnerService twillRunner;
    private TwillRunnerService remoteExecutionTwillRunner;
    private ExploreClient exploreClient;
    private LogAppenderInitializer logAppenderInitializer;
    private MetadataStorage metadataStorage;


    private MasterLeaderElectionHandler(CConfiguration cConf, Configuration hConf, ZKClientService zkClient,
                                        LeaderElectionInfoService electionInfoService) {
      this.cConf = cConf;
      this.hConf = hConf;
      this.zkClient = zkClient;
      this.electionInfoService = electionInfoService;
      this.controller = new AtomicReference<>();
      this.services = new ArrayList<>();
    }

    @Override
    public void leader() {
      LOG.info("Became leader for master services");

      // We need to create a new injector each time becoming leader so that new instances of singleton Services
      // will be created
      injector = createLeaderInjector(cConf, hConf, zkClient, electionInfoService);

      logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
      logAppenderInitializer.initialize();

      if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
        exploreClient = injector.getInstance(ExploreClient.class);
      }

      try {
        // Define all StructuredTable before starting any services that need StructuredTable
        StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class),
                                        injector.getInstance(StructuredTableRegistry.class));
      } catch (IOException | TableAlreadyExistsException e) {
        throw new RuntimeException("Unable to create the system tables.", e);
      }
      metadataStorage = injector.getInstance(MetadataStorage.class);
      try {
        metadataStorage.createIndex();
      } catch (IOException e) {
        throw new RuntimeException("Unable to create the metadata tables.", e);
      }

      authorizerInstantiator = injector.getInstance(AuthorizerInstantiator.class);
      services.add(injector.getInstance(KafkaClientService.class));
      services.add(injector.getInstance(MetricsCollectionService.class));
      services.add(injector.getInstance(OperationalStatsService.class));
      ServiceStore serviceStore = getAndStart(injector, ServiceStore.class);
      services.add(serviceStore);
      services.add(injector.getInstance(SecureStoreService.class));
      services.add(injector.getInstance(RuntimeServer.class));

      twillRunner = injector.getInstance(TwillRunnerService.class);
      twillRunner.start();

      remoteExecutionTwillRunner = injector.getInstance(Key.get(TwillRunnerService.class,
                                                                Constants.AppFabric.RemoteExecution.class));
      remoteExecutionTwillRunner.start();

      TokenSecureStoreRenewer secureStoreRenewer = injector.getInstance(TokenSecureStoreRenewer.class);

      // Schedule secure store update.
      if (User.isHBaseSecurityEnabled(hConf) || UserGroupInformation.isSecurityEnabled()) {
        secureStoreUpdateCancellable = twillRunner.setSecureStoreRenewer(secureStoreRenewer, 30000L,
                                                                         secureStoreRenewer.getUpdateInterval(),
                                                                         30000L,
                                                                         TimeUnit.MILLISECONDS);
      }

      // Create app-fabric and dataset services
      services.add(new RetryOnStartFailureService(() -> injector.getInstance(DatasetService.class),
                                                  RetryStrategies.exponentialDelay(200, 5000, TimeUnit.MILLISECONDS)));
      services.add(injector.getInstance(AppFabricServer.class));

      executor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("master-runner"));

      // Start monitoring twill application
      monitorTwillApplication(executor, 0, controller, twillRunner, serviceStore, secureStoreRenewer);

      // Starts all services.
      for (Service service : services) {
        if (service.isRunning()) {
          // Some services are already started
          continue;
        }
        LOG.info("Starting service in master: {}", service);
        try {
          service.startAndWait();
        } catch (Throwable t) {
          // shut down the executor and stop the twill app,
          // then throw an exception to cause the leader election service to stop
          // leader election's listener will then shutdown the master
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

    private void stop(boolean stopRequested) {
      // Shutdown the retry executor so that no re-run of the twill app will be attempted
      if (executor != null) {
        executor.shutdownNow();
      }
      // Stop secure store update
      if (secureStoreUpdateCancellable != null) {
        secureStoreUpdateCancellable.cancel();
      }

      // If it is a transition from leader to follower due to ZK event, stop the twillRunner.
      // Otherwise it will be stopped in the postStop() method
      if (!stopRequested) {
        controller.set(null);
        stopQuietly(remoteExecutionTwillRunner);
        stopQuietly(twillRunner);
      }
      // Stop local services last since DatasetService is running locally
      // and remote services need it to preserve states.
      for (Service service : Lists.reverse(services)) {
        stopQuietly(service);
      }
      services.clear();
      Closeables.closeQuietly(metadataStorage);
      Closeables.closeQuietly(authorizerInstantiator);
      Closeables.closeQuietly(exploreClient);
      Closeables.closeQuietly(logAppenderInitializer);
    }

    /**
     * Stops the twill application if necessary. If this process was not the leader, this method will just return.
     * If this process was the leader, it will check if there is any other participants currently running. If there is,
     * it won't stop the twill application; otherwise it will.
     */
    void postStop() {
      TwillController twillController = controller.get();
      try {
        // The twill controller won't be null if this was the leader.
        if (twillController == null) {
          return;
        }

        SortedMap<Integer, LeaderElectionInfoService.Participant> participants = ImmutableSortedMap.of();
        try {
          participants = electionInfoService.fetchCurrentParticipants();

          // Expected the size of participants to be non-zero
          // If the current process is the only participant, the size of the map would be one, because
          // LeaderElection only remove the node after the follower() method is returned.
          // If there are other processes as participants, the size would be > 1.
          // In case if the participants map is empty, also terminate the twill ap to be safe
        } catch (Exception e) {
          // Calling e.toString() explicitly, otherwise it will print the stack trace, which we don't need in this case
          LOG.warn("Unable to detect if there are other leader election partitions due to {}", e.toString());
        }

        // If failed to get the participant information, the safest thing to do is to shutdown the Twill app
        // to make sure nothing left behind
        if (participants.isEmpty()) {
          LOG.info("No other master process detected, stopping master twill application");
          twillController.terminate();
          try {
            twillController.awaitTerminated();
            LOG.info("Master twill application terminated successfully");
          } catch (ExecutionException e) {
            LOG.info("Master twill application terminated with exception", e.getCause());
          }
        } else {
          LOG.info("Keep the twill application running to fail-over to a master in the set {}",
                   participants.values());
        }
      } finally {
        stopQuietly(twillRunner);
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
                                         final AtomicReference<TwillController> serviceController,
                                         final TwillRunnerService twillRunner, final ServiceStore serviceStore,
                                         final TokenSecureStoreRenewer secureStoreRenewer) {
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
          controller = startTwillApplication(twillRunner, serviceStore, secureStoreRenewer);
        } catch (Exception e) {
          LOG.error("Failed to start master twill application", e);
          throw e;
        }
        startTime = System.currentTimeMillis();
      }

      // Attache the log handler to pipe container logs to master log
      if (cConf.getBoolean(Constants.COLLECT_CONTAINER_LOGS)) {
        controller.addLogHandler(new LoggerLogHandler(LOG));
      }

      // Monitor the application
      serviceController.set(controller);
      final TwillController finalController = controller;
      controller.onTerminated(new Runnable() {
        @Override
        public void run() {
          if (executor.isShutdown()) {
            return;
          }
          Thread.interrupted();
          try {
            finalController.terminate().get();
            LOG.warn("{} was terminated", Constants.Service.MASTER_SERVICES);
          } catch (InterruptedException e) {
            // Should never happen
          } catch (ExecutionException e) {
            LOG.error("{} was terminated due to failure", Constants.Service.MASTER_SERVICES, e.getCause());
          }

          LOG.warn("Restarting {} with back-off", Constants.Service.MASTER_SERVICES);
          backoffRun();
        }

        private void backoffRun() {
          if (System.currentTimeMillis() - startTime > SUCCESSFUL_RUN_DURATION_MS) {
            // Restart immediately
            executor.execute(new Runnable() {
              @Override
              public void run() {
                monitorTwillApplication(executor, 0, serviceController, twillRunner, serviceStore, secureStoreRenewer);
              }
            });
            return;
          }

          long nextRunTime = Math.min(500 * (long) Math.pow(2, failures + 1), MAX_BACKOFF_TIME_MS);
          executor.schedule(new Runnable() {
            @Override
            public void run() {
              monitorTwillApplication(executor, failures + 1, serviceController,
                                      twillRunner, serviceStore, secureStoreRenewer);
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
                                                  TokenSecureStoreRenewer secureStoreRenewer) {
      try {
        // Create a temp dir for the run to hold temporary files created to run the application
        Path tempPath = Files.createDirectories(Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                          cConf.get(Constants.AppFabric.TEMP_DIR)).toAbsolutePath());
        final Path runDir = Files.createTempDirectory(tempPath, "master");
        try {
          Path logbackFile = saveLogbackConf(runDir.resolve("logback.xml"));
          MasterTwillApplication masterTwillApp = new MasterTwillApplication(cConf,
                                                                             getServiceInstances(serviceStore, cConf));
          List<String> extraClassPath = masterTwillApp.prepareLocalizeResource(runDir, hConf);
          TwillPreparer preparer = twillRunner.prepare(masterTwillApp);

          Map<String, String> twillConfigs = new HashMap<>();
          if (!cConf.getBoolean(Constants.COLLECT_CONTAINER_LOGS)) {
            twillConfigs.put(Configs.Keys.LOG_COLLECTION_ENABLED, Boolean.toString(false));
          }
          twillConfigs.put(Configs.Keys.YARN_ATTEMPT_FAILURES_VALIDITY_INTERVAL,
                           cConf.get(Constants.AppFabric.YARN_ATTEMPT_FAILURES_VALIDITY_INTERVAL));
          preparer.withConfiguration(twillConfigs);

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
            preparer.addSecureStore(YarnSecureStore.create(secureStoreRenewer.createCredentials()));
          }

          // add hadoop classpath to application classpath and exclude hadoop classes from bundle jar.
          List<String> yarnAppClassPath = Arrays.asList(
            hConf.getTrimmedStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));

          preparer.withApplicationClassPaths(yarnAppClassPath).withBundlerClassAcceptor(new HadoopClassExcluder());

          // Setup extra classpath. Currently twill doesn't support different classpath per runnable,
          // hence we just set it for all containers. The actual jars are localized via the MasterTwillApplication,
          // and having missing jars as specified in the classpath is ok.
          boolean yarnFirst = cConf.getBoolean(Constants.Explore.CONTAINER_YARN_APP_CLASSPATH_FIRST);
          if (yarnFirst) {
            // It's ok to have yarn application classpath set here even it can affect non-explore container,
            // since we anyway have yarn application classpath in the "withApplicationClassPaths".
            preparer = preparer.withClassPaths(Iterables.concat(yarnAppClassPath, extraClassPath));
          } else {
            preparer = preparer.withClassPaths(extraClassPath);
          }

          // Add explore dependencies
          if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
            prepareExploreContainer(preparer);
          }

          // Set the container to use MasterServiceMainClassLoader for class rewriting
          preparer.setClassLoader(MasterServiceMainClassLoader.class.getName());

          // Set per service configurations
          prepareServiceConfig(preparer, masterTwillApp.getRunnableConfigPrefixes());

          TwillController controller = preparer.start(cConf.getLong(Constants.AppFabric.PROGRAM_MAX_START_SECONDS),
                                                      TimeUnit.SECONDS);

          // Add a listener to delete temp files when application started/terminated.
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
     * Reads the master service instance count configuration.
     */
    private Map<String, Integer> getServiceInstances(ServiceStore serviceStore, CConfiguration cConf) {
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
     * Sets the configurations for each service.
     */
    private TwillPreparer prepareServiceConfig(TwillPreparer preparer, Map<String, String> runnableConfigPrefixes) {
      for (Map.Entry<String, String> entry : runnableConfigPrefixes.entrySet()) {
        String runnableName = entry.getKey();
        String configPrefix = entry.getValue() + "twill.";

        Map<String, String> config = new HashMap<>();
        for (Map.Entry<String, String> confEntry : cConf) {
          if (confEntry.getKey().startsWith(configPrefix)) {
            // Get the key that twill recognize, which is prefixed with "twill."
            String key = confEntry.getKey().substring(entry.getValue().length());
            // Special case for jvm options.
            if ("twill.jvm.opts".equals(key)) {
              preparer.setJVMOptions(runnableName, confEntry.getValue());
            } else {
              config.put(key, confEntry.getValue());
            }
          }
        }

        if (!config.isEmpty()) {
          preparer.withConfiguration(runnableName, config);
        }
      }

      return preparer;
    }


    /**
     * Prepare the specs of the twill application for the Explore twill runnable.
     * Add jars needed by the Explore module in the classpath of the containers, and
     * add conf files (hive_site.xml, etc) as resources available for the Explore twill
     * runnable.
     */
    private TwillPreparer prepareExploreContainer(TwillPreparer preparer) throws IOException {
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
}
