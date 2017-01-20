/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.yarn;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.Configs;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.ProcessController;
import org.apache.twill.internal.RunIds;
import org.apache.twill.internal.SingleRunnableApplication;
import org.apache.twill.internal.appmaster.ApplicationMasterLiveNodeData;
import org.apache.twill.internal.io.BasicLocationCache;
import org.apache.twill.internal.io.LocationCache;
import org.apache.twill.internal.io.NoCachingLocationCache;
import org.apache.twill.internal.utils.Dependencies;
import org.apache.twill.internal.yarn.VersionDetectYarnAppClientFactory;
import org.apache.twill.internal.yarn.YarnAppClient;
import org.apache.twill.internal.yarn.YarnApplicationReport;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An implementation of {@link org.apache.twill.api.TwillRunnerService} that runs application on a YARN cluster.
 * TODO: Copied from Twill 0.7 for CDAP-CDAP-6609.
 * TODO: Remove this after the fix is moved to Twill (TWILL-189).
 */
public final class YarnTwillRunnerService implements TwillRunnerService {

  private static final Logger LOG = LoggerFactory.getLogger(YarnTwillRunnerService.class);
  private static final int ZK_TIMEOUT = 10000;
  private static final Function<String, RunId> STRING_TO_RUN_ID = new Function<String, RunId>() {
    @Override
    public RunId apply(String input) {
      return RunIds.fromString(input);
    }
  };
  private static final Function<YarnTwillController, TwillController> CAST_CONTROLLER =
    new Function<YarnTwillController, TwillController>() {
      @Override
      public TwillController apply(YarnTwillController controller) {
        return controller;
      }
    };

  private final YarnConfiguration yarnConfig;
  private final YarnAppClient yarnAppClient;
  private final ZKClientService zkClientService;
  private final LocationFactory locationFactory;
  private final Table<String, RunId, YarnTwillController> controllers;
  private final Set<URL> twillClassPaths;
  // A Guava service to help the state transition.
  private final Service serviceDelegate;
  private LocationCache locationCache;
  private LocationCacheCleaner locationCacheCleaner;
  private ScheduledExecutorService secureStoreScheduler;

  private Iterable<LiveInfo> liveInfos;
  private Cancellable watchCancellable;

  private volatile String jvmOptions = null;

  /**
   * Creates an instance with a {@link FileContextLocationFactory} created base on the given configuration with the
   * user home directory as the location factory namespace.
   *
   * @param config Configuration of the yarn cluster
   * @param zkConnect ZooKeeper connection string
   */
  public YarnTwillRunnerService(YarnConfiguration config, String zkConnect) {
    this(config, zkConnect, createDefaultLocationFactory(config));
  }

  /**
   * Creates an instance.
   *
   * @param config Configuration of the yarn cluster
   * @param zkConnect ZooKeeper connection string
   * @param locationFactory Factory to create {@link Location} instances that are readable and writable by this service
   */
  public YarnTwillRunnerService(YarnConfiguration config, String zkConnect, LocationFactory locationFactory) {
    this.yarnConfig = config;
    this.yarnAppClient = new VersionDetectYarnAppClientFactory().create(config);
    this.locationFactory = locationFactory;
    this.zkClientService = getZKClientService(zkConnect);
    this.controllers = HashBasedTable.create();
    this.twillClassPaths = new HashSet<>();
    this.serviceDelegate = new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        YarnTwillRunnerService.this.startUp();
      }

      @Override
      protected void shutDown() throws Exception {
        YarnTwillRunnerService.this.shutDown();
      }
    };
  }

  @Override
  public void start() {
    serviceDelegate.startAndWait();
  }

  @Override
  public void stop() {
    serviceDelegate.stopAndWait();
  }

  /**
   * This methods sets the extra JVM options that will be passed to the java command line for every application
   * started through this {@link YarnTwillRunnerService} instance. It only affects applications that are started
   * after options is set.
   *
   * This is intended for advance usage. All options will be passed unchanged to the java command line. Invalid
   * options could cause application not able to start.
   *
   * @param options extra JVM options.
   */
  public void setJVMOptions(String options) {
    Preconditions.checkArgument(options != null, "JVM options cannot be null.");
    this.jvmOptions = options;
  }

  /**
   * Returns any extra JVM options that have been set.
   * @see #setJVMOptions(String)
   */
  public String getJVMOptions() {
    return jvmOptions;
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(final SecureStoreUpdater updater,
                                               long initialDelay, long delay, TimeUnit unit) {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return new Cancellable() {
        @Override
        public void cancel() {
          // No-op
        }
      };
    }

    synchronized (this) {
      if (secureStoreScheduler == null) {
        secureStoreScheduler = Executors.newSingleThreadScheduledExecutor(
          Threads.createDaemonThreadFactory("secure-store-updater"));
      }
    }

    final ScheduledFuture<?> future = secureStoreScheduler.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        // Collects all <application, runId> pairs first
        Multimap<String, RunId> liveApps = HashMultimap.create();
        synchronized (YarnTwillRunnerService.this) {
          for (Table.Cell<String, RunId, YarnTwillController> cell : controllers.cellSet()) {
            liveApps.put(cell.getRowKey(), cell.getColumnKey());
          }
        }

        // Collect all secure stores that needs to be updated.
        Table<String, RunId, SecureStore> secureStores = HashBasedTable.create();
        for (Map.Entry<String, RunId> entry : liveApps.entries()) {
          try {
            secureStores.put(entry.getKey(), entry.getValue(), updater.update(entry.getKey(), entry.getValue()));
          } catch (Throwable t) {
            LOG.warn("Exception thrown by SecureStoreUpdater {}", updater, t);
          }
        }

        // Update secure stores.
        updateSecureStores(secureStores);
      }
    }, initialDelay, delay, unit);

    return new Cancellable() {
      @Override
      public void cancel() {
        future.cancel(false);
      }
    };
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable) {
    return prepare(runnable, ResourceSpecification.BASIC);
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification) {
    return prepare(new SingleRunnableApplication(runnable, resourceSpecification));
  }

  @Override
  public TwillPreparer prepare(TwillApplication application) {
    Preconditions.checkState(serviceDelegate.isRunning(), "Service not start. Please call start() first.");
    final TwillSpecification twillSpec = application.configure();
    final String appName = twillSpec.getName();
    RunId runId = RunIds.generate();
    Location appLocation = locationFactory.create(String.format("/%s/%s", twillSpec.getName(), runId.getId()));
    LocationCache locationCache = this.locationCache;
    if (locationCache == null) {
      locationCache = new NoCachingLocationCache(appLocation);
    }

    return new YarnTwillPreparer(yarnConfig, twillSpec, runId, yarnAppClient,
                                 zkClientService.getConnectString(), appLocation, twillClassPaths, jvmOptions,
                                 locationCache, new YarnTwillControllerFactory() {
      @Override
      public YarnTwillController create(RunId runId, Iterable<LogHandler> logHandlers,
                                        Callable<ProcessController<YarnApplicationReport>> startUp,
                                        long startTimeout, TimeUnit startTimeoutUnit) {
        ZKClient zkClient = ZKClients.namespace(zkClientService, "/" + appName);
        YarnTwillController controller = listenController(new YarnTwillController(appName, runId, zkClient,
                                                                                  logHandlers, startUp,
                                                                                  startTimeout, startTimeoutUnit));
        synchronized (YarnTwillRunnerService.this) {
          Preconditions.checkArgument(!controllers.contains(appName, runId),
                                      "Application %s with runId %s is already running.", appName, runId);
          controllers.put(appName, runId, controller);
        }
        return controller;
      }
    });
  }

  @Override
  public synchronized TwillController lookup(String applicationName, final RunId runId) {
    return controllers.get(applicationName, runId);
  }

  @Override
  public Iterable<TwillController> lookup(final String applicationName) {
    return new Iterable<TwillController>() {
      @Override
      public Iterator<TwillController> iterator() {
        synchronized (YarnTwillRunnerService.this) {
          return Iterators.transform(ImmutableList.copyOf(controllers.row(applicationName).values()).iterator(),
                                     CAST_CONTROLLER);
        }
      }
    };
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    return liveInfos;
  }

  private void startUp() throws Exception {
    zkClientService.startAndWait();

    // Find all the classpaths for Twill classes. It is used for class filtering when building application jar
    // in the YarnTwillPreparer
    Dependencies.findClassDependencies(getClass().getClassLoader(), new ClassAcceptor() {
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        if (!className.startsWith("org.apache.twill.")) {
          return false;
        }
        twillClassPaths.add(classPathUrl);
        return true;
      }
    }, getClass().getName());

    // Create the root node, so that the namespace root would get created if it is missing
    // If the exception is caused by node exists, then it's ok. Otherwise propagate the exception.
    ZKOperations.ignoreError(zkClientService.create("/", null, CreateMode.PERSISTENT),
                             KeeperException.NodeExistsException.class, null).get();

    watchCancellable = watchLiveApps();
    liveInfos = createLiveInfos();

    boolean enableSecureStoreUpdate = yarnConfig.getBoolean(Configs.Keys.SECURE_STORE_UPDATE_LOCATION_ENABLED, true);
    // Schedule an updater for updating HDFS delegation tokens
    if (UserGroupInformation.isSecurityEnabled() && enableSecureStoreUpdate) {
      long renewalInterval = yarnConfig.getLong(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
                                                DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
      // Schedule it five minutes before it expires.
      long delay = renewalInterval - TimeUnit.MINUTES.toMillis(5);
      // Just to safeguard. In practice, the value shouldn't be that small, otherwise nothing could work.
      if (delay <= 0) {
        delay = (renewalInterval <= 2) ? 1 : renewalInterval / 2;
      }
      scheduleSecureStoreUpdate(new LocationSecureStoreUpdater(yarnConfig, locationFactory),
                                delay, delay, TimeUnit.MILLISECONDS);
    }

    // Optionally create a LocationCache
    String cacheDir = yarnConfig.get(Configs.Keys.LOCATION_CACHE_DIR);
    if (cacheDir != null) {
      String sessionId = Long.toString(System.currentTimeMillis());
      try {
        Location cacheBase = locationFactory.create(cacheDir);
        cacheBase.mkdirs();
        cacheBase.setPermissions("775");

        // Use a unique cache directory for each instance of this class
        Location cacheLocation = cacheBase.append(sessionId);
        cacheLocation.mkdirs();
        cacheLocation.setPermissions("775");

        locationCache = new BasicLocationCache(cacheLocation);
        locationCacheCleaner = startLocationCacheCleaner(cacheBase, sessionId);
      } catch (IOException e) {
        LOG.warn("Failed to create location cache directory. Location cache cannot be enabled.", e);
      }
    }
  }

  /**
   * Forces a cleanup of location cache based on the given time.
   */
  @VisibleForTesting
  void forceLocationCacheCleanup(long currentTime) {
    locationCacheCleaner.forceCleanup(currentTime);
  }

  private LocationCacheCleaner startLocationCacheCleaner(final Location cacheBase, final String sessionId) {
    LocationCacheCleaner cleaner = new LocationCacheCleaner(
      yarnConfig, cacheBase, sessionId, new Predicate<Location>() {
      @Override
      public boolean apply(Location location) {
        // Collects all the locations that is being used by any live applications
        Set<Location> activeLocations = new HashSet<>();
        synchronized (YarnTwillRunnerService.this) {
          for (YarnTwillController controller : controllers.values()) {
            ApplicationMasterLiveNodeData amLiveNodeData = controller.getApplicationMasterLiveNodeData();
            if (amLiveNodeData != null) {
              for (LocalFile localFile : amLiveNodeData.getLocalFiles()) {
                activeLocations.add(locationFactory.create(localFile.getURI()));
              }
            }
          }
        }

        try {
          // Always keep the launcher.jar and twill.jar from the current session as they should never change,
          // hence never expires
          activeLocations.add(cacheBase.append(sessionId).append(Constants.Files.LAUNCHER_JAR));
          activeLocations.add(cacheBase.append(sessionId).append(Constants.Files.TWILL_JAR));
        } catch (IOException e) {
          // This should not happen
          LOG.warn("Failed to construct cache location", e);
        }

        return !activeLocations.contains(location);
      }
    });
    cleaner.startAndWait();
    return cleaner;
  }

  private void shutDown() throws Exception {
    // Shutdown shouldn't stop any controllers, as stopping this client service should let the remote containers
    // running. However, this assumes that this TwillRunnerService is a long running service and you only stop it
    // when the JVM process is about to exit. Hence it is important that threads created in the controllers are
    // daemon threads.
    synchronized (this) {
      if (locationCacheCleaner != null) {
        locationCacheCleaner.stopAndWait();
      }
      if (secureStoreScheduler != null) {
        secureStoreScheduler.shutdownNow();
      }
    }
    watchCancellable.cancel();
    zkClientService.stopAndWait();
  }

  private Cancellable watchLiveApps() {
    final Map<String, Cancellable> watched = Maps.newConcurrentMap();

    final AtomicBoolean cancelled = new AtomicBoolean(false);
    // Watch child changes in the root, which gives all application names.
    final Cancellable cancellable = ZKOperations.watchChildren(zkClientService, "/",
                                                               new ZKOperations.ChildrenCallback() {
      @Override
      public void updated(NodeChildren nodeChildren) {
        if (cancelled.get()) {
          return;
        }

        Set<String> apps = ImmutableSet.copyOf(nodeChildren.getChildren());

        // For each for the application name, watch for ephemeral nodes under /instances.
        for (final String appName : apps) {
          if (watched.containsKey(appName)) {
            continue;
          }

          final String instancePath = String.format("/%s/instances", appName);
          watched.put(appName,
                      ZKOperations.watchChildren(zkClientService, instancePath, new ZKOperations.ChildrenCallback() {
            @Override
            public void updated(NodeChildren nodeChildren) {
              if (cancelled.get()) {
                return;
              }
              if (nodeChildren.getChildren().isEmpty()) {     // No more child, means no live instances
                Cancellable removed = watched.remove(appName);
                if (removed != null) {
                  removed.cancel();
                }
                return;
              }
              synchronized (YarnTwillRunnerService.this) {
                // For each of the children, which the node name is the runId,
                // fetch the application Id and construct TwillController.
                for (final RunId runId : Iterables.transform(nodeChildren.getChildren(), STRING_TO_RUN_ID)) {
                  if (controllers.contains(appName, runId)) {
                    continue;
                  }
                  updateController(appName, runId, cancelled);
                }
              }
            }
          }));
        }

        // Remove app watches for apps that are gone. Removal of controller from controllers table is done
        // in the state listener attached to the twill controller.
        for (String removeApp : Sets.difference(watched.keySet(), apps)) {
          watched.remove(removeApp).cancel();
        }
      }
    });
    return new Cancellable() {
      @Override
      public void cancel() {
        cancelled.set(true);
        cancellable.cancel();
        for (Cancellable c : watched.values()) {
          c.cancel();
        }
      }
    };
  }

  private YarnTwillController listenController(final YarnTwillController controller) {
    controller.onTerminated(new Runnable() {
      @Override
      public void run() {
        synchronized (YarnTwillRunnerService.this) {
          Iterables.removeIf(controllers.values(), new Predicate<YarnTwillController>() {
            @Override
            public boolean apply(YarnTwillController input) {
              return input == controller;
            }
          });
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return controller;
  }

  private ZKClientService getZKClientService(String zkConnect) {
    return ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(ZKClientService.Builder.of(zkConnect)
                                   .setSessionTimeout(ZK_TIMEOUT)
                                   .build(), RetryStrategies.exponentialDelay(100, 2000, TimeUnit.MILLISECONDS))));
  }

  private Iterable<LiveInfo> createLiveInfos() {
    return new Iterable<LiveInfo>() {

      @Override
      public Iterator<LiveInfo> iterator() {
        Map<String, Map<RunId, YarnTwillController>> controllerMap;
        synchronized (YarnTwillRunnerService.this) {
          controllerMap = ImmutableTable.copyOf(controllers).rowMap();
        }
        return Iterators.transform(controllerMap.entrySet().iterator(),
                                   new Function<Map.Entry<String, Map<RunId, YarnTwillController>>, LiveInfo>() {
          @Override
          public LiveInfo apply(final Map.Entry<String, Map<RunId, YarnTwillController>> entry) {
            return new LiveInfo() {
              @Override
              public String getApplicationName() {
                return entry.getKey();
              }

              @Override
              public Iterable<TwillController> getControllers() {
                return Iterables.transform(entry.getValue().values(), CAST_CONTROLLER);
              }
            };
          }
        });
      }
    };
  }

  private void updateController(final String appName, final RunId runId, final AtomicBoolean cancelled) {
    String instancePath = String.format("/%s/instances/%s", appName, runId.getId());

    // Fetch the content node.
    Futures.addCallback(zkClientService.getData(instancePath), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        if (cancelled.get()) {
          return;
        }

        ApplicationMasterLiveNodeData amLiveNodeData = ApplicationMasterLiveNodeDecoder.decode(result);
        if (amLiveNodeData == null) {
          return;
        }

        synchronized (YarnTwillRunnerService.this) {
          if (!controllers.contains(appName, runId)) {
            ZKClient zkClient = ZKClients.namespace(zkClientService, "/" + appName);
            YarnTwillController controller = listenController(
              new YarnTwillController(appName, runId, zkClient, amLiveNodeData, yarnAppClient));
            controllers.put(appName, runId, controller);
            controller.start();
          }
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.warn("Failed in fetching application instance node.", t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }


  private void updateSecureStores(Table<String, RunId, SecureStore> secureStores) {
    for (Table.Cell<String, RunId, SecureStore> cell : secureStores.cellSet()) {
      SecureStore secureStore = cell.getValue();
      if (!(secureStore instanceof YarnSecureStore)) {
        LOG.warn("Only Yarn SecureStore is supported. Ignore update for {}.", cell);
        continue;
      }
      YarnSecureStore yarnSecureStore = (YarnSecureStore) secureStore;
      if (yarnSecureStore.getStore().getAllTokens().isEmpty()) {
        // Nothing to update.
        continue;
      }

      try {
        updateCredentials(cell.getRowKey(), cell.getColumnKey(), yarnSecureStore);
        synchronized (YarnTwillRunnerService.this) {
          // Notify the application for secure store updates if it is still running.
          YarnTwillController controller = controllers.get(cell.getRowKey(), cell.getColumnKey());
          if (controller != null) {
            controller.secureStoreUpdated();
          }
        }
      } catch (Throwable t) {
        LOG.warn("Failed to update secure store for {}.", cell, t);
      }
    }
  }

  private void updateCredentials(final String application, final RunId runId,
                                 YarnSecureStore yarnSecureStore) throws IOException {
    Location credentialsLocation;
    try {
      credentialsLocation = yarnSecureStore.getUgi().doAs(new PrivilegedExceptionAction<Location>() {
        @Override
        public Location run() throws Exception {
          return locationFactory.create(String.format("/%s/%s/%s", application, runId.getId(),
                                                      Constants.Files.CREDENTIALS));
        }
      });
    } catch (Exception e) {
      // the only checked exception thrown in in the above Callable is IOException
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }

    // Try to read the old credentials.
    Credentials credentials = new Credentials();
    if (credentialsLocation.exists()) {
      try (DataInputStream is = new DataInputStream(new BufferedInputStream(credentialsLocation.getInputStream()))) {
        credentials.readTokenStorageStream(is);
      }
    }

    // Overwrite with the updates.
    credentials.addAll(yarnSecureStore.getStore());

    // Overwrite the credentials.
    Location tmpLocation = credentialsLocation.getTempFile(Constants.Files.CREDENTIALS);

    // Save the credentials store with user-only permission.
    try (DataOutputStream os = new DataOutputStream(new BufferedOutputStream(tmpLocation.getOutputStream("600")))) {
      credentials.writeTokenStorageToStream(os);
    }

    // Rename the tmp file into the credentials location
    tmpLocation.renameTo(credentialsLocation);

    LOG.debug("Secure store for {} {} saved to {}.", application, runId, credentialsLocation);
  }

  private static LocationFactory createDefaultLocationFactory(Configuration configuration) {
    try {
      FileContext fc = FileContext.getFileContext(configuration);
      return new FileContextLocationFactory(configuration, fc, fc.getHomeDirectory().toUri().getPath());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
