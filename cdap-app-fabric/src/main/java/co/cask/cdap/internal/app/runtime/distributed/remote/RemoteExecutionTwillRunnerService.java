/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed.remote;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.retry.RetryableException;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.common.ssh.DefaultSSHSession;
import co.cask.cdap.common.ssh.SSHConfig;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.distributed.ProgramTwillApplication;
import co.cask.cdap.internal.app.runtime.distributed.TwillAppNames;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitor;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitorClient;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitorServerInfo;
import co.cask.cdap.internal.app.runtime.monitor.proxy.MonitorSocksProxy;
import co.cask.cdap.internal.profile.ProfileMetricService;
import co.cask.cdap.internal.provision.LocationBasedSSHKeyPair;
import co.cask.cdap.internal.provision.ProvisioningService;
import co.cask.cdap.logging.remote.RemoteExecutionLogProcessor;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.Node;
import co.cask.cdap.runtime.spi.ssh.SSHKeyPair;
import co.cask.cdap.runtime.spi.ssh.SSHSession;
import co.cask.cdap.security.tools.KeyStores;
import co.cask.common.http.HttpRequestConfig;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.SingleRunnableApplication;
import org.apache.twill.internal.io.BasicLocationCache;
import org.apache.twill.internal.io.LocationCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A {@link TwillRunnerService} implementations that uses ssh and {@link RuntimeMonitor} to launch and monitor
 * {@link TwillApplication} with single {@link TwillRunnable}.
 */
public class RemoteExecutionTwillRunnerService implements TwillRunnerService {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionTwillRunnerService.class);

  // Max out the HTTPS certificate validity.
  // We use max int of seconds to compute number of days to avoid overflow.
  private static final int CERT_VALIDITY_DAYS = (int) TimeUnit.SECONDS.toDays(Integer.MAX_VALUE);
  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final DatasetFramework datasetFramework;
  private final ConcurrentMap<ProgramRunId, RemoteExecutionTwillController> controllers;
  private final Transactional transactional;
  private final MultiThreadMessagingContext messagingContext;
  private final RemoteExecutionLogProcessor logProcessor;
  private final MetricsCollectionService metricsCollectionService;
  private final ProvisioningService provisioningService;
  private final ProgramStateWriter programStateWriter;
  private final SSHSessionManager sshSessionManager;
  private final MonitorSocksProxy monitorSocksProxy;

  private LocationCache locationCache;
  private Path cachePath;
  private ScheduledExecutorService monitorScheduler;
  
  @Inject
  RemoteExecutionTwillRunnerService(CConfiguration cConf, Configuration hConf,
                                    LocationFactory locationFactory, MessagingService messagingService,
                                    DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                    RemoteExecutionLogProcessor logProcessor,
                                    MetricsCollectionService metricsCollectionService,
                                    ProvisioningService provisioningService, ProgramStateWriter programStateWriter) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
        NamespaceId.SYSTEM, Collections.emptyMap(), null, null, messagingContext)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );

    this.controllers = new ConcurrentHashMap<>();
    this.logProcessor = logProcessor;
    this.metricsCollectionService = metricsCollectionService;
    this.provisioningService = provisioningService;
    this.programStateWriter = programStateWriter;
    this.sshSessionManager = new SSHSessionManager();
    this.monitorSocksProxy = new MonitorSocksProxy(cConf, sshSessionManager);
  }

  @Override
  public void start() {
    try {
      // Use local directory for caching generated jar files
      Path tempDir = Files.createDirectories(Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                       cConf.get(Constants.AppFabric.TEMP_DIR)).toAbsolutePath());
      cachePath = Files.createTempDirectory(tempDir, "runner.cache");
      locationCache = new BasicLocationCache(Locations.toLocation(cachePath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    monitorSocksProxy.startAndWait();
    monitorScheduler = Executors.newScheduledThreadPool(cConf.getInt(Constants.RuntimeMonitor.THREADS),
                                                        Threads.createDaemonThreadFactory("runtime-monitor-%d"));
    long startMillis = System.currentTimeMillis();
    Thread t = new Thread(() -> initializeRuntimeMonitors(startMillis), "runtime-monitor-initializer");
    t.setDaemon(true);
    t.start();
  }

  @Override
  public void stop() {
    // Stops all the runtime monitor. This won't terminate the remotely running program.
    List<ListenableFuture<Service.State>> stopFutures = new ArrayList<>();
    for (RemoteExecutionTwillController controller : controllers.values()) {
      stopFutures.add(controller.getRuntimeMonitor().stop());
    }

    // Wait for all of them to stop
    try {
      Uninterruptibles.getUninterruptibly(Futures.successfulAsList(stopFutures));
    } catch (Exception e) {
      // This shouldn't happen
      LOG.warn("Exception raised when waiting for runtime monitors to stop.", e);
    }

    try {
      try {
        monitorSocksProxy.stopAndWait();
      } catch (Exception e) {
        LOG.warn("Exception raised when stopping runtime monitor socks proxy", e);
      }
      sshSessionManager.close();

      if (cachePath != null) {
        DirUtils.deleteDirectoryContents(cachePath.toFile());
      }
    } catch (IOException e) {
      LOG.warn("Exception raised during stop", e);
    } finally {
      if (monitorScheduler != null) {
        monitorScheduler.shutdownNow();
      }
    }
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
    Configuration config = new Configuration(hConf);

    // Restrict the usage to launch user program only.
    if (!(application instanceof ProgramTwillApplication)) {
      throw new IllegalArgumentException("Only instance of ProgramTwillApplication is supported");
    }

    ProgramTwillApplication programTwillApp = (ProgramTwillApplication) application;
    ProgramRunId programRunId = programTwillApp.getProgramRunId();
    ProgramOptions programOptions = programTwillApp.getProgramOptions();

    persistRunningState(programRunId, programOptions);

    try {
      // Generate the KeyStores for HTTPS
      Location keysDir = getKeysDirLocation(programOptions, locationFactory);

      KeyStore serverKeyStore = KeyStores.generatedCertKeyStore(CERT_VALIDITY_DAYS, "");
      KeyStore clientKeyStore = KeyStores.generatedCertKeyStore(CERT_VALIDITY_DAYS, "");

      saveKeyStores(serverKeyStore, clientKeyStore, keysDir);

      ClusterKeyInfo clusterKeyInfo = new ClusterKeyInfo(programOptions, locationFactory);
      return new RemoteExecutionTwillPreparer(cConf, config, clusterKeyInfo.getSSHConfig(),
                                              serverKeyStore, clientKeyStore,
                                              application.configure(), programRunId, programOptions, null,
                                              locationCache, locationFactory,
                                              createControllerFactory(programRunId, programOptions, clusterKeyInfo)) {
        @Override
        public TwillController start(long timeout, TimeUnit timeoutUnit) {
          try {
            return super.start(timeout, timeoutUnit);
          } catch (Exception e) {
            deleteRunningState(programRunId);
            throw e;
          }
        }
      };
    } catch (Exception e) {
      deleteRunningState(programRunId);
      throw Throwables.propagate(e);
    }
  }

  @Nullable
  @Override
  public TwillController lookup(String applicationName, RunId runId) {
    ProgramId programId = TwillAppNames.fromTwillAppName(applicationName, false);
    if (programId == null) {
      return null;
    }
    return controllers.get(programId.run(runId));
  }

  @Override
  public Iterable<TwillController> lookup(String applicationName) {
    ProgramId programId = TwillAppNames.fromTwillAppName(applicationName, false);
    if (programId == null) {
      return Collections.emptyList();
    }

    return controllers.entrySet().stream()
      .filter(entry -> programId.equals(entry.getKey().getParent()))
      .map(Map.Entry::getValue)
      .map(TwillController.class::cast)
      ::iterator;
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    // Groups the controllers by the ProgramId, then transform it to an Iterator of LiveInfo
    return controllers.entrySet().stream()
      .collect(Collectors.groupingBy(e -> e.getKey().getParent(),
                                     Collectors.mapping(e -> TwillController.class.cast(e.getValue()),
                                                        Collectors.toList())))
      .entrySet().stream()
      .map(e -> new LiveInfo() {
        @Override
        public String getApplicationName() {
          return TwillAppNames.toTwillAppName(e.getKey());
        }

        @Override
        public Iterable<TwillController> getControllers() {
          return e.getValue();
        }
      })
      .map(LiveInfo.class::cast)
      ::iterator;
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater updater, long initialDelay,
                                               long delay, TimeUnit unit) {
    // This method is deprecated and not used in CDAP
    throw new UnsupportedOperationException("The scheduleSecureStoreUpdate method is deprecated, " +
                                              "use setSecureStoreRenewer instead");
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer, long initialDelay,
                                           long delay, long retryDelay, TimeUnit unit) {
    return () -> { };
  }

  /**
   * Creates a new instance of {@link RemoteExecutionTwillControllerFactory} for a given run.
   */
  private RemoteExecutionTwillControllerFactory createControllerFactory(ProgramRunId programRunId,
                                                                        ProgramOptions programOptions,
                                                                        ClusterKeyInfo clusterKeyInfo) {
    return () -> controllers.computeIfAbsent(programRunId, key -> {
      SSHConfig sshConfig = clusterKeyInfo.getSSHConfig();

      MonitorServerAddressSupplier serverAddressSupplier = new MonitorServerAddressSupplier(programRunId, sshConfig);

      // Creates a runtime monitor
      RuntimeMonitorClient runtimeMonitorClient = new RuntimeMonitorClient(
        HttpRequestConfig.DEFAULT, clusterKeyInfo.getClientKeyStore(),
        KeyStores.createTrustStore(clusterKeyInfo.getServerKeyStore()),
        serverAddressSupplier, new Proxy(Proxy.Type.SOCKS, monitorSocksProxy.getBindAddress())
      );

      RemoteProcessController remoteProcessController = new SSHRemoteProcessController(key, programOptions,
                                                                                       sshConfig, provisioningService);
      ProfileMetricService profileMetricsService = createProfileMetricsService(key, programOptions,
                                                                               clusterKeyInfo.getCluster());
      RuntimeMonitor runtimeMonitor = new RuntimeMonitor(key, cConf, runtimeMonitorClient,
                                                         datasetFramework, transactional,
                                                         messagingContext, monitorScheduler, logProcessor,
                                                         profileMetricsService,
                                                         remoteProcessController, programStateWriter);
      RemoteExecutionTwillController controller = new RemoteExecutionTwillController(
        RunIds.fromString(key.getRun()), runtimeMonitor);

      // When the program completed, remove the controller from the map.
      // Also remove the ssh config from the session manager so that it can't be used again.
      controller.onTerminated(() -> {
        controllers.remove(key, controller);
        serverAddressSupplier.close();
      }, Threads.SAME_THREAD_EXECUTOR);

      return controller;
    });
  }

  /**
   * Creates a {@link ProfileMetricService} for the profile being used for the given program run.
   */
  private ProfileMetricService createProfileMetricsService(ProgramRunId programRunId,
                                                           ProgramOptions programOptions, Cluster cluster) {
    Optional<ProfileId> profile = SystemArguments.getProfileIdFromArgs(programRunId.getNamespaceId(),
                                                                       programOptions.getArguments().asMap());
    ProfileId profileId = profile.orElseThrow(
      () -> new IllegalStateException("Missing profile information for program run " + programRunId));

    return new ProfileMetricService(metricsCollectionService, programRunId, profileId,
                                    cluster.getNodes().size(), monitorScheduler);
  }

  /**
   * Saves the given {@link KeyStore} under the given directory.
   */
  private void saveKeyStores(KeyStore serverKeyStore, KeyStore clientKeyStore, Location keysDir) {
    Map<String, KeyStore> keyStores = ImmutableMap.of(
      Constants.RuntimeMonitor.SERVER_KEYSTORE, serverKeyStore,
      Constants.RuntimeMonitor.CLIENT_KEYSTORE, clientKeyStore
    );

    for (Map.Entry<String, KeyStore> entry : keyStores.entrySet()) {
      try (OutputStream os = keysDir.append(entry.getKey()).getOutputStream("600")) {
        entry.getValue().store(os, "".toCharArray());
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Initialize {@link RuntimeMonitor} to re-monitor programs that are already running when this service starts.
   *
   * @param startMillis time in milliseconds that this service starts. It is used such that it will only monitor
   *                    program runs launched before starting of this service.
   */
  private void initializeRuntimeMonitors(long startMillis) {
    int limit = cConf.getInt(Constants.RuntimeMonitor.INIT_BATCH_SIZE);
    AtomicReference<ProgramRunId> lastProgramRunId = new AtomicReference<>();
    RetryStrategy retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.runtime.monitor.");

    boolean completed = false;

    try {
      while (!completed) {
        // Scan the dataset and update the controllers map. Retry on all exception.
        completed = Retries.callWithRetries(() -> Transactionals.execute(transactional, context -> {
          RemoteRuntimeDataset dataset = RemoteRuntimeDataset.create(context, datasetFramework);
          List<Map.Entry<ProgramRunId, ProgramOptions>> scanResult = dataset.scan(limit, lastProgramRunId.get());
          for (Map.Entry<ProgramRunId, ProgramOptions> entry : scanResult) {
            ProgramRunId programRunId = entry.getKey();
            ProgramOptions programOptions = entry.getValue();

            lastProgramRunId.set(programRunId);

            if (RunIds.getTime(programRunId.getRun(), TimeUnit.MILLISECONDS) > startMillis) {
              continue;
            }

            ClusterKeyInfo clusterKeyInfo = new ClusterKeyInfo(programOptions, locationFactory);
            RemoteExecutionTwillController controller = createControllerFactory(programRunId, programOptions,
                                                                                clusterKeyInfo).create();
            controller.getRuntimeMonitor().start();
          }

          return scanResult.isEmpty();
        }, RetryableException.class), retryStrategy, e -> true);
      }
    } catch (Exception e) {
      LOG.error("Failed to load runtime dataset", e);
    }
  }

  /**
   * Persists running state to the {@link RemoteRuntimeDataset}.
   */
  private void persistRunningState(ProgramRunId programRunId, ProgramOptions programOptions) {
    // Only retry for a max of 5 seconds. If it still failed to persist, it will fail to launch the program.
    RetryStrategy retryStrategy = RetryStrategies.timeLimit(5L, TimeUnit.SECONDS,
                                                            RetryStrategies.exponentialDelay(100L, 1000L,
                                                                                             TimeUnit.MILLISECONDS));
    Retries.runWithRetries(() -> Transactionals.execute(transactional, context -> {
      RemoteRuntimeDataset dataset = RemoteRuntimeDataset.create(context, datasetFramework);
      dataset.write(programRunId, programOptions);
    }, RetryableException.class), retryStrategy);
  }

  /**
   * Deletes the running state from the {@link RemoteRuntimeDataset}.
   */
  private void deleteRunningState(ProgramRunId programRunId) {
    try {
      // No need to retry. If deletion failed, the state will be left in the dataset until next time CDAP restarts,
      // the state transition logic will have it cleared.
      // The deletion is only needed when successfully write the states to dataset, but failed to run the launch script
      // remote, which should be rare.
      Transactionals.execute(transactional, context -> {
        RemoteRuntimeDataset dataset = RemoteRuntimeDataset.create(context, datasetFramework);
        dataset.delete(programRunId);
      });
    } catch (Exception e) {
      LOG.warn("Failed to delete execution state for program run {}", programRunId, e);
    }
  }

  /**
   * Static helper method to return the secure keys location from the given {@link ProgramOptions}.
   */
  private static Location getKeysDirLocation(ProgramOptions programOptions, LocationFactory locationFactory) {
    return locationFactory.create(
      GSON.fromJson(programOptions.getArguments().getOption(ProgramOptionConstants.SECURE_KEYS_DIR), URI.class));
  }

  /**
   * Private class to implement a {@link Supplier} of {@link InetSocketAddress} to provide the runtime monitor server
   * address to {@link RuntimeMonitorClient}. After it gets the address of the server, it register
   * the address to {@link SSHSessionManager} to allow the {@link MonitorSocksProxy} using the {@link SSHSession}
   * associated with it. When {@link #close()} is invoked, it remove the server address
   * from the {@link SSHSessionManager} to prevent further usage.
   */
  private final class MonitorServerAddressSupplier implements Supplier<InetSocketAddress>, AutoCloseable {

    private final ProgramRunId programRunId;
    private final SSHConfig sshConfig;

    private volatile InetSocketAddress address;
    private boolean closed;
  
    private MonitorServerAddressSupplier(ProgramRunId programRunId, SSHConfig sshConfig) {
      this.programRunId = programRunId;
      this.sshConfig = sshConfig;
    }

    @Override
    public InetSocketAddress get() {
      InetSocketAddress address = this.address;
      if (address != null) {
        return address;
      }
      synchronized (this) {
        if (closed) {
          return null;
        }

        address = this.address;
        if (address != null) {
          return address;
        }

        try (SSHSession session = new DefaultSSHSession(sshConfig)) {
          // Try to read the port file on the remote host, for up to 3 seconds.
          RetryStrategy strategy = RetryStrategies.timeLimit(3, TimeUnit.SECONDS,
                                                             RetryStrategies.fixDelay(200, TimeUnit.MILLISECONDS));
          int port = Retries.callWithRetries(() -> {
            String json = session.executeAndWait("cat " + programRunId.getRun() + "/"
                                                   + cConf.get(Constants.RuntimeMonitor.SERVER_INFO_FILE));
            RuntimeMonitorServerInfo info = GSON.fromJson(json, RuntimeMonitorServerInfo.class);
            if (info == null) {
              throw new IOException("Cannot find the port for the runtime monitor server");
            }
            return info.getPort();
          }, strategy, IOException.class::isInstance);

          this.address = address = new InetSocketAddress(InetAddress.getByName(sshConfig.getHost()), port);

          // Once acquired the server address, add it to the SSHSessionManager so that the proxy server can use it
          sshSessionManager.addSSHConfig(address, sshConfig);
          return address;
        } catch (Exception e) {
          return null;
        }
      }
    }

    @Override
    public synchronized void close() {
      closed = true;
      InetSocketAddress address = this.address;
      if (address != null) {
        sshSessionManager.removeSSHConfig(address);
      }
    }
  }

  /**
   * A private class to hold secure key information for a program runtime.
   */
  private static final class ClusterKeyInfo {
    private final Cluster cluster;
    private final SSHConfig sshConfig;
    private final KeyStore serverKeyStore;
    private final KeyStore clientKeyStore;

    ClusterKeyInfo(ProgramOptions programOptions,
                   LocationFactory locationFactory) throws IOException, GeneralSecurityException {
      Arguments systemArgs = programOptions.getArguments();
      Location keysDir = getKeysDirLocation(programOptions, locationFactory);

      this.cluster = GSON.fromJson(systemArgs.getOption(ProgramOptionConstants.CLUSTER), Cluster.class);
      this.sshConfig = createSSHConfig(cluster, keysDir);
      this.serverKeyStore = KeyStores.load(keysDir.append(Constants.RuntimeMonitor.SERVER_KEYSTORE), () -> "");
      this.clientKeyStore = KeyStores.load(keysDir.append(Constants.RuntimeMonitor.CLIENT_KEYSTORE), () -> "");
    }

    Cluster getCluster() {
      return cluster;
    }

    SSHConfig getSSHConfig() {
      return sshConfig;
    }

    KeyStore getServerKeyStore() {
      return serverKeyStore;
    }

    KeyStore getClientKeyStore() {
      return clientKeyStore;
    }

    /**
     * Creates a {@link SSHKeyPair} by loading keys from the given location and {@link Cluster} properties.
     */
    private SSHKeyPair createSSHKeyPair(Location keysDir, Cluster cluster) {
      String sshUser = cluster.getProperties().get(Constants.RuntimeMonitor.SSH_USER);
      if (sshUser == null) {
        throw new IllegalStateException("Missing SSH user");
      }
      try {
        return new LocationBasedSSHKeyPair(keysDir, sshUser);
      } catch (IOException e) {
        throw new RuntimeException("Failed to create SSHKeyPair from location " + keysDir, e);
      }
    }

    /**
     * Creates a {@link SSHConfig} for ssh to the master node of the given {@link Cluster}.
     *
     * @param cluster the cluster information
     * @param keysDir the {@link Location} that contains the ssh keys
     * @return a {@link SSHConfig}
     */
    private SSHConfig createSSHConfig(Cluster cluster, Location keysDir) {
      // Loads the SSH keys
      SSHKeyPair sshKeyPair = createSSHKeyPair(keysDir, cluster);

      Node masterNode = cluster.getNodes().stream()
        .filter(node -> node.getType() == Node.Type.MASTER)
        .findFirst().orElseThrow(
          () -> new IllegalArgumentException("Missing master node information for the cluster " + cluster.getName()));

      // Creates and return the twill preparer
      return SSHConfig.builder(masterNode.getIpAddress())
        .setUser(sshKeyPair.getPublicKey().getUser())
        .setPrivateKeySupplier(sshKeyPair.getPrivateKeySupplier())
        .build();
    }
  }
}
