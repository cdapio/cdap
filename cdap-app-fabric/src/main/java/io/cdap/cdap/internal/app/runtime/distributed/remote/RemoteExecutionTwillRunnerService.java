/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.security.KeyStores;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.ssh.SSHConfig;
import io.cdap.cdap.common.twill.TwillAppNames;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.distributed.ProgramTwillApplication;
import io.cdap.cdap.internal.app.runtime.monitor.RemoteExecutionLogProcessor;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitor;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitorClient;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitorServerInfo;
import io.cdap.cdap.internal.app.runtime.monitor.ServiceSocksProxyInfo;
import io.cdap.cdap.internal.app.runtime.monitor.proxy.MonitorSocksProxy;
import io.cdap.cdap.internal.app.runtime.monitor.proxy.ServiceSocksProxy;
import io.cdap.cdap.internal.app.runtime.monitor.proxy.ServiceSocksProxyAuthenticator;
import io.cdap.cdap.internal.profile.ProfileMetricService;
import io.cdap.cdap.internal.provision.LocationBasedSSHKeyPair;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.Node;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import io.cdap.cdap.runtime.spi.ssh.SSHSession;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.common.http.HttpRequestConfig;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.SingleRunnableApplication;
import org.apache.twill.internal.io.BasicLocationCache;
import org.apache.twill.internal.io.LocationCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A {@link TwillRunnerService} implementations that uses ssh and {@link RuntimeMonitor} to launch and monitor
 * {@link TwillApplication} with single {@link TwillRunnable}.
 */
public class RemoteExecutionTwillRunnerService implements TwillRunnerService {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionTwillRunnerService.class);
  private static final Logger WARN_LOG = Loggers.sampling(
    LOG, LogSamplers.all(LogSamplers.skipFirstN(20), LogSamplers.limitRate(TimeUnit.SECONDS.toMillis(30))));

  // Max out the HTTPS certificate validity.
  // We use max int of seconds to compute number of days to avoid overflow.
  private static final int CERT_VALIDITY_DAYS = (int) TimeUnit.SECONDS.toDays(Integer.MAX_VALUE);
  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final Map<ProgramRunId, RemoteExecutionTwillController> controllers;
  private final Lock controllersLock;
  private final MultiThreadMessagingContext messagingContext;
  private final RemoteExecutionLogProcessor logProcessor;
  private final MetricsCollectionService metricsCollectionService;
  private final ProvisioningService provisioningService;
  private final ProgramStateWriter programStateWriter;
  private final SSHSessionManager sshSessionManager;
  private final MonitorSocksProxy monitorSocksProxy;
  private final ServiceSocksProxy serviceSocksProxy;
  private final RuntimeServiceSocksProxyAuthenticator serviceSocksProxyAuthenticator;
  private final TransactionRunner transactionRunner;

  private LocationCache locationCache;
  private Path cachePath;
  private ExecutorService startupTaskExecutor;
  private ScheduledExecutorService monitorScheduler;
  
  @Inject
  RemoteExecutionTwillRunnerService(CConfiguration cConf, Configuration hConf,
                                    DiscoveryServiceClient discoveryServiceClient,
                                    LocationFactory locationFactory, MessagingService messagingService,
                                    RemoteExecutionLogProcessor logProcessor,
                                    MetricsCollectionService metricsCollectionService,
                                    ProvisioningService provisioningService, ProgramStateWriter programStateWriter,
                                    TransactionRunner transactionRunner) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.transactionRunner = transactionRunner;
    this.controllers = new ConcurrentHashMap<>();
    this.controllersLock = new ReentrantLock();
    this.logProcessor = logProcessor;
    this.metricsCollectionService = metricsCollectionService;
    this.provisioningService = provisioningService;
    this.programStateWriter = programStateWriter;
    this.sshSessionManager = new SSHSessionManager();
    this.monitorSocksProxy = new MonitorSocksProxy(cConf, sshSessionManager);
    this.serviceSocksProxyAuthenticator = new RuntimeServiceSocksProxyAuthenticator();
    this.serviceSocksProxy = new ServiceSocksProxy(discoveryServiceClient, serviceSocksProxyAuthenticator);
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
    serviceSocksProxy.startAndWait();

    startupTaskExecutor = Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("runtime-startup-%d"));
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
        serviceSocksProxy.stopAndWait();
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
      if (startupTaskExecutor != null) {
        startupTaskExecutor.shutdownNow();
      }
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

      ClusterKeyInfo clusterKeyInfo = new ClusterKeyInfo(cConf, programOptions, locationFactory);
      return new RemoteExecutionTwillPreparer(cConf, config, clusterKeyInfo.getSSHConfig(),
                                              serverKeyStore, clientKeyStore,
                                              application.configure(), programRunId, programOptions, null,
                                              locationCache, locationFactory,
                                              new ControllerFactory(programRunId, programOptions, clusterKeyInfo)) {
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
        completed = Retries.callWithRetries(() -> TransactionRunners.run(transactionRunner, context -> {
          RemoteRuntimeTable dataset = RemoteRuntimeTable.create(context);
          List<Map.Entry<ProgramRunId, ProgramOptions>> scanResult = dataset.scan(limit, lastProgramRunId.get());
          for (Map.Entry<ProgramRunId, ProgramOptions> entry : scanResult) {
            ProgramRunId programRunId = entry.getKey();
            ProgramOptions programOptions = entry.getValue();

            lastProgramRunId.set(programRunId);

            if (RunIds.getTime(programRunId.getRun(), TimeUnit.MILLISECONDS) > startMillis) {
              continue;
            }

            ClusterKeyInfo clusterKeyInfo = new ClusterKeyInfo(cConf, programOptions, locationFactory);
            // Creates a controller via the controller factory.
            // Since there is no startup start needed, the timeout is arbitrarily short
            new ControllerFactory(programRunId, programOptions, clusterKeyInfo).create(null, 5, TimeUnit.SECONDS);
          }

          return scanResult.isEmpty();
        }, RetryableException.class), retryStrategy, e -> true);
      }
    } catch (Exception e) {
      LOG.error("Failed to load runtime dataset", e);
    }
  }

  /**
   * Persists running state to the {@link RemoteRuntimeTable}.
   */
  private void persistRunningState(ProgramRunId programRunId, ProgramOptions programOptions) {
    // Only retry for a max of 5 seconds. If it still failed to persist, it will fail to launch the program.
    RetryStrategy retryStrategy = RetryStrategies.timeLimit(5L, TimeUnit.SECONDS,
                                                            RetryStrategies.exponentialDelay(100L, 1000L,
                                                                                             TimeUnit.MILLISECONDS));
    Retries.runWithRetries(() -> TransactionRunners.run(transactionRunner, context -> {
      RemoteRuntimeTable dataset = RemoteRuntimeTable.create(context);
      dataset.write(programRunId, programOptions);
    }, RetryableException.class), retryStrategy);
  }

  /**
   * Deletes the running state from the {@link RemoteRuntimeTable}.
   */
  private void deleteRunningState(ProgramRunId programRunId) {
    try {
      // No need to retry. If deletion failed, the state will be left in the dataset until next time CDAP restarts,
      // the state transition logic will have it cleared.
      // The deletion is only needed when successfully write the states to dataset, but failed to run the launch script
      // remote, which should be rare.
      TransactionRunners.run(transactionRunner, context -> {
        RemoteRuntimeTable dataset = RemoteRuntimeTable.create(context);
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
   * Implementation of {@link RemoteExecutionTwillControllerFactory}.
   */
  private final class ControllerFactory implements RemoteExecutionTwillControllerFactory {

    private final ProgramRunId programRunId;
    private final ProgramOptions programOptions;
    private final ClusterKeyInfo clusterKeyInfo;

    ControllerFactory(ProgramRunId programRunId, ProgramOptions programOptions, ClusterKeyInfo clusterKeyInfo) {
      this.programRunId = programRunId;
      this.programOptions = programOptions;
      this.clusterKeyInfo = clusterKeyInfo;
    }

    @Override
    public RemoteExecutionTwillController create(@Nullable Callable<Void> startupTask,
                                                 long timeout, TimeUnit timeoutUnit) {
      // Make sure we don't run the startup task and create controller if there is already one existed.
      controllersLock.lock();
      try {
        RemoteExecutionTwillController controller = controllers.get(programRunId);
        if (controller != null) {
          return controller;
        }

        CompletableFuture<Void> startupTaskCompletion = new CompletableFuture<>();

        // Execute the startup task if provided
        if (startupTask != null) {
          Future<?> startupTaskFuture = startupTaskExecutor.submit(() -> {
            Map<String, String> systemArgs = programOptions.getArguments().asMap();
            LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(programRunId, systemArgs);
            Cancellable restoreContext = LoggingContextAccessor.setLoggingContext(loggingContext);
            try {
              startupTaskCompletion.complete(startupTask.call());
            } catch (Throwable t) {
              startupTaskCompletion.completeExceptionally(t);
            } finally {
              restoreContext.cancel();
            }
          });

          // Schedule the timeout check and cancel the startup task if timeout reached.
          // This is a quick task, hence just piggy back on the monitor scheduler to do so.
          monitorScheduler.schedule(() -> {
            if (!startupTaskFuture.isDone()) {
              startupTaskFuture.cancel(true);
              startupTaskCompletion.completeExceptionally(
                new TimeoutException("Starting of program run " + programRunId + " takes longer then "
                                       + timeout + " " + timeoutUnit.name().toLowerCase()));
            }
          }, timeout, timeoutUnit);

          // If the startup task failed, publish failure state and delete the program running state
          startupTaskCompletion.whenComplete((res, throwable) -> {
            if (throwable == null) {
              LOG.debug("Startup task completed for program run {}", programRunId);
            } else {
              LOG.error("Fail to start program run {}", programRunId, throwable);
              deleteRunningState(programRunId);
              programStateWriter.error(programRunId, throwable);
            }
          });
        } else {
          // Otherwise, complete the startup task immediately
          startupTaskCompletion.complete(null);
        }

        controller = createController(startupTaskCompletion);
        controllers.put(programRunId, controller);
        return controller;
      } finally {
        controllersLock.unlock();
      }
    }

    /**
     * Creates a new instance of {@link RemoteExecutionTwillController}.
     */
    private RemoteExecutionTwillController createController(CompletableFuture<Void> startupTaskCompletion) {
      // Create a new controller
      SSHConfig sshConfig = clusterKeyInfo.getSSHConfig();
      LOG.info("Creating controller for program run {} with SSH config {}", programRunId, sshConfig);

      Cancellable removeSSHConfig = sshSessionManager.addSSHConfig(programRunId, clusterKeyInfo.getSSHConfig(),
                                                                   new ProgramRunSSHSessionConsumer(programRunId));

      // Allow the remote runtime to use the service proxy
      serviceSocksProxyAuthenticator.add(clusterKeyInfo.getServerKeyStoreHash());

      MonitorServerAddressSupplier serverAddressSupplier = new MonitorServerAddressSupplier(programRunId);

      // Creates a runtime monitor
      RuntimeMonitorClient runtimeMonitorClient = new RuntimeMonitorClient(
        HttpRequestConfig.DEFAULT, clusterKeyInfo.getClientKeyStore(),
        KeyStores.createTrustStore(clusterKeyInfo.getServerKeyStore()),
        serverAddressSupplier, new Proxy(Proxy.Type.SOCKS, monitorSocksProxy.getBindAddress())
      );

      RemoteProcessController processController = new SSHRemoteProcessController(programRunId, programOptions,
                                                                                 sshConfig, provisioningService);
      ProfileMetricService profileMetricsService = createProfileMetricsService(programRunId, programOptions,
                                                                               clusterKeyInfo.getCluster());
      RuntimeMonitor runtimeMonitor = new RuntimeMonitor(programRunId, cConf, runtimeMonitorClient,
                                                         messagingContext, monitorScheduler, logProcessor,
                                                         processController, programStateWriter,
                                                         transactionRunner, profileMetricsService);
      RemoteExecutionTwillController controller = new RemoteExecutionTwillController(
        RunIds.fromString(programRunId.getRun()), runtimeMonitor);

      // When the program completed, remove the controller from the map.
      // Also remove the ssh config from the session manager so that it can't be used again.
      controller.onTerminated(() -> {
        LOG.info("Controller completed for program run {} with SSH config {}", programRunId, sshConfig);
        serviceSocksProxyAuthenticator.remove(clusterKeyInfo.getServerKeyStoreHash());
        controllers.remove(programRunId, controller);
        serverAddressSupplier.close();
        removeSSHConfig.cancel();
      }, Threads.SAME_THREAD_EXECUTOR);

      controller.start(startupTaskCompletion);
      return controller;
    }
  }

  /**
   * A {@link Consumer} of {@link SSHSession} for new ssh session created for each program run.
   */
  private final class ProgramRunSSHSessionConsumer implements Consumer<SSHSession> {

    private final ProgramRunId programRunId;

    private ProgramRunSSHSessionConsumer(ProgramRunId programRunId) {
      this.programRunId = programRunId;
    }

    @Override
    public void accept(SSHSession session) {
      try {
        // Creates a new remote port forwarding from the session.
        // We don't need to care about closing the forwarding as it will last until the session get closed,
        // in which the remote port forwarding will be closed automatically
        int remotePort = session.createRemotePortForward(0,
                                                         serviceSocksProxy.getBindAddress().getPort()).getRemotePort();

        LOG.debug("Service SOCKS proxy started on port {} for program run {}", remotePort, programRunId);
        ServiceSocksProxyInfo info = new ServiceSocksProxyInfo(remotePort);

        // Upload the service socks proxy information to the remote runtime
        String targetPath = session.executeAndWait("echo `pwd`/" + programRunId.getRun()).trim();
        byte[] content = GSON.toJson(info).getBytes(StandardCharsets.UTF_8);
        session.copy(new ByteArrayInputStream(content),
                     targetPath, Constants.RuntimeMonitor.SERVICE_PROXY_FILE, content.length, 0600, null, null);
        LOG.debug("Service proxy file uploaded to remote runtime for program run {}", programRunId);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
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

    private volatile InetSocketAddress address;
    private boolean closed;
  
    private MonitorServerAddressSupplier(ProgramRunId programRunId) {
      this.programRunId = programRunId;
    }

    @Nullable
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

        SSHSession session = sshSessionManager.getSession(programRunId);
        try {
          // Try to read the port file on the remote host, for up to 3 seconds.
          RetryStrategy strategy = RetryStrategies.timeLimit(3, TimeUnit.SECONDS,
                                                             RetryStrategies.fixDelay(200, TimeUnit.MILLISECONDS));
          RuntimeMonitorServerInfo serverInfo = Retries.callWithRetries(() -> {
            String json = session.executeAndWait("cat " + programRunId.getRun() + "/"
                                                   + cConf.get(Constants.RuntimeMonitor.SERVER_INFO_FILE));
            RuntimeMonitorServerInfo info = GSON.fromJson(json, RuntimeMonitorServerInfo.class);
            if (info == null) {
              throw new IOException("Cannot find the port for the runtime monitor server");
            }
            // This is for backward compatibility case. It could happen when CDAP is upgraded to monitor a
            // remote runtime that was started with older CDAP version.
            // For older CDAP, we assume the runtime monitor server binds to 127.0.0.1.
            if (info.getHostAddress() == null) {
              info = new RuntimeMonitorServerInfo(new InetSocketAddress("127.0.0.1", info.getPort()));
            }

            return info;
          }, strategy, IOException.class::isInstance);

          this.address = address = new InetSocketAddress(session.getAddress().getAddress(), serverInfo.getPort());

          // Once acquired the server address, add it to the SSHSessionManager so that the proxy server can use it
          sshSessionManager.addRuntimeServer(programRunId, address.getAddress(), serverInfo);

          LOG.debug("Remote runtime server for program run {} is running at {}", programRunId, address);
          return address;
        } catch (Exception e) {
          WARN_LOG.warn("Failed to create SSH session for program run {} monitoring. Will be retried.", programRunId);
          return null;
        }
      }
    }

    @Override
    public synchronized void close() {
      closed = true;
      InetSocketAddress address = this.address;
      if (address != null) {
        sshSessionManager.removeRuntimeServer(programRunId, address.getAddress());
      }
    }
  }

  /**
   * A private class to hold secure key information for a program runtime.
   */
  private static final class ClusterKeyInfo {

    private final CConfiguration cConf;
    private final Cluster cluster;
    private final SSHConfig sshConfig;
    private final KeyStore serverKeyStore;
    private final KeyStore clientKeyStore;
    private final String serverKeyStoreHash;

    ClusterKeyInfo(CConfiguration cConf, ProgramOptions programOptions,
                   LocationFactory locationFactory) throws IOException, GeneralSecurityException {
      Arguments systemArgs = programOptions.getArguments();
      Location keysDir = getKeysDirLocation(programOptions, locationFactory);

      this.cConf = cConf;
      this.cluster = GSON.fromJson(systemArgs.getOption(ProgramOptionConstants.CLUSTER), Cluster.class);
      this.sshConfig = createSSHConfig(cluster, keysDir);
      this.serverKeyStore = KeyStores.load(keysDir.append(Constants.RuntimeMonitor.SERVER_KEYSTORE), () -> "");
      this.clientKeyStore = KeyStores.load(keysDir.append(Constants.RuntimeMonitor.CLIENT_KEYSTORE), () -> "");
      this.serverKeyStoreHash = KeyStores.hash(serverKeyStore);
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

    String getServerKeyStoreHash() {
      return serverKeyStoreHash;
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
        .setProxyAddress(Networks.getAddress(cConf, Constants.NETWORK_PROXY_ADDRESS))
        .setUser(sshKeyPair.getPublicKey().getUser())
        .setPrivateKeySupplier(sshKeyPair.getPrivateKeySupplier())
        .build();
    }
  }

  /**
   * A {@link ServiceSocksProxyAuthenticator} that authenticates based on a known set of usernames and passwords.
   */
  private static final class RuntimeServiceSocksProxyAuthenticator implements ServiceSocksProxyAuthenticator {

    private final Set<String> allowed = Collections.newSetFromMap(new ConcurrentHashMap<>());

    void add(String password) {
      allowed.add(password);
    }

    void remove(String password) {
      allowed.remove(password);
    }

    @Override
    public boolean authenticate(String username, String password) {
      return Objects.equals(username, password) && allowed.contains(password);
    }
  }
}
