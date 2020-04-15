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

import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.ssh.SSHConfig;
import io.cdap.cdap.common.twill.TwillAppNames;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.distributed.ProgramTwillApplication;
import io.cdap.cdap.internal.app.runtime.monitor.proxy.ServiceSocksProxy;
import io.cdap.cdap.internal.app.runtime.monitor.proxy.ServiceSocksProxyAuthenticator;
import io.cdap.cdap.internal.app.services.ProgramCompletionNotifier;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.provision.LocationBasedSSHKeyPair;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.Node;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.apache.twill.internal.SingleRunnableApplication;
import org.apache.twill.internal.io.BasicLocationCache;
import org.apache.twill.internal.io.LocationCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A {@link TwillRunnerService} implementations that uses ssh to launch and monitor
 * {@link TwillApplication} with a single {@link TwillRunnable}.
 */
public class RemoteExecutionTwillRunnerService implements TwillRunnerService, ProgramCompletionNotifier {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionTwillRunnerService.class);
  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final ProvisioningService provisioningService;
  private final ProgramStateWriter programStateWriter;
  private final TransactionRunner txRunner;
  private final ServiceSocksProxy serviceSocksProxy;
  private final RuntimeServiceSocksProxyAuthenticator serviceSocksProxyAuthenticator;
  private final Map<ProgramRunId, RemoteExecutionTwillController> controllers;
  private final Lock controllersLock;

  private LocationCache locationCache;
  private Path cachePath;
  private ScheduledExecutorService scheduler;

  @Inject
  RemoteExecutionTwillRunnerService(CConfiguration cConf, Configuration hConf,
                                    DiscoveryServiceClient discoveryServiceClient,
                                    LocationFactory locationFactory,
                                    ProvisioningService provisioningService,
                                    ProgramStateWriter programStateWriter,
                                    TransactionRunner transactionRunner) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.provisioningService = provisioningService;
    this.programStateWriter = programStateWriter;
    this.txRunner = transactionRunner;
    this.controllers = new ConcurrentHashMap<>();
    this.controllersLock = new ReentrantLock();
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

    scheduler = Executors.newScheduledThreadPool(cConf.getInt(Constants.RuntimeMonitor.THREADS),
                                                 Threads.createDaemonThreadFactory("runtime-scheduler-%d"));
    long startMillis = System.currentTimeMillis();
    scheduler.execute(new Runnable() {
      @Override
      public void run() {
        // Wait till the provisioning service is fully started before initializing controllers.
        if (!provisioningService.isRunning()) {
          scheduler.schedule(this, 1, TimeUnit.SECONDS);
          return;
        }
        initializeControllers(startMillis);
      }
    });
  }

  @Override
  public void stop() {
    try {
      if (EnumSet.of(Service.State.STARTING, Service.State.RUNNING).contains(serviceSocksProxy.state())) {
        serviceSocksProxy.stopAndWait();
      }
    } catch (Exception e) {
      LOG.warn("Exception raised when stopping runtime monitor socks proxy", e);
    }
    // Release the resource. This won't terminate the controllers.
    controllers.values().forEach(RemoteExecutionTwillController::release);

    try {
      if (cachePath != null) {
        DirUtils.deleteDirectoryContents(cachePath.toFile());
      }
    } catch (IOException e) {
      LOG.warn("Exception raised during stop", e);
    } finally {
      if (scheduler != null) {
        scheduler.shutdownNow();
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
    CConfiguration cConfCopy = CConfiguration.copy(cConf);
    Configuration hConfCopy = new Configuration(hConf);

    // Restrict the usage to launch user program only.
    if (!(application instanceof ProgramTwillApplication)) {
      throw new IllegalArgumentException("Only instance of ProgramTwillApplication is supported");
    }

    ProgramTwillApplication programTwillApp = (ProgramTwillApplication) application;
    ProgramRunId programRunId = programTwillApp.getProgramRunId();
    ProgramOptions programOpts = programTwillApp.getProgramOptions();

    return createPreparer(cConfCopy, hConfCopy, programRunId, programOpts, application.configure(), locationCache,
                          new ControllerFactory(programRunId, programOpts));
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
                                     Collectors.mapping(e -> (TwillController) e.getValue(),
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

  @SuppressWarnings("deprecation")
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

  @Override
  public void onProgramCompleted(ProgramRunId programRunId, ProgramRunStatus completionStatus) {
    RemoteExecutionTwillController controller = controllers.remove(programRunId);
    if (controller != null) {
      controller.complete();
    }
  }

  private TwillPreparer createPreparer(CConfiguration cConf, Configuration hConf, ProgramRunId programRunId,
                                       ProgramOptions programOpts, TwillSpecification twillSpec,
                                       LocationCache locationCache, TwillControllerFactory controllerFactory) {

    Location serviceProxySecretLocation = null;
    if (SystemArguments.getRuntimeMonitorType(cConf, programOpts) == RemoteMonitorType.SSH) {
      serviceProxySecretLocation = generateAndSaveServiceProxySecret(programRunId,
                                                                     getKeysDirLocation(programOpts, locationFactory));
    }

    RuntimeJobManager jobManager = provisioningService.getRuntimeJobManager(programRunId, programOpts).orElse(null);
    // Use RuntimeJobManager to launch the remote process if it is supported
    if (jobManager != null) {
      return new RuntimeJobTwillPreparer(cConf, hConf, twillSpec, programRunId, programOpts,
                                         serviceProxySecretLocation,
                                         locationCache, locationFactory,
                                         controllerFactory,
                                         () -> provisioningService.getRuntimeJobManager(programRunId, programOpts)
                                           .orElseThrow(IllegalStateException::new));
    }

    // Use SSH if there is no RuntimeJobManager
    ClusterKeyInfo clusterKeyInfo = new ClusterKeyInfo(cConf, programOpts, locationFactory);
    return new RemoteExecutionTwillPreparer(cConf, hConf, clusterKeyInfo.getSSHConfig(),
                                            serviceProxySecretLocation,
                                            twillSpec, programRunId, programOpts,
                                            locationCache, locationFactory, controllerFactory);
  }

  /**
   * Returns the port that the service socks proxy is listening on. It will start the socks proxy if it is not running.
   */
  private int getServiceSocksProxyPort() {
    if (serviceSocksProxy.state() == Service.State.NEW) {
      // It's ok to have multiple threads calling start if the proxy is not running.
      serviceSocksProxy.startAndWait();
    }
    return serviceSocksProxy.getBindAddress().getPort();
  }

  /**
   * Generates a secret for the remote runtime to use to authenticate itself to the service socks proxy.
   */
  private Location generateAndSaveServiceProxySecret(ProgramRunId programRunId, Location keysDir) {
    try {
      SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
      byte[] salt = new byte[20];   // Same size as a SHA1 hash
      secureRandom.nextBytes(salt);

      String secret = Hashing.sha1().newHasher()
        .putBytes(salt)
        .putString(programRunId.getRun())
        .hash().toString();

      Location location = keysDir.append(Constants.RuntimeMonitor.SERVICE_PROXY_PASSWORD_FILE);
      try (OutputStream os = location.getOutputStream()) {
        os.write(secret.getBytes(StandardCharsets.UTF_8));
      }
      return location;
    } catch (NoSuchAlgorithmException | IOException e) {
      throw new RuntimeException("Failed to generate service proxy secret for " + programRunId, e);
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
   * Initialize {@link RemoteExecutionTwillController}s to programs that are already running when this service starts.
   *
   * @param startMillis time in milliseconds that this service starts. It is used such that it will only monitor
   *                    program runs launched before starting of this service.
   */
  private void initializeControllers(long startMillis) {
    int limit = cConf.getInt(Constants.RuntimeMonitor.INIT_BATCH_SIZE);
    RetryStrategy retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.runtime.monitor.");
    AtomicReference<AppMetadataStore.Cursor> cursorRef = new AtomicReference<>();
    AtomicInteger count = new AtomicInteger();
    AtomicBoolean completed = new AtomicBoolean();

    try {
      while (!completed.get()) {
        Retries.runWithRetries(() -> TransactionRunners.run(txRunner, context -> {
          AppMetadataStore store = AppMetadataStore.create(context);
          completed.set(true);
          store.scanActiveRuns(cursorRef.get(), limit, (cursor, runRecordDetail) -> {
            if (runRecordDetail.getStartTs() > startMillis) {
              return;
            }
            completed.set(false);
            try {
              if (createControllerIfNeeded(runRecordDetail)) {
                count.incrementAndGet();
              }
            } catch (Exception e) {
              // This can happen if the key files are gone. We won't be able to monitor it any more,
              // hence treat it as failed.
              ProgramRunId programRunId = runRecordDetail.getProgramRunId();
              LOG.warn("Failed to create controller from run record for {}. Marking program as failed.",
                       programRunId, e);
              programStateWriter.error(programRunId, e);
            }
            cursorRef.set(cursor);
          });
        }, RetryableException.class), retryStrategy, e -> true);
      }
      LOG.debug("Initialization completed with {} runs added", count.get());
    } catch (Exception e) {
      LOG.error("Failed to load runtime dataset", e);
    }
  }

  /**
   * Creates a {@link TwillController} based on the given {@link RunRecordDetail} if it should be monitored by this
   * service.
   */
  private boolean createControllerIfNeeded(RunRecordDetail runRecordDetail) {
    Map<String, String> systemArgs = runRecordDetail.getSystemArgs();
    try {
      ClusterMode clusterMode = ClusterMode.valueOf(systemArgs.getOrDefault(ProgramOptionConstants.CLUSTER_MODE,
                                                                            ClusterMode.ON_PREMISE.name()));
      if (clusterMode != ClusterMode.ISOLATED) {
        LOG.debug("Ignore run {} of non supported cluster mode {}", runRecordDetail.getProgramRunId(), clusterMode);
        return false;
      }
    } catch (IllegalArgumentException e) {
      LOG.warn("Ignore run record with an invalid cluster mode", e);
      return false;
    }

    ProgramOptions programOpts = new SimpleProgramOptions(runRecordDetail.getProgramRunId().getParent(),
                                                          new BasicArguments(runRecordDetail.getSystemArgs()),
                                                          new BasicArguments(runRecordDetail.getUserArgs()));
    // Creates a controller via the controller factory.
    // Since there is no startup start needed, the timeout is arbitrarily short
    new ControllerFactory(runRecordDetail.getProgramRunId(), programOpts).create(null, 5, TimeUnit.SECONDS);
    return true;
  }

  private final class ControllerFactory implements TwillControllerFactory {

    private final ProgramRunId programRunId;
    private final ProgramOptions programOpts;

    private ControllerFactory(ProgramRunId programRunId, ProgramOptions programOpts) {
      this.programRunId = programRunId;
      this.programOpts = programOpts;
    }

    @Override
    public TwillController create(@Nullable Callable<Void> startupTask, long timeout, TimeUnit timeoutUnit) {
      // Make sure we don't run the startup task and create controller if there is already one existed.
      controllersLock.lock();
      try {
        RemoteExecutionTwillController controller = controllers.get(programRunId);
        if (controller != null) {
          return controller;
        }

        CompletableFuture<Void> startupTaskCompletion = new CompletableFuture<>();
        RemoteProcessController processController = createRemoteProcessController(programRunId, programOpts);
        try {
          controller = createController(programRunId, programOpts, processController, startupTaskCompletion);
        } catch (Exception e) {
          throw new RuntimeException("Failed to create controller for " + programRunId, e);
        }
        RemoteExecutionTwillController finalController = controller;

        // Execute the startup task if provided
        if (startupTask != null) {
          ClassLoader startupClassLoader = Optional
            .ofNullable(Thread.currentThread().getContextClassLoader())
            .orElse(getClass().getClassLoader());
          Future<?> startupTaskFuture = scheduler.submit(() -> {
            Map<String, String> systemArgs = programOpts.getArguments().asMap();
            LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(programRunId, systemArgs);
            Cancellable restoreContext = LoggingContextAccessor.setLoggingContext(loggingContext);
            ClassLoader oldCl = ClassLoaders.setContextClassLoader(startupClassLoader);
            try {
              startupTaskCompletion.complete(startupTask.call());
            } catch (Throwable t) {
              startupTaskCompletion.completeExceptionally(t);
            } finally {
              ClassLoaders.setContextClassLoader(oldCl);
              restoreContext.cancel();
            }
          });

          // Schedule the timeout check and cancel the startup task if timeout reached.
          // This is a quick task, hence just piggy back on the monitor scheduler to do so.
          scheduler.schedule(() -> {
            if (!startupTaskFuture.isDone()) {
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
              // The startup task completion can be failed in multiple scenarios.
              // It can be caused by the startup task failure.
              // It can also be due to cancellation from the controller, or a start up timeout.
              // In either case, always cancel the startup task. If the task is already completed, there is no.
              startupTaskFuture.cancel(true);
              try {
                // Attempt to force kill the remote process. If there is no such process found, it won't throw.
                processController.kill();
              } catch (Exception e) {
                LOG.warn("Force termination of remote process for {} failed", programRunId, e);
              }
              programStateWriter.error(programRunId, throwable);
            }
          });
        } else {
          // Otherwise, complete the startup task immediately
          startupTaskCompletion.complete(null);
        }

        LOG.debug("Created controller for program run {}", programRunId);
        controllers.put(programRunId, finalController);
        return finalController;
      } finally {
        controllersLock.unlock();
      }
    }

    /**
     * Creates a new instance of {@link RemoteExecutionTwillController}.
     */
    private RemoteExecutionTwillController createController(ProgramRunId programRunId, ProgramOptions programOpts,
                                                            RemoteProcessController processController,
                                                            CompletableFuture<Void> startupTaskCompletion)
      throws Exception {
      RemoteExecutionService remoteExecutionService = createRemoteExecutionService(programRunId, programOpts,
                                                                                   processController);

      // Create the controller and start the runtime monitor when the startup task completed successfully.
      RemoteExecutionTwillController controller = new RemoteExecutionTwillController(cConf, programRunId,
                                                                                     startupTaskCompletion,
                                                                                     processController,
                                                                                     scheduler, remoteExecutionService);
      startupTaskCompletion.thenAccept(o -> remoteExecutionService.start());
      return controller;
    }

    private RemoteProcessController createRemoteProcessController(ProgramRunId programRunId,
                                                                  ProgramOptions programOpts) {
      RuntimeJobManager jobManager = provisioningService.getRuntimeJobManager(programRunId, programOpts).orElse(null);
      // Use RuntimeJobManager to control the remote process if it is supported
      if (jobManager != null) {
        LOG.debug("Creating controller for program run {} with runtime job manager", programRunId);
        return new RuntimeJobRemoteProcessController(
          programRunId,
          () -> provisioningService.getRuntimeJobManager(programRunId, programOpts)
            .orElseThrow(IllegalStateException::new));
      }

      // Otherwise, default to SSH
      ClusterKeyInfo clusterKeyInfo = new ClusterKeyInfo(cConf, programOpts, locationFactory);
      SSHConfig sshConfig = clusterKeyInfo.getSSHConfig();
      LOG.debug("Creating controller for program run {} with SSH config {}", programRunId, sshConfig);

      return new SSHRemoteProcessController(programRunId, programOpts, sshConfig, provisioningService);
    }

    private RemoteExecutionService createRemoteExecutionService(ProgramRunId programRunId, ProgramOptions programOpts,
                                                                RemoteProcessController processController)
      throws IOException {
      // If monitor via URL directly, no need to run service socks proxy
      RemoteMonitorType monitorType = SystemArguments.getRuntimeMonitorType(cConf, programOpts);
      if (monitorType == RemoteMonitorType.URL) {
        LOG.debug("Monitor program run {} with direct url", programRunId);
        return new RemoteExecutionService(cConf, programRunId, scheduler, processController, programStateWriter);
      }

      // SSH monitor. The remote exeuction service will starts the service proxy
      ClusterKeyInfo clusterKeyInfo = new ClusterKeyInfo(cConf, programOpts, locationFactory);
      SSHConfig sshConfig = clusterKeyInfo.getSSHConfig();
      RemoteExecutionService remoteExecutionService = new SSHRemoteExecutionService(cConf, programRunId,
                                                                                    sshConfig,
                                                                                    getServiceSocksProxyPort(),
                                                                                    processController,
                                                                                    programStateWriter, scheduler);
      LOG.debug("Monitor program run {} with SSH config {}", programRunId, sshConfig);
      String proxySecret = clusterKeyInfo.getServerProxySecret();
      remoteExecutionService.addListener(new ServiceListenerAdapter() {
        @Override
        public void running() {
          serviceSocksProxyAuthenticator.add(proxySecret);
        }

        @Override
        public void terminated(Service.State from) {
          serviceSocksProxyAuthenticator.remove(proxySecret);
        }

        @Override
        public void failed(Service.State from, Throwable failure) {
          serviceSocksProxyAuthenticator.remove(proxySecret);
        }
      }, Threads.SAME_THREAD_EXECUTOR);
      return remoteExecutionService;
    }
  }

  /**
   * A private class to hold secure key information for a program runtime.
   */
  private static final class ClusterKeyInfo {

    private final CConfiguration cConf;
    private final Location keysDir;
    private final SSHConfig sshConfig;
    private volatile String serverProxySecret;

    ClusterKeyInfo(CConfiguration cConf, ProgramOptions programOptions, LocationFactory locationFactory) {
      Arguments systemArgs = programOptions.getArguments();

      this.cConf = cConf;
      this.keysDir = getKeysDirLocation(programOptions, locationFactory);

      Cluster cluster = GSON.fromJson(systemArgs.getOption(ProgramOptionConstants.CLUSTER), Cluster.class);
      this.sshConfig = createSSHConfig(cluster, keysDir);
    }

    SSHConfig getSSHConfig() {
      return sshConfig;
    }

    String getServerProxySecret() throws IOException {
      String secret = serverProxySecret;
      if (secret != null) {
        return secret;
      }
      try (InputStream is = keysDir.append(Constants.RuntimeMonitor.SERVICE_PROXY_PASSWORD_FILE).getInputStream()) {
        serverProxySecret = secret = new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8);
      }
      return secret;
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
