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

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.LoggingContextAccessor.LoggingContextRestorer;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.common.twill.TwillAppNames;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.distributed.ProgramTwillApplication;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitors;
import io.cdap.cdap.internal.app.services.ProgramCompletionNotifier;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.internal.tethering.runtime.spi.provisioner.TetheringProvisioner;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobStatus;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.SingleRunnableApplication;
import org.apache.twill.internal.io.BasicLocationCache;
import org.apache.twill.internal.io.LocationCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TwillRunnerService} implementation that uses a {@link RuntimeJobManager} and URL
 * runtime monitoring to launch and monitor a {@link TwillApplication} with a single
 * {@link TwillRunnable}.
 */
public class RemoteExecutionTwillRunnerService implements TwillRunnerService,
    ProgramCompletionNotifier {

  private static final Logger LOG = LoggerFactory.getLogger(
      RemoteExecutionTwillRunnerService.class);
  private static final Gson GSON = new Gson();
  //We can have really long jobs, so the tokens would be long-lived
  private static final long DEFAULT_EXPIRATION = TimeUnit.DAYS.toMillis(365);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final ProvisioningService provisioningService;
  private final ProgramStateWriter programStateWriter;
  private final TransactionRunner txRunner;
  private final Map<ProgramRunId, RemoteExecutionTwillController> controllers;
  private final Lock controllersLock;
  private final AccessTokenCodec accessTokenCodec;
  private final TokenManager tokenManager;

  private LocationCache locationCache;
  private Path cachePath;
  private ScheduledExecutorService scheduler;

  @Inject
  RemoteExecutionTwillRunnerService(CConfiguration cConf, Configuration hConf,
      LocationFactory locationFactory,
      ProvisioningService provisioningService,
      ProgramStateWriter programStateWriter,
      TransactionRunner transactionRunner,
      AccessTokenCodec accessTokenCodec,
      TokenManager tokenManager) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.provisioningService = provisioningService;
    this.programStateWriter = programStateWriter;
    this.txRunner = transactionRunner;
    this.accessTokenCodec = accessTokenCodec;
    this.tokenManager = tokenManager;
    this.controllers = new ConcurrentHashMap<>();
    this.controllersLock = new ReentrantLock();
  }

  @Override
  public void start() {
    doInitialize();
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

  final void doInitialize() {
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
  }

  @Override
  public void stop() {
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
  public TwillPreparer prepare(TwillRunnable runnable,
      ResourceSpecification resourceSpecification) {
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

    return createPreparer(cConfCopy, hConfCopy, programRunId, programOpts, application.configure(),
        locationCache,
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
    throw new UnsupportedOperationException("The scheduleSecureStoreUpdate method is deprecated, "
        + "use setSecureStoreRenewer instead");
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer, long initialDelay,
      long delay, long retryDelay, TimeUnit unit) {
    return () -> {
    };
  }

  @Override
  public void onProgramCompleted(ProgramRunId programRunId, ProgramRunStatus completionStatus) {
    RemoteExecutionTwillController controller;
    controllersLock.lock();
    try {
      controller = controllers.remove(programRunId);
    } finally {
      controllersLock.unlock();
    }
    if (controller != null) {
      controller.complete();
    }
  }

  private TwillPreparer createPreparer(CConfiguration cConf, Configuration hConf,
      ProgramRunId programRunId,
      ProgramOptions programOpts, TwillSpecification twillSpec,
      LocationCache locationCache, TwillControllerFactory controllerFactory) {
    RuntimeMonitors.requireMonitorUrl(cConf, programOpts);

    Map<String, Location> secretFiles = new HashMap<>();
    if (SecurityUtil.isInternalAuthEnabled(cConf)) {
      Location keysDirLocation = getKeysDirLocation(programOpts, locationFactory);
      secretFiles.put(Constants.Security.Authentication.RUNTIME_TOKEN_FILE,
          generateAndSaveRuntimeToken(programRunId, keysDirLocation));
    }

    return new RuntimeJobTwillPreparer(cConf, hConf, twillSpec, programRunId, programOpts,
        secretFiles,
        locationCache, locationFactory,
        controllerFactory,
        getRuntimeJobManagerSupplier(programRunId, programOpts));
  }

  private Supplier<RuntimeJobManager> getRuntimeJobManagerSupplier(ProgramRunId programRunId,
      ProgramOptions programOpts) {
    String provisioner = SystemArguments.getProfileProvisioner(programOpts.getArguments().asMap());
    return () -> provisioningService.getRuntimeJobManager(programRunId, programOpts)
        .orElseThrow(() -> new UnsupportedOperationException(
            String.format("Provisioner '%s' does not support RuntimeJobManager for %s",
                provisioner, programRunId)));
  }

  /**
   * Generates a runtime token to talk back from the execution cluster to CDAP instance.
   */
  private Location generateAndSaveRuntimeToken(ProgramRunId programRunId, Location keysDir) {
    try {
      long currentTimestamp = System.currentTimeMillis();
      //TODO: Use a better identity & expiration
      UserIdentity identity = new UserIdentity(Constants.Security.Authentication.RUNTIME_IDENTITY,
          UserIdentity.IdentifierType.INTERNAL, Collections.emptyList(),
          currentTimestamp, currentTimestamp + DEFAULT_EXPIRATION);
      AccessToken accessToken = tokenManager.signIdentifier(identity);
      byte[] encodedAccessToken = Base64.getEncoder().encode(accessTokenCodec.encode(accessToken));
      Location location = keysDir.append(Constants.Security.Authentication.RUNTIME_TOKEN_FILE);
      try (OutputStream os = location.getOutputStream()) {
        os.write(encodedAccessToken);
      }
      return location;
    } catch (IOException e) {
      throw new RuntimeException("Failed to generate runtime token for " + programRunId, e);
    }
  }

  /**
   * Static helper method to return the secure keys location from the given {@link ProgramOptions}.
   */
  private static Location getKeysDirLocation(ProgramOptions programOptions,
      LocationFactory locationFactory) {
    return locationFactory.create(
        GSON.fromJson(
            programOptions.getArguments().getOption(ProgramOptionConstants.SECURE_KEYS_DIR),
            URI.class));
  }

  /**
   * Initialize {@link RemoteExecutionTwillController}s to programs that are already running when
   * this service starts.
   *
   * @param startMillis time in milliseconds that this service starts. It is used such that it
   *     will only monitor program runs launched before starting of this service.
   */
  private void initializeControllers(long startMillis) {
    int limit = cConf.getInt(Constants.RuntimeMonitor.INIT_BATCH_SIZE);
    RetryStrategy retryStrategy = RetryStrategies.fromConfiguration(cConf,
        Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX);
    AtomicReference<AppMetadataStore.Cursor> cursorRef = new AtomicReference<>(
        AppMetadataStore.Cursor.EMPTY);
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
              // This can happen if runtime monitor configuration is missing or unsupported.
              // We won't be able to monitor the run any more, hence treat it as failed.
              ProgramRunId programRunId = runRecordDetail.getProgramRunId();
              LOG.warn(
                  "Failed to create controller from run record for {}. Marking program as failed.",
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
   * Creates a {@link TwillController} based on the given {@link RunRecordDetail} if it should be
   * monitored by this service.
   */
  private boolean createControllerIfNeeded(RunRecordDetail runRecordDetail) {
    // Controller only needs to be created for program runs in RUNNING or SUSPENDED state.
    // Program runs in PENDING and STARTING state will eventually start and controller will be created later.
    if (runRecordDetail.getStatus() != ProgramRunStatus.RUNNING
        && runRecordDetail.getStatus() != ProgramRunStatus.SUSPENDED) {
      LOG.debug("Skip creating controller for run {} with status {}",
          runRecordDetail.getProgramRunId(),
          runRecordDetail.getStatus());
      return false;
    }
    TwillController twillController = createTwillControllerFromRunRecord(runRecordDetail);
    return twillController != null;
  }

  public TwillController createTwillControllerFromRunRecord(RunRecordDetail runRecordDetail) {
    Map<String, String> systemArgs = runRecordDetail.getSystemArgs();
    try {
      ClusterMode clusterMode = ClusterMode.valueOf(
          systemArgs.getOrDefault(ProgramOptionConstants.CLUSTER_MODE,
              ClusterMode.ON_PREMISE.name()));
      if (clusterMode != ClusterMode.ISOLATED) {
        LOG.debug("Ignore run {} of non supported cluster mode {}",
            runRecordDetail.getProgramRunId(), clusterMode);
        return null;
      }
    } catch (IllegalArgumentException e) {
      LOG.warn("Ignore run record with an invalid cluster mode", e);
      return null;
    }

    ProgramOptions programOpts = new SimpleProgramOptions(
        runRecordDetail.getProgramRunId().getParent(),
        new BasicArguments(runRecordDetail.getSystemArgs()),
        new BasicArguments(runRecordDetail.getUserArgs()));
    // Creates a controller via the controller factory.
    // Since there is no startup start needed, the timeout is arbitrarily short
    return new ControllerFactory(runRecordDetail.getProgramRunId(), programOpts).create(null, 5,
        TimeUnit.SECONDS);
  }

  protected void monitorController(ProgramRunId programRunId,
      CompletableFuture<Void> startupTaskCompletion,
      RemoteExecutionTwillController controller,
      RemoteExecutionService remoteExecutionService) {
    startupTaskCompletion.thenAccept(o -> Services.start(remoteExecutionService));

    // On this controller termination, make sure it is removed from the controllers map and have resources released.
    controller.onTerminated(() -> {
      if (controllers.remove(programRunId, controller)) {
        controller.complete();
      }
    }, scheduler);
  }

  protected void addController(ProgramRunId programRunId,
      RemoteExecutionTwillController controller) {
    controllers.put(programRunId, controller);
  }

  protected RemoteExecutionTwillController getController(ProgramRunId programRunId) {
    return controllers.get(programRunId);
  }

  private final class ControllerFactory implements TwillControllerFactory {

    private final ProgramRunId programRunId;
    private final ProgramOptions programOpts;

    private ControllerFactory(ProgramRunId programRunId, ProgramOptions programOpts) {
      this.programRunId = programRunId;
      this.programOpts = programOpts;
    }

    @Override
    public TwillController create(@Nullable Callable<Void> startupTask, long timeout,
        TimeUnit timeoutUnit) {
      // Make sure we don't run the startup task and create controller if there is already one existed.
      controllersLock.lock();
      try {
        RemoteExecutionTwillController controller = getController(programRunId);
        if (controller != null) {
          return controller;
        }

        CompletableFuture<Void> startupTaskCompletion = new CompletableFuture<>();
        RuntimeJobRemoteProcessController processController = createRemoteProcessController(
            programRunId, programOpts);
        controller = createController(programRunId, programOpts, processController,
            startupTaskCompletion);

        // Execute the startup task if provided
        if (startupTask != null) {
          ClassLoader startupClassLoader = Optional
              .ofNullable(Thread.currentThread().getContextClassLoader())
              .orElse(getClass().getClassLoader());
          Future<?> startupTaskFuture = scheduler.submit(() -> {
            Map<String, String> systemArgs = programOpts.getArguments().asMap();
            LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(
                programRunId, systemArgs);
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
                  new TimeoutException(
                      "Starting of program run " + programRunId + " takes longer then "
                          + timeout + " " + timeoutUnit.name().toLowerCase()));
            }
          }, timeout, timeoutUnit);

          // If the startup task failed, publish failure state and delete the program running state
          startupTaskCompletion.whenComplete((res, throwable) -> {
            Map<String, String> systemArgs = programOpts.getArguments().asMap();
            LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(
              programRunId, systemArgs);
            try (LoggingContextRestorer restoreContext = LoggingContextAccessor.setLoggingContext(loggingContext)) {
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
                  processController.kill(RuntimeJobStatus.RUNNING);
                } catch (Exception e) {
                  LOG.warn("Force termination of remote process for {} failed", programRunId, e);
                }
                programStateWriter.error(programRunId, throwable);
              }
            } catch (Exception e) {
              LOG.error("Exception caught while setting logging context for program run {}", programRunId, e);
              programStateWriter.error(programRunId, throwable);
            }
          });
        } else {
          // Otherwise, complete the startup task immediately
          startupTaskCompletion.complete(null);
        }

        LOG.debug("Created controller for program run {}", programRunId);
        addController(programRunId, controller);
        return controller;
      } finally {
        controllersLock.unlock();
      }
    }

    /**
     * Creates a new instance of {@link RemoteExecutionTwillController}.
     */
    private RemoteExecutionTwillController createController(ProgramRunId programRunId,
        ProgramOptions programOpts,
        RuntimeJobRemoteProcessController processController,
        CompletableFuture<Void> startupTaskCompletion) {
      RemoteExecutionService remoteExecutionService = createRemoteExecutionService(programRunId,
          programOpts,
          processController);

      // TODO: (CDAP-19047) rework the contract here to be cleaner instead of implementation specific
      // If this is a tethered run, the RuntimeJobRemoteProcessController needs to be used to gracefully stop the run
      // Otherwise, stopping is handled by the RuntimeClient to have the program shut itself down,
      // so the RuntimeJobRemoteProcessController doesn't need to explicitly stop anything
      Map<String, String> systemArguments = programOpts.getArguments().asMap();
      String provisionerName = SystemArguments.getProfileProvisioner(systemArguments);
      String peerName = systemArguments.get(ProgramOptionConstants.PEER_NAME);
      boolean useControllerToStop = TetheringProvisioner.TETHERING_NAME.equals(provisionerName)
          && peerName == null;
      // Create the controller and start the runtime monitor when the startup task completed successfully.
      RemoteExecutionTwillController controller = new RemoteExecutionTwillController(cConf,
          programRunId,
          startupTaskCompletion,
          processController,
          scheduler, remoteExecutionService,
          useControllerToStop);

      monitorController(programRunId, startupTaskCompletion, controller, remoteExecutionService);
      return controller;
    }

    private RuntimeJobRemoteProcessController createRemoteProcessController(
        ProgramRunId programRunId, ProgramOptions programOpts) {
      LOG.debug("Creating controller for program run {} with runtime job manager", programRunId);
      return new RuntimeJobRemoteProcessController(
          programRunId, getRuntimeJobManagerSupplier(programRunId, programOpts));
    }

    private RemoteExecutionService createRemoteExecutionService(ProgramRunId programRunId,
        ProgramOptions programOpts,
        RuntimeJobRemoteProcessController processController) {
      RuntimeMonitors.requireMonitorUrl(cConf, programOpts);
      LOG.debug("Monitor program run {} with direct url", programRunId);
      return new RemoteExecutionService(cConf, programRunId, scheduler, processController,
          programStateWriter);
    }
  }
}
