/*
 * Copyright © 2020-2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.preview.PreviewConfigModule;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRequestQueue;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.InternalRouter;
import io.cdap.cdap.common.conf.Constants.Preview;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.encryption.AeadCipher;
import io.cdap.cdap.common.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerTwillRunnable;
import io.cdap.cdap.master.spi.twill.DependentTwillPreparer;
import io.cdap.cdap.master.spi.twill.ExtendedTwillPreparer;
import io.cdap.cdap.master.spi.twill.SecretDisk;
import io.cdap.cdap.master.spi.twill.SecureTwillPreparer;
import io.cdap.cdap.master.spi.twill.SecurityContext;
import io.cdap.cdap.master.spi.twill.StatefulDisk;
import io.cdap.cdap.master.spi.twill.StatefulTwillPreparer;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PreviewManager} to be used in distributed mode where it launches preview runner in
 * separate processes.
 */
public class DistributedPreviewManager extends DefaultPreviewManager implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedPreviewManager.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final FeatureFlagsProvider featureFlagsProvider;
  private final TwillRunner twillRunner;
  private final PreviewRunStopper previewRunStopper;
  private final PreviewStore previewStore;
  private ScheduledExecutorService scheduler;
  private TwillController controller;

  @Inject
  DistributedPreviewManager(CConfiguration cConf, Configuration hConf,
      DiscoveryServiceClient discoveryServiceClient,
      @Named(DataSetsModules.BASE_DATASET_FRAMEWORK) DatasetFramework datasetFramework,
      TransactionSystemClient transactionSystemClient,
      AccessControllerInstantiator accessControllerInstantiator,
      AccessEnforcer accessEnforcer,
      AuthenticationContext authenticationContext,
      @Named(PreviewConfigModule.PREVIEW_LEVEL_DB) LevelDBTableService previewLevelDbTableService,
      @Named(PreviewConfigModule.PREVIEW_CCONF) CConfiguration previewCconf,
      @Named(PreviewConfigModule.PREVIEW_HCONF) Configuration previewHconf,
      @Named(PreviewConfigModule.PREVIEW_SCONF) SConfiguration previewSconf,
      PreviewRequestQueue previewRequestQueue, PreviewStore previewStore,
      PreviewRunStopper previewRunStopper, MessagingService messagingService,
      MetricsCollectionService metricsCollectionService,
      PreviewDataCleanupService previewDataCleanupService,
      TwillRunner twillRunner,
      @Named(UserCredentialAeadEncryptionModule.USER_CREDENTIAL_ENCRYPTION) AeadCipher userEncryptionAeadCipher) {
    super(discoveryServiceClient, datasetFramework, transactionSystemClient,
        accessControllerInstantiator, accessEnforcer, authenticationContext,
        previewLevelDbTableService,
        previewCconf, previewHconf, previewSconf, previewRequestQueue, previewStore,
        previewRunStopper,
        messagingService, previewDataCleanupService, metricsCollectionService, userEncryptionAeadCipher);

    this.cConf = cConf;
    this.hConf = hConf;
    this.twillRunner = twillRunner;
    this.featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
    this.previewRunStopper = previewRunStopper;
    this.previewStore = previewStore;
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();
    scheduler = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("preview-manager"));
    scheduler.scheduleWithFixedDelay(this, 2, 5, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    super.shutDown();
    if (scheduler != null) {
      Future<?> future = scheduler.submit(() -> {
        try {
          if (controller != null) {
            controller.terminate().get(10, TimeUnit.SECONDS);
          }
        } catch (Exception e) {
          LOG.warn("Failed to terminate preview runner", e);
        } finally {
          scheduler.shutdown();
        }
      });
      future.get();
    }
  }

  @Override
  public void run() {
    TwillController activeController = null;
    for (TwillController controller : twillRunner.lookup(PreviewRunnerTwillApplication.NAME)) {
      // If detected more than one controller, terminate those extra controllers.
      if (activeController != null) {
        controller.terminate();
      } else {
        activeController = controller;
      }
    }

    // If there is no preview runner running, create one
    if (activeController == null) {
      try {
        Path tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
            cConf.get(Constants.AppFabric.TEMP_DIR)).toPath();
        Files.createDirectories(tmpDir);

        Path runDir = Files.createTempDirectory(tmpDir, "preview");
        try {
          CConfiguration cConfCopy = CConfiguration.copy(cConf);
          if (!cConf.getBoolean(Constants.Twill.Security.WORKER_MOUNT_SECRET)) {
            // Unset the internal certificate path since certificate is stored cdap-security which
            // is not going to be exposed to preview runner.
            // TODO: CDAP-18768 this will break preview when certificate checking is enabled.
            cConfCopy.unset(Constants.Security.SSL.INTERNAL_CERT_PATH);
          }
          // Enable the use of internal router in the preview runner pods if
          // required.
          cConfCopy.setBoolean(InternalRouter.CLIENT_ENABLED,
              cConf.getBoolean(Preview.INTERNAL_ROUTER_ENABLED));

          Path cConfPath = runDir.resolve("cConf.xml");
          try (Writer writer = Files.newBufferedWriter(cConfPath, StandardCharsets.UTF_8)) {
            cConfCopy.writeXml(writer);
          }
          Path hConfPath = runDir.resolve("hConf.xml");
          try (Writer writer = Files.newBufferedWriter(hConfPath, StandardCharsets.UTF_8)) {
            hConf.writeXml(writer);
          }

          ResourceSpecification runnerResourceSpec = ResourceSpecification.Builder.with()
              .setVirtualCores(cConf.getInt(Constants.Preview.CONTAINER_CORES))
              .setMemory(cConf.getInt(Constants.Preview.CONTAINER_MEMORY_MB),
                  ResourceSpecification.SizeUnit.MEGA)
              .setInstances(cConf.getInt(Constants.Preview.CONTAINER_COUNT))
              .build();

          Optional<ResourceSpecification> artifactLocalizerResourceSpec = Optional.of(
              ResourceSpecification.Builder.with()
                  .setVirtualCores(cConf.getInt(Constants.ArtifactLocalizer.CONTAINER_CORES))
                  .setMemory(cConf.getInt(Constants.ArtifactLocalizer.CONTAINER_MEMORY_MB),
                      ResourceSpecification.SizeUnit.MEGA)
                  .setInstances(cConf.getInt(Constants.TaskWorker.CONTAINER_COUNT))
                  .build());

          LOG.info("Starting preview runners with {} instances and artifactLocalizer enabled",
              runnerResourceSpec.getInstances());

          TwillPreparer twillPreparer = twillRunner.prepare(
              new PreviewRunnerTwillApplication(cConfPath.toUri(),
                  hConfPath.toUri(),
                  runnerResourceSpec,
                  artifactLocalizerResourceSpec));

          Map<String, String> configMap = new HashMap<>();
          configMap.put(ProgramOptionConstants.RUNTIME_NAMESPACE,
              NamespaceId.SYSTEM.getNamespace());
          twillPreparer.withConfiguration(Collections.unmodifiableMap(configMap));
          // If internal router is enabled, we need to localize the cdap-site copy
          // as a configmap so that the init container also uses the internal
          // router.
          if (twillPreparer instanceof ExtendedTwillPreparer) {
            twillPreparer = ((ExtendedTwillPreparer) twillPreparer)
                .setShouldLocalizeConfigurationAsConfigmap(
                    cConf.getBoolean(Preview.INTERNAL_ROUTER_ENABLED));
          }

          if (Feature.NAMESPACED_SERVICE_ACCOUNTS.isEnabled(featureFlagsProvider)) {
            String localhost = InetAddress.getLoopbackAddress().getHostName();
            twillPreparer = twillPreparer.withEnv(PreviewRunnerTwillRunnable.class.getSimpleName(),
                ImmutableMap.of(
                    Constants.Preview.GCE_METADATA_HOST_ENV_VAR,
                    String.format("%s:%s", localhost,
                        cConf.getInt(Constants.ArtifactLocalizer.PORT))
                ));
            twillPreparer = ((SecureTwillPreparer) twillPreparer)
                .withNamespacedWorkloadIdentity(PreviewRunnerTwillRunnable.class.getSimpleName());
          }

          String priorityClass = cConf.get(Constants.Preview.CONTAINER_PRIORITY_CLASS_NAME);
          if (priorityClass != null) {
            twillPreparer = twillPreparer.setSchedulerQueue(priorityClass);
          }

          if (twillPreparer instanceof DependentTwillPreparer) {
            twillPreparer = ((DependentTwillPreparer) twillPreparer)
                .dependentRunnableNames(PreviewRunnerTwillRunnable.class.getSimpleName(),
                    ArtifactLocalizerTwillRunnable.class.getSimpleName());
          }

          if (twillPreparer instanceof StatefulTwillPreparer) {
            int diskSize = cConf.getInt(Constants.Preview.CONTAINER_DISK_SIZE_GB);
            twillPreparer = ((StatefulTwillPreparer) twillPreparer)
                .withStatefulRunnable(PreviewRunnerTwillRunnable.class.getSimpleName(), false,
                    new StatefulDisk("preview-runner-data", diskSize,
                        cConf.get(Constants.CFG_LOCAL_DATA_DIR)));
          }

          if (twillPreparer instanceof SecureTwillPreparer) {
            String twillUserIdentity = cConf.get(Constants.Twill.Security.IDENTITY_USER);
            if (twillUserIdentity != null) {
              SecurityContext securityContext = new SecurityContext.Builder()
                  .withIdentity(twillUserIdentity).build();
              twillPreparer = ((SecureTwillPreparer) twillPreparer)
                  .withSecurityContext(PreviewRunnerTwillRunnable.class.getSimpleName(),
                      securityContext);
            }

            // Mount secret in ArtifactLocalizer sidecar which only runs trusted code,
            // so requests originated by ArtifactLocalizer can run with system identity when internal auth
            // is enabled.
            String artifactLocalizerSecretName = cConf.get(
                Constants.Twill.Security.MASTER_SECRET_DISK_NAME);
            String artifactLocalizerSecretPath = cConf.get(
                Constants.Twill.Security.MASTER_SECRET_DISK_PATH);
            twillPreparer = ((SecureTwillPreparer) twillPreparer)
                .withSecretDisk(ArtifactLocalizerTwillRunnable.class.getSimpleName(),
                    new SecretDisk(artifactLocalizerSecretName, artifactLocalizerSecretPath));
            // Mount worker secrets as configured in CConf
            if (cConf.getBoolean(Constants.Twill.Security.WORKER_MOUNT_SECRET)) {
              String secretName = cConf.get(Constants.Twill.Security.WORKER_SECRET_DISK_NAME);
              String secretPath = cConf.get(Constants.Twill.Security.WORKER_SECRET_DISK_PATH);
              twillPreparer = ((SecureTwillPreparer) twillPreparer)
                  .withSecretDisk(PreviewRunnerTwillRunnable.class.getSimpleName(),
                      new SecretDisk(secretName, secretPath));
            }
          }

          // Set JVM options for preview runner and artifact localizer
          twillPreparer.setJVMOptions(PreviewRunnerTwillRunnable.class.getSimpleName(),
              cConf.get(Constants.Preview.CONTAINER_JVM_OPTS));
          twillPreparer.setJVMOptions(ArtifactLocalizerTwillRunnable.class.getSimpleName(),
              cConf.get(Constants.ArtifactLocalizer.CONTAINER_JVM_OPTS));

          activeController = twillPreparer.start(5, TimeUnit.MINUTES);
          activeController.onRunning(() -> deleteDir(runDir), Threads.SAME_THREAD_EXECUTOR);
          activeController.onTerminated(() -> deleteDir(runDir), Threads.SAME_THREAD_EXECUTOR);
        } catch (Exception e) {
          deleteDir(runDir);
          throw e;
        }
      } catch (Exception e) {
        LOG.warn("Failed to launch preview runner. It will be retried", e);
      }
    }
    controller = activeController;
  }

  /**
   * Overrides the default poll behavior to enforce a "one-request-per-runner" policy.
   * <p>
   * This implementation first checks if the polling runner is already associated with an active
   * preview run. If it is, this indicates a protocol violation or a state inconsistency. In this
   * case, the existing preview run is immediately marked as {@link PreviewStatus.Status#KILLED}
   * with a reason indicating an invalid state. A best-effort attempt is then made to terminate the
   * runner's container, and the poll request is denied by returning an empty {@link Optional}.
   * <p>
   * If the runner is not associated with an existing run, this method delegates to the parent
   * {@code poll} method to retrieve the next available preview request from the queue.
   *
   * @param pollerInfo Information that uniquely identifies the polling preview runner.
   * @return {@link Optional} containing a {@link PreviewRequest} if a job is available for a valid
   * runner, or {@link Optional#empty()} if the request is denied due to a protocol violation or if
   * no job is currently available in the queue.
   */
  @Override
  public Optional<PreviewRequest> poll(@Nullable byte[] pollerInfo) {
    // In a distributed environment, pollerInfo is always expected.
    // Return empty to deny the request without throwing an exception.
    if (pollerInfo == null) {
      LOG.warn("poll() was called with a null pollerInfo in a distributed environment. "
          + "This is unexpected. Denying the request.");
      return Optional.empty();
    }

    // Check if the runner is already registered to an application.
    // If no app is associated with this runner, it's a valid new runner.
    // Proceed to the default polling behavior.
    ApplicationId existingAppId = previewStore.getApplicationId(pollerInfo);
    if (existingAppId == null) {
      return super.poll(pollerInfo);
    }

    // The runner is already registered, which is a state violation.
    // Handle the violation and deny the request.
    handlePollingViolation(existingAppId, pollerInfo);
    return Optional.empty();
  }

  private void handlePollingViolation(ApplicationId appId, byte[] pollerInfo) {
    try {
      PreviewStatus status = previewStore.getPreviewStatus(appId);
      if (status != null && !status.getStatus().isEndState()) {
        LOG.warn("Runner for application '{}' polled for a new request, indicating a state violation. "
            + "Setting status to KILLED.", appId);
        previewStore.setPreviewStatus(appId,
            new PreviewStatus(PreviewStatus.Status.KILLED, status.getSubmitTime(),
                new BasicThrowable(new IllegalStateException(
                    "Preview run stopped due to an invalid state. "
                        + "A runner can only be assigned one preview run at a time.")), null, null));
      }
      previewRunStopper.stop(pollerInfo);
    } catch (Exception e) {
      LOG.warn("Attempted to stop a runner due to a state violation but failed. "
          + "The runner was still denied a new request.", e);
    }
  }

  /**
   * Deletes the given directory {@link Path} recursively.
   */
  private void deleteDir(Path dir) {
    try {
      if (Files.isDirectory(dir)) {
        DirUtils.deleteDirectoryContents(dir.toFile());
      }
    } catch (IOException e) {
      LOG.warn("Failed to clean up directory {}", dir, e);
    }
  }
}
