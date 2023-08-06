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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.preview.PreviewConfigModule;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.app.preview.PreviewRequestQueue;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.InternalRouter;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerTwillRunnable;
import io.cdap.cdap.master.spi.twill.DependentTwillPreparer;
import io.cdap.cdap.master.spi.twill.ExtendedTwillPreparer;
import io.cdap.cdap.master.spi.twill.SecretDisk;
import io.cdap.cdap.master.spi.twill.SecureTwillPreparer;
import io.cdap.cdap.master.spi.twill.SecurityContext;
import io.cdap.cdap.master.spi.twill.StatefulDisk;
import io.cdap.cdap.master.spi.twill.StatefulTwillPreparer;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
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
  private final TwillRunner twillRunner;
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
      @Named(PreviewConfigModule.PREVIEW_LEVEL_DB) LevelDBTableService previewLevelDBTableService,
      @Named(PreviewConfigModule.PREVIEW_CCONF) CConfiguration previewCConf,
      @Named(PreviewConfigModule.PREVIEW_HCONF) Configuration previewHConf,
      @Named(PreviewConfigModule.PREVIEW_SCONF) SConfiguration previewSConf,
      PreviewRequestQueue previewRequestQueue, PreviewStore previewStore,
      PreviewRunStopper previewRunStopper, MessagingService messagingService,
      MetricsCollectionService metricsCollectionService,
      PreviewDataCleanupService previewDataCleanupService,
      TwillRunner twillRunner) {
    super(discoveryServiceClient, datasetFramework, transactionSystemClient,
        accessControllerInstantiator, accessEnforcer, authenticationContext,
        previewLevelDBTableService,
        previewCConf, previewHConf, previewSConf, previewRequestQueue, previewStore,
        previewRunStopper,
        messagingService, previewDataCleanupService, metricsCollectionService);

    this.cConf = cConf;
    this.hConf = hConf;
    this.twillRunner = twillRunner;
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
          Path cConfPath = runDir.resolve("cConf.xml");
          if (!cConf.getBoolean(Constants.Twill.Security.WORKER_MOUNT_SECRET)) {
            // Unset the internal certificate path since certificate is stored cdap-security which
            // is not going to be exposed to preview runner.
            // TODO: CDAP-18768 this will break preview when certificate checking is enabled.
            cConfCopy.unset(Constants.Security.SSL.INTERNAL_CERT_PATH);
          }
          // TODO: Set this base on a cConf property.
          cConfCopy.setBoolean(InternalRouter.USE_INTERNAL_ROUTER, true);
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

          Optional<ResourceSpecification> artifactLocalizerResourceSpec = Optional.empty();
          artifactLocalizerResourceSpec = Optional.of(
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
          configMap.put(ProgramOptionConstants.USE_CONFIGMAP, "true");
          twillPreparer.withConfiguration(Collections.unmodifiableMap(configMap));

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

          if (twillPreparer instanceof ExtendedTwillPreparer) {
            ((ExtendedTwillPreparer) twillPreparer)
                .setAnnotations(Collections.singletonMap(
                    "master.environment.k8s.enable.internal.router", "true"));
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
