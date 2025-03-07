/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.app.preview;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.preview.PreviewRunnerTwillApplication;
import io.cdap.cdap.internal.app.preview.PreviewRunnerTwillRunnable;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerTwillRunnable;
import io.cdap.cdap.master.spi.twill.DependentTwillPreparer;
import io.cdap.cdap.master.spi.twill.ExtendedTwillPreparer;
import io.cdap.cdap.master.spi.twill.SecretDisk;
import io.cdap.cdap.master.spi.twill.SecureTwillPreparer;
import io.cdap.cdap.master.spi.twill.SecurityContext;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Launches a pool of preview runners.
 */
public class PreviewRunnerServiceLauncher extends AbstractScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewRunnerServiceLauncher.class);
//  private static final String STATEFUL_DISK_NAME = "preview-runner-data"; // TODO : dbshweta - check if required

  private final CConfiguration cConf;
  private final Configuration hConf;

  private final FeatureFlagsProvider featureFlagsProvider;

  private final TwillRunner twillRunner;
  private TwillController twillController;

  private ScheduledExecutorService executor;

  /**
   * Default Constructor with injected configuration and {@link TwillRunner}.
   */
  @Inject
  public PreviewRunnerServiceLauncher(CConfiguration cConf, Configuration hConf, TwillRunner twillRunner) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.twillRunner = twillRunner;
    this.featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting PreviewRunnerServiceLauncher.");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down PreviewRunnerServiceLauncher.");
    try {
      if (twillController != null) {
        twillController.terminate().get(10, TimeUnit.SECONDS);
      }
    } catch (Exception e) {
      LOG.warn("Failed to terminate PreviewRunnerServiceLauncher run", e);
    }
    if (executor != null) {
      executor.shutdownNow();
    }
    LOG.info("Shutting down PreviewRunnerServiceLauncher has completed.");
  }

  @Override
  protected void runOneIteration() throws Exception {
    run();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0,
                                          cConf.getInt(Constants.Preview.POOL_CHECK_INTERVAL), TimeUnit.SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("preview-runner-service-launcher-scheduler"));
    return executor;
  }

  /**
   * Inner run method for the service.
   */
  public void run() {
    // TODO : dbshweta check this flow and requirements
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

        Path runDir = Files.createTempDirectory(tmpDir, "preview.runner.launcher");
        try {
          // Unset the internal certificate path since certificate is stored cdap-security which
          // is not exposed (i.e. mounted in k8s) to TaskWorkerService.
          CConfiguration cConfCopy = CConfiguration.copy(cConf);
          cConfCopy.unset(Constants.Security.SSL.INTERNAL_CERT_PATH);

          // Enable the use of internal router in the preview runners pods if
          // required.
          cConfCopy.setBoolean(Constants.InternalRouter.CLIENT_ENABLED,
                               cConf.getBoolean(Constants.Preview.INTERNAL_ROUTER_ENABLED));

          Path cConfPath = runDir.resolve("cConf.xml");
          try (Writer writer = Files.newBufferedWriter(cConfPath, StandardCharsets.UTF_8)) {
            cConfCopy.writeXml(writer);
          }
          Path hConfPath = runDir.resolve("hConf.xml");
          try (Writer writer = Files.newBufferedWriter(hConfPath, StandardCharsets.UTF_8)) {
            hConf.writeXml(writer);
          }

          ResourceSpecification previewRunnerResourceSpec = ResourceSpecification.Builder.with()
            .setVirtualCores(cConf.getInt(Constants.Preview.CONTAINER_CORES))
            .setMemory(cConf.getInt(Constants.Preview.CONTAINER_MEMORY_MB),
                       ResourceSpecification.SizeUnit.MEGA)
            .setInstances(cConf.getInt(Constants.Preview.CONTAINER_COUNT))
            .build();

          ResourceSpecification artifactLocalizerResourceSpec = ResourceSpecification.Builder.with()
            .setVirtualCores(cConf.getInt(Constants.ArtifactLocalizer.CONTAINER_CORES))
            .setMemory(cConf.getInt(Constants.ArtifactLocalizer.CONTAINER_MEMORY_MB),
                       ResourceSpecification.SizeUnit.MEGA)
            .setInstances(cConf.getInt(Constants.Preview.CONTAINER_COUNT))
            .build();

          LOG.info("Starting Preview Runner pool with {} instances",
                   previewRunnerResourceSpec.getInstances());

          TwillPreparer twillPreparer = twillRunner.prepare(
            new PreviewRunnerTwillApplication(cConfPath.toUri(), hConfPath.toUri(),
                                           previewRunnerResourceSpec,
                                              Optional.ofNullable(artifactLocalizerResourceSpec)));
          // If internal router is enabled, we need to localize the cdap-site copy
          // as a configmap so that the init container also uses the internal
          // router.
          if (twillPreparer instanceof ExtendedTwillPreparer) {
            twillPreparer = ((ExtendedTwillPreparer) twillPreparer)
              .setShouldLocalizeConfigurationAsConfigmap(
                cConf.getBoolean(Constants.Preview.INTERNAL_ROUTER_ENABLED));
          }

          Map<String, String> configMap = new HashMap<>();
          configMap.put(ProgramOptionConstants.RUNTIME_NAMESPACE,
                        NamespaceId.SYSTEM.getNamespace());
          twillPreparer.withConfiguration(Collections.unmodifiableMap(configMap));

          if (Feature.NAMESPACED_SERVICE_ACCOUNTS.isEnabled(featureFlagsProvider)) {
            String localhost = InetAddress.getLoopbackAddress().getHostName();
            twillPreparer = twillPreparer.withEnv(PreviewRunnerTwillRunnable.class.getSimpleName(),
                                                  ImmutableMap.of(
                                                    Constants.TaskWorker.GCE_METADATA_HOST_ENV_VAR,
                                                    String.format("%s:%s", localhost,
                                                                  cConf.getInt(Constants.ArtifactLocalizer.PORT))
                                                  ));
            twillPreparer = ((SecureTwillPreparer) twillPreparer)
              .withNamespacedWorkloadIdentity(PreviewRunnerTwillRunnable.class.getSimpleName());
          }

          String priorityClass = cConf.get(Constants.TaskWorker.CONTAINER_PRIORITY_CLASS_NAME);
          if (priorityClass != null) {
            twillPreparer = twillPreparer.setSchedulerQueue(priorityClass);
          }

          if (twillPreparer instanceof DependentTwillPreparer) {
            twillPreparer = ((DependentTwillPreparer) twillPreparer)
              .dependentRunnableNames(PreviewRunnerTwillRunnable.class.getSimpleName(),
                                      ArtifactLocalizerTwillRunnable.class.getSimpleName());
          }

          /* if (twillPreparer instanceof StatefulTwillPreparer) {
            int diskSize = cConf.getInt(Constants.Preview.CONTAINER_DISK_SIZE_GB);
            twillPreparer = ((StatefulTwillPreparer) twillPreparer)
              .withStatefulRunnable(PreviewRunnerTwillRunnable.class.getSimpleName(), false,
                                    new StatefulDisk(STATEFUL_DISK_NAME, diskSize,
                                                     cConf.get(Constants.CFG_LOCAL_DATA_DIR)));

            if (cConf.getBoolean(Constants.Preview.CONTAINER_DISK_READONLY)) {
              twillPreparer = ((StatefulTwillPreparer) twillPreparer)
                .withReadonlyDisk(TaskWorkerTwillRunnable.class.getSimpleName(),
                                  STATEFUL_DISK_NAME);
            }
          } */

          if (twillPreparer instanceof ExtendedTwillPreparer) {
            int workDirSize = cConf.getInt(Constants.Preview.CONTAINER_WORKDIR_SIZE_MB);
            twillPreparer = ((ExtendedTwillPreparer) twillPreparer)
              .setWorkdirSizeLimit(workDirSize);
          }

          if (twillPreparer instanceof SecureTwillPreparer) {
            SecurityContext securityContext = createSecurityContext();
            twillPreparer = ((SecureTwillPreparer) twillPreparer)
              .withSecurityContext(PreviewRunnerTwillRunnable.class.getSimpleName(),
                                   securityContext);
            // Mount secret in ArtifactLocalizer sidecar which only run trusted code,
            // so requests originated by ArtifactLocalizer can run with system identity when internal auth
            // is enabled.
            twillPreparer = ((SecureTwillPreparer) twillPreparer)
              .withSecretDisk(ArtifactLocalizerTwillRunnable.class.getSimpleName(),
                              new SecretDisk(cConf.get(Constants.Twill.Security.MASTER_SECRET_DISK_NAME),
                                             cConf.get(Constants.Twill.Security.MASTER_SECRET_DISK_PATH)));
            if (cConf.getBoolean(Constants.Twill.Security.WORKER_MOUNT_SECRET)) {
              String secretName = cConf.get(Constants.Twill.Security.WORKER_SECRET_DISK_NAME);
              String secretPath = cConf.get(Constants.Twill.Security.WORKER_SECRET_DISK_PATH);
              twillPreparer = ((SecureTwillPreparer) twillPreparer)
                .withSecretDisk(PreviewRunnerTwillRunnable.class.getSimpleName(),
                                new SecretDisk(secretName, secretPath));
            }
          }

          // Set JVM options for task worker and artifact localizer
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
        LOG.warn(String.format("Failed to launch Preview Runner pool, retry in %d",
                               cConf.getInt(Constants.TaskWorker.POOL_CHECK_INTERVAL)), e);
      }
    }
    this.twillController = activeController;
  }

  private SecurityContext createSecurityContext() {
    SecurityContext.Builder builder = new SecurityContext.Builder();
    String twillUserIdentity = cConf.get(Constants.Twill.Security.IDENTITY_USER);
    if (twillUserIdentity != null) {
      builder.withIdentity(twillUserIdentity);
    }

    try {
      Long userId = cConf.getLong(Constants.TaskWorker.CONTAINER_RUN_AS_USER);
      builder.withUserId(userId);
    } catch (NullPointerException e) {
      //no-op if configuration property does not exist
    }

    try {
      Long groupId = cConf.getLong(Constants.TaskWorker.CONTAINER_RUN_AS_GROUP);
      builder.withGroupId(groupId);
    } catch (NullPointerException e) {
      //no-op if configuration property does not exist
    }

    return builder.build();
  }

  private void deleteDir(Path dir) {
    try {
      if (Files.isDirectory(dir)) {
        DirUtils.deleteDirectoryContents(dir.toFile());
      }
    } catch (IOException e) {
      LOG.warn("Failed to cleanup directory {}", dir, e);
    }
  }

}
