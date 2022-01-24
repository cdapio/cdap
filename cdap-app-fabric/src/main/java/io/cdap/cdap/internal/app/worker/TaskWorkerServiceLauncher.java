/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerTwillRunnable;
import io.cdap.cdap.master.spi.twill.DependentTwillPreparer;
import io.cdap.cdap.master.spi.twill.SecretDisk;
import io.cdap.cdap.master.spi.twill.SecureTwillPreparer;
import io.cdap.cdap.master.spi.twill.SecurityContext;
import io.cdap.cdap.master.spi.twill.StatefulDisk;
import io.cdap.cdap.master.spi.twill.StatefulTwillPreparer;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Launches a pool of task workers.
 */
public class TaskWorkerServiceLauncher extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerServiceLauncher.class);
  private static final String STATEFUL_DISK_NAME = "task-worker-data";

  private final CConfiguration cConf;
  private final Configuration hConf;

  private final TwillRunner twillRunner;
  private TwillController twillController;

  private ScheduledExecutorService executor;

  @Inject
  public TaskWorkerServiceLauncher(CConfiguration cConf, Configuration hConf,
                                   TwillRunner twillRunner) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.twillRunner = twillRunner;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting TaskWorkerServiceLauncher.");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down TaskWorkerServiceLauncher.");
    try {
      if (twillController != null) {
        twillController.terminate().get(10, TimeUnit.SECONDS);
      }
    } catch (Exception e) {
      LOG.warn("Failed to terminate TaskWorkerServiceLauncher run", e);
    }
    if (executor != null) {
      executor.shutdownNow();
    }
    LOG.info("Shutting down TaskWorkerServiceLauncher has completed.");
  }

  @Override
  protected void runOneIteration() throws Exception {
    run();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0,
                                          cConf.getInt(Constants.TaskWorker.POOL_CHECK_INTERVAL), TimeUnit.SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("task-worker-service-launcher-scheduler"));
    return executor;
  }

  public void run() {
    TwillController activeController = null;
    for (TwillController controller : twillRunner.lookup(TaskWorkerTwillApplication.NAME)) {
      // If detected more than one controller, terminate those extra controllers.
      if (activeController != null) {
        controller.terminate();
      } else {
        activeController = controller;
      }
    }
    // If there is no task worker runner running, create one
    if (activeController == null) {
      try {
        Path tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                               cConf.get(Constants.AppFabric.TEMP_DIR)).toPath();
        Files.createDirectories(tmpDir);

        Path runDir = Files.createTempDirectory(tmpDir, "task.worker.launcher");
        try {
          // Unset the internal certificate path since certificate is stored cdap-security which
          // is not exposed (i.e. mounted in k8s) to TaskWorkerService.
          CConfiguration cConfCopy = CConfiguration.copy(cConf);
          cConfCopy.unset(Constants.Security.SSL.INTERNAL_CERT_PATH);
          Path cConfPath = runDir.resolve("cConf.xml");
          try (Writer writer = Files.newBufferedWriter(cConfPath, StandardCharsets.UTF_8)) {
            cConfCopy.writeXml(writer);
          }
          Path hConfPath = runDir.resolve("hConf.xml");
          try (Writer writer = Files.newBufferedWriter(hConfPath, StandardCharsets.UTF_8)) {
            hConf.writeXml(writer);
          }

          ResourceSpecification taskworkerResourceSpec = ResourceSpecification.Builder.with()
            .setVirtualCores(cConf.getInt(Constants.TaskWorker.CONTAINER_CORES))
            .setMemory(cConf.getInt(Constants.TaskWorker.CONTAINER_MEMORY_MB), ResourceSpecification.SizeUnit.MEGA)
            .setInstances(cConf.getInt(Constants.TaskWorker.CONTAINER_COUNT))
            .build();

          ResourceSpecification artifactLocalizerResourceSpec = ResourceSpecification.Builder.with()
            .setVirtualCores(cConf.getInt(Constants.ArtifactLocalizer.CONTAINER_CORES))
            .setMemory(cConf.getInt(Constants.ArtifactLocalizer.CONTAINER_MEMORY_MB),
                       ResourceSpecification.SizeUnit.MEGA)
            .setInstances(cConf.getInt(Constants.TaskWorker.CONTAINER_COUNT))
            .build();

          LOG.info("Starting TaskWorker pool with {} instances", taskworkerResourceSpec.getInstances());

          TwillPreparer twillPreparer = twillRunner.prepare(
            new TaskWorkerTwillApplication(cConfPath.toUri(), hConfPath.toUri(), taskworkerResourceSpec,
                                           artifactLocalizerResourceSpec));

          String priorityClass = cConf.get(Constants.TaskWorker.CONTAINER_PRIORITY_CLASS_NAME);
          if (priorityClass != null) {
            twillPreparer = twillPreparer.setSchedulerQueue(priorityClass);
          }

          if (twillPreparer instanceof DependentTwillPreparer) {
            twillPreparer = ((DependentTwillPreparer) twillPreparer)
              .dependentRunnableNames(TaskWorkerTwillRunnable.class.getSimpleName(),
                                      ArtifactLocalizerTwillRunnable.class.getSimpleName());
          }

          if (twillPreparer instanceof StatefulTwillPreparer) {
            int diskSize = cConf.getInt(Constants.TaskWorker.CONTAINER_DISK_SIZE_GB);
            twillPreparer = ((StatefulTwillPreparer) twillPreparer)
              .withStatefulRunnable(TaskWorkerTwillRunnable.class.getSimpleName(), false,
                                    new StatefulDisk(STATEFUL_DISK_NAME, diskSize,
                                                     cConf.get(Constants.CFG_LOCAL_DATA_DIR)));
            
            if (cConf.getBoolean(Constants.TaskWorker.CONTAINER_DISK_READONLY)) {
              twillPreparer = ((StatefulTwillPreparer) twillPreparer)
                .withReadonlyDisk(TaskWorkerTwillRunnable.class.getSimpleName(), STATEFUL_DISK_NAME);
            }
          }

          if (twillPreparer instanceof SecureTwillPreparer) {
            SecurityContext securityContext = createSecurityContext();
            twillPreparer = ((SecureTwillPreparer) twillPreparer)
              .withSecurityContext(TaskWorkerTwillRunnable.class.getSimpleName(), securityContext);
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
                .withSecretDisk(TaskWorkerTwillRunnable.class.getSimpleName(), new SecretDisk(secretName, secretPath));
            }
          }

          activeController = twillPreparer.start(5, TimeUnit.MINUTES);
          activeController.onRunning(() -> deleteDir(runDir), Threads.SAME_THREAD_EXECUTOR);
          activeController.onTerminated(() -> deleteDir(runDir), Threads.SAME_THREAD_EXECUTOR);
        } catch (Exception e) {
          deleteDir(runDir);
          throw e;
        }
      } catch (Exception e) {
        LOG.warn(String.format("Failed to launch TaskWorker pool, retry in %d",
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
