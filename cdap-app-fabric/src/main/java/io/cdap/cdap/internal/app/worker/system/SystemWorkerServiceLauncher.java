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

package io.cdap.cdap.internal.app.worker.system;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerTwillRunnable;
import io.cdap.cdap.master.spi.twill.DependentTwillPreparer;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SystemWorkerServiceLauncher extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(SystemWorkerServiceLauncher.class);
  private final CConfiguration cConf;
  private final Configuration hConf;

  private final TwillRunner twillRunner;
  private TwillController twillController;

  private ScheduledExecutorService executor;

  @Inject
  public SystemWorkerServiceLauncher(CConfiguration cConf, Configuration hConf,
      TwillRunner twillRunner) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.twillRunner = twillRunner;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting SystemServiceLauncher.");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down SystemServiceLauncher.");
    try {
      if (twillController != null) {
        twillController.terminate().get(10, TimeUnit.SECONDS);
      }
    } catch (Exception e) {
      LOG.warn("Failed to terminate SystemServiceLauncher run", e);
    }
    if (executor != null) {
      executor.shutdownNow();
    }
    LOG.info("Shutting down SystemServiceLauncher has completed.");
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

  public void run() {
    TwillController activeController = null;
    for (TwillController controller : twillRunner.lookup(SystemWorkerTwillApplication.NAME)) {
      // If detected more than one controller, terminate those extra controllers.
      if (activeController != null) {
        controller.terminate();
      } else {
        activeController = controller;
      }
    }
    // If there is no system worker runner running, create one
    if (activeController == null) {
      try {
        Path tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
            cConf.get(Constants.AppFabric.TEMP_DIR)).toPath();
        Files.createDirectories(tmpDir);

        Path runDir = Files.createTempDirectory(tmpDir, "system.worker.launcher");
        try {
          CConfiguration cConfCopy = CConfiguration.copy(cConf);
          Path cConfPath = runDir.resolve("cConf.xml");
          try (Writer writer = Files.newBufferedWriter(cConfPath, StandardCharsets.UTF_8)) {
            cConfCopy.writeXml(writer);
          }
          Path hConfPath = runDir.resolve("hConf.xml");
          try (Writer writer = Files.newBufferedWriter(hConfPath, StandardCharsets.UTF_8)) {
            hConf.writeXml(writer);
          }

          ResourceSpecification systemResourceSpec = ResourceSpecification.Builder.with()
              .setVirtualCores(cConf.getInt(Constants.SystemWorker.CONTAINER_CORES))
              .setMemory(cConf.getInt(Constants.SystemWorker.CONTAINER_MEMORY_MB), ResourceSpecification.SizeUnit.MEGA)
              .setInstances(cConf.getInt(Constants.SystemWorker.CONTAINER_COUNT))
              .build();

          ResourceSpecification artifactLocalizerResourceSpec = ResourceSpecification.Builder.with()
              .setVirtualCores(cConf.getInt(Constants.ArtifactLocalizer.CONTAINER_CORES))
              .setMemory(cConf.getInt(Constants.ArtifactLocalizer.CONTAINER_MEMORY_MB),
                  ResourceSpecification.SizeUnit.MEGA)
              .setInstances(cConf.getInt(Constants.TaskWorker.CONTAINER_COUNT))
              .build();

          LOG.info("Starting SystemWorker pool with {} instances", systemResourceSpec.getInstances());

          TwillPreparer twillPreparer = twillRunner.prepare(
              new SystemWorkerTwillApplication(cConfPath.toUri(), hConfPath.toUri(), systemResourceSpec,
                  artifactLocalizerResourceSpec));

          String priorityClass = cConf.get(Constants.TaskWorker.CONTAINER_PRIORITY_CLASS_NAME);
          if (priorityClass != null) {
            twillPreparer = twillPreparer.setSchedulerQueue(priorityClass);
          }

          if (twillPreparer instanceof DependentTwillPreparer) {
            twillPreparer = ((DependentTwillPreparer) twillPreparer)
                .dependentRunnableNames(SystemWorkerTwillRunnable.class.getSimpleName(),
                    ArtifactLocalizerTwillRunnable.class.getSimpleName());
          }

          activeController = twillPreparer.start(5, TimeUnit.MINUTES);
          activeController.onRunning(() -> deleteDir(runDir), Threads.SAME_THREAD_EXECUTOR);
          activeController.onTerminated(() -> deleteDir(runDir), Threads.SAME_THREAD_EXECUTOR);
        } catch (Exception e) {
          deleteDir(runDir);
          throw e;
        }
      } catch (Exception e) {
        LOG.warn(String.format("Failed to launch SystemWorker pool, retry in %d",
            cConf.getInt(Constants.TaskWorker.POOL_CHECK_INTERVAL)), e);
      }
    }
    this.twillController = activeController;
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
