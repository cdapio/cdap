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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.utils.DirUtils;
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
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Launches a pool of task workers.
 */
public class TaskWorkerServiceLauncher extends AbstractIdleService implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerServiceLauncher.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final SConfiguration sConf;

  private final TwillRunner twillRunner;
  private TwillController twillController;
  private ScheduledExecutorService scheduler;

  @Inject
  public TaskWorkerServiceLauncher(CConfiguration cConf, SConfiguration sConf, Configuration hConf,
                                   TwillRunner twillRunner) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.sConf = sConf;
    this.twillRunner = twillRunner;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting TaskWorkerServiceLauncher.");
    scheduler = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("task-worker-service-launcher"));
    scheduler.scheduleWithFixedDelay(this, 2, 5, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down TaskWorkerServiceLauncher.");
    if (scheduler != null) {
      Future<?> future = scheduler.submit(() -> {
        try {
          if (twillController != null) {
            twillController.terminate().get(10, TimeUnit.SECONDS);
          }
        } catch (Exception e) {
          LOG.warn("Failed to terminate preview runner", e);
        } finally {
          scheduler.shutdown();
        }
      });
      future.get();
    }
    LOG.info("Shutting down TaskWorkerServiceLauncher has completed.");
  }

  @Override
  public void run() {
    LOG.info("Starting TaskWorkerService worker pool");
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
          Path cConfPath = runDir.resolve("cConf.xml");
          try (Writer writer = Files.newBufferedWriter(cConfPath, StandardCharsets.UTF_8)) {
            cConf.writeXml(writer);
          }
          Path hConfPath = runDir.resolve("hConf.xml");
          try (Writer writer = Files.newBufferedWriter(hConfPath, StandardCharsets.UTF_8)) {
            hConf.writeXml(writer);
          }

          ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
            .setVirtualCores(cConf.getInt(Constants.TaskWorker.CONTAINER_CORES))
            .setMemory(cConf.getInt(Constants.TaskWorker.CONTAINER_MEMORY_MB), ResourceSpecification.SizeUnit.MEGA)
            .setInstances(cConf.getInt(Constants.TaskWorker.CONTAINER_COUNT))
            .build();

          LOG.info("Starting TaskWorker pool with {} instances", resourceSpec.getInstances());

          TwillPreparer twillPreparer = twillRunner.prepare(new TaskWorkerTwillApplication(cConfPath.toUri(),
                                                                                           hConfPath.toUri(),
                                                                                           resourceSpec));
          String priorityClass = cConf.get(Constants.TaskWorker.CONTAINER_PRIORITY_CLASS_NAME);
          if (priorityClass != null) {
            twillPreparer = twillPreparer.setSchedulerQueue(priorityClass);
          }

          if (twillPreparer instanceof StatefulTwillPreparer) {
            int diskSize = cConf.getInt(Constants.TaskWorker.CONTAINER_DISK_SIZE_GB);
            twillPreparer = ((StatefulTwillPreparer) twillPreparer)
              .withStatefulRunnable(TaskWorkerTwillRunnable.class.getSimpleName(), false,
                                    new StatefulDisk("task-worker-data", diskSize, "/data"));
          }

          activeController = twillPreparer.start(5, TimeUnit.MINUTES);
          activeController.onRunning(() -> deleteDir(runDir), Threads.SAME_THREAD_EXECUTOR);
          activeController.onTerminated(() -> deleteDir(runDir), Threads.SAME_THREAD_EXECUTOR);
        } catch (Exception e) {
          deleteDir(runDir);
          throw e;
        }
      } catch (Exception e) {
        LOG.warn("Failed to launch TaskWorker pool. It will NOT be retried", e);
      }
    }
    this.twillController = activeController;
    LOG.info("Starting TaskWorker pool has completed");
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
