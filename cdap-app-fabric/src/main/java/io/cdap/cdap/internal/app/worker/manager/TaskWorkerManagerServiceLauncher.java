/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.manager;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.master.spi.twill.ExtendedTwillPreparer;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launches the single task worker manager proxy that fronts the task worker pool. Singleton so
 * that only one reconciliation loop can launch it.
 */
@Singleton
public class TaskWorkerManagerServiceLauncher extends AbstractScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerManagerServiceLauncher.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final TwillRunner twillRunner;

  private TwillController twillController;
  private ScheduledExecutorService executor;

  @Inject
  public TaskWorkerManagerServiceLauncher(CConfiguration cConf, Configuration hConf,
      TwillRunner twillRunner) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.twillRunner = twillRunner;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting TaskWorkerManagerServiceLauncher.");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down TaskWorkerManagerServiceLauncher.");
    try {
      if (twillController != null) {
        twillController.terminate().get(10, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while terminating the task worker manager proxy", e);
    } catch (Exception e) {
      LOG.warn("Failed to terminate TaskWorkerManagerServiceLauncher run", e);
    }
    if (executor != null) {
      executor.shutdownNow();
    }
    LOG.info("Shutting down TaskWorkerManagerServiceLauncher has completed.");
  }

  @Override
  protected void runOneIteration() throws Exception {
    run();
  }

  @Override
  protected Scheduler scheduler() {
    // No initial delay: App Fabric starts dispatching through the proxy as soon as it's up.
    return Scheduler.newFixedRateSchedule(0,
        cConf.getInt(Constants.TaskWorkerManager.POOL_CHECK_INTERVAL), TimeUnit.SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("task-worker-manager-service-launcher-scheduler"));
    return executor;
  }

  /**
   * Ensures exactly one proxy application is running. Package-private for tests.
   */
  void run() {
    TwillController activeController = null;
    for (TwillController controller : twillRunner.lookup(TaskWorkerManagerTwillApplication.NAME)) {
      // If more than one controller is detected, terminate the extras.
      if (activeController != null) {
        controller.terminate();
      } else {
        activeController = controller;
      }
    }

    if (activeController == null) {
      try {
        Path tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
            cConf.get(Constants.AppFabric.TEMP_DIR)).toPath();
        Files.createDirectories(tmpDir);

        Path runDir = Files.createTempDirectory(tmpDir, "task.worker.manager.launcher");
        try {
          // Unset the internal certificate path since the certificate is stored in cdap-security,
          // which is not mounted into the proxy pod.
          CConfiguration cConfCopy = CConfiguration.copy(cConf);
          cConfCopy.unset(Constants.Security.SSL.INTERNAL_CERT_PATH);
          // Instruct KubeMasterEnvironment in the proxy pod to discover task worker pod endpoints
          // directly via V1Endpoints rather than the ClusterIP.
          cConfCopy.set(Constants.TaskWorkerManager.ENDPOINTS_SERVICES, Constants.Service.TASK_WORKER);

          Path cConfPath = runDir.resolve("cConf.xml");
          try (Writer writer = Files.newBufferedWriter(cConfPath, StandardCharsets.UTF_8)) {
            cConfCopy.writeXml(writer);
          }
          Path hConfPath = runDir.resolve("hConf.xml");
          try (Writer writer = Files.newBufferedWriter(hConfPath, StandardCharsets.UTF_8)) {
            hConf.writeXml(writer);
          }

          // Always one instance: the lease table is in memory, so two proxies would lease the same
          // pod to two namespaces.
          ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
              .setVirtualCores(cConf.getInt(Constants.TaskWorkerManager.CONTAINER_CORES))
              .setMemory(cConf.getInt(Constants.TaskWorkerManager.CONTAINER_MEMORY_MB),
                  ResourceSpecification.SizeUnit.MEGA)
              .setInstances(1)
              .build();

          LOG.info("Starting TaskWorkerManager proxy");

          TwillPreparer twillPreparer = twillRunner.prepare(
              new TaskWorkerManagerTwillApplication(cConfPath.toUri(), hConfPath.toUri(), resourceSpec));

          Map<String, String> configMap = new HashMap<>();
          configMap.put(ProgramOptionConstants.RUNTIME_NAMESPACE,
              NamespaceId.SYSTEM.getNamespace());
          twillPreparer.withConfiguration(Collections.unmodifiableMap(configMap));

          // Same priority class as the task workers, since every task goes through the proxy.
          String priorityClass = cConf.get(Constants.TaskWorker.CONTAINER_PRIORITY_CLASS_NAME);
          if (priorityClass != null) {
            twillPreparer = twillPreparer.setSchedulerQueue(priorityClass);
          }

          // Recreate, not RollingUpdate, so an update never runs two proxies at once.
          if (twillPreparer instanceof ExtendedTwillPreparer) {
            twillPreparer = ((ExtendedTwillPreparer) twillPreparer).withRecreateStrategy();
          }

          // No SecurityContext: the proxy needs App Fabric's system service account to watch
          // Kubernetes endpoints.

          twillPreparer.setJVMOptions(TaskWorkerManagerTwillRunnable.class.getSimpleName(),
              cConf.get(Constants.TaskWorkerManager.CONTAINER_JVM_OPTS));

          activeController = twillPreparer.start(5, TimeUnit.MINUTES);
          activeController.onRunning(() -> deleteDir(runDir), Threads.SAME_THREAD_EXECUTOR);
          activeController.onTerminated(() -> deleteDir(runDir), Threads.SAME_THREAD_EXECUTOR);
        } catch (Exception e) {
          deleteDir(runDir);
          throw e;
        }
      } catch (Exception e) {
        LOG.warn("Failed to launch the task worker manager proxy, retrying in {} seconds",
            cConf.getInt(Constants.TaskWorkerManager.POOL_CHECK_INTERVAL), e);
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
