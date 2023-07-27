/*
 * Copyright © 2022 Cask Data, Inc.
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
import io.cdap.cdap.common.conf.Constants.Twill.Security;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.master.spi.autoscaler.AutoscalingConfig;
import io.cdap.cdap.master.spi.twill.AutoscalingConfigTwillPreparer;
import io.cdap.cdap.master.spi.twill.SecretDisk;
import io.cdap.cdap.master.spi.twill.SecureTwillPreparer;
import io.cdap.cdap.master.spi.twill.SecurityContext;
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

public class SystemWorkerServiceLauncher extends AbstractScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(SystemWorkerServiceLauncher.class);
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final SConfiguration sConf;

  private final TwillRunner twillRunner;
  private TwillController twillController;

  private ScheduledExecutorService executor;

  @Inject
  public SystemWorkerServiceLauncher(CConfiguration cConf, Configuration hConf,
      SConfiguration sConf,
      TwillRunner twillRunner) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.sConf = sConf;
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

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("system-worker-service-launcher-scheduler"));
    return executor;
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
          Path sConfPath = runDir.resolve("sConf.xml");
          try (Writer writer = Files.newBufferedWriter(sConfPath, StandardCharsets.UTF_8)) {
            sConf.writeXml(writer);
          }

          ResourceSpecification systemResourceSpec = ResourceSpecification.Builder.with()
              .setVirtualCores(cConf.getInt(Constants.SystemWorker.CONTAINER_CORES))
              .setMemory(cConf.getInt(Constants.SystemWorker.CONTAINER_MEMORY_MB),
                  ResourceSpecification.SizeUnit.MEGA)
              .setInstances(cConf.getInt(Constants.SystemWorker.CONTAINER_COUNT))
              .build();

          LOG.info("Starting SystemWorker pool with {} instances",
              systemResourceSpec.getInstances());

          TwillPreparer twillPreparer = twillRunner.prepare(
              new SystemWorkerTwillApplication(cConfPath.toUri(), hConfPath.toUri(),
                  sConfPath.toUri(),
                  systemResourceSpec));

          Map<String, String> configMap = new HashMap<>();
          configMap.put(ProgramOptionConstants.RUNTIME_NAMESPACE,
              NamespaceId.SYSTEM.getNamespace());
          twillPreparer.withConfiguration(Collections.unmodifiableMap(configMap));

          String priorityClass = cConf.get(Constants.TaskWorker.CONTAINER_PRIORITY_CLASS_NAME);
          if (priorityClass != null) {
            twillPreparer = twillPreparer.setSchedulerQueue(priorityClass);
          }

          if(twillPreparer instanceof AutoscalingConfigTwillPreparer){
            AutoscalingConfig autoscalingConfig = new AutoscalingConfig(
                    cConf.get(Constants.SystemWorker.AUTOSCALER_METRIC_NAME),
                    cConf.getInt(Constants.SystemWorker.MIN_REPLICA_COUNT),
                    cConf.getInt(Constants.SystemWorker.MAX_REPLICA_COUNT),
                    cConf.get(Constants.SystemWorker.DESIRED_AVERAGE_METRIC_VALUE),
                    cConf.getInt(Constants.SystemWorker.STABILIZATION_WINDOW_TIME),
                    cConf.getInt(Constants.SystemWorker.PERIOD_TIME),
                    cConf.getInt(Constants.SystemWorker.POD_UPDATE_COUNT));
            ((AutoscalingConfigTwillPreparer) twillPreparer).getAutoscalingConfig(autoscalingConfig);
          }

          if (twillPreparer instanceof SecureTwillPreparer) {
            SecurityContext securityContext = createSecurityContext();
            twillPreparer = ((SecureTwillPreparer) twillPreparer)
                .withSecurityContext(SystemWorkerTwillRunnable.class.getSimpleName(),
                    securityContext);
            String secretName = cConf.get(Security.MASTER_SECRET_DISK_NAME);
            String secretPath = cConf.get(Security.MASTER_SECRET_DISK_PATH);
            twillPreparer = ((SecureTwillPreparer) twillPreparer)
                .withSecretDisk(SystemWorkerTwillRunnable.class.getSimpleName(),
                    new SecretDisk(secretName,
                        secretPath));
          }

          // Set JVM options for system worker
          twillPreparer.setJVMOptions(SystemWorkerTwillRunnable.class.getSimpleName(),
              cConf.get(Constants.SystemWorker.CONTAINER_JVM_OPTS));

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

  private SecurityContext createSecurityContext() {
    SecurityContext.Builder builder = new SecurityContext.Builder();
    String twillUserIdentity = cConf.get(Security.IDENTITY_SYSTEM);
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


}
