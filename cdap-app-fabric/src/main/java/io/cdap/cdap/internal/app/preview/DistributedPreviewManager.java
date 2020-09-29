/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.master.spi.twill.StatefulDisk;
import io.cdap.cdap.master.spi.twill.StatefulTwillPreparer;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.security.authorization.AuthorizerInstantiator;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
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
 * A {@link PreviewManager} to be used in distributed mode where it launches preview runner in separate processes.
 */
public class DistributedPreviewManager extends DefaultPreviewManager implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedPreviewManager.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final TwillRunner twillRunner;
  private ScheduledExecutorService scheduler;
  private TwillController controller;

  @Inject
  DistributedPreviewManager(CConfiguration cConf, Configuration hConf, DiscoveryService discoveryService,
                            @Named(DataSetsModules.BASE_DATASET_FRAMEWORK) DatasetFramework datasetFramework,
                            TransactionSystemClient transactionSystemClient,
                            AuthorizerInstantiator authorizerInstantiator, AuthorizationEnforcer authorizationEnforcer,
                            @Named(PreviewConfigModule.PREVIEW_LEVEL_DB) LevelDBTableService previewLevelDBTableService,
                            @Named(PreviewConfigModule.PREVIEW_CCONF) CConfiguration previewCConf,
                            @Named(PreviewConfigModule.PREVIEW_HCONF) Configuration previewHConf,
                            @Named(PreviewConfigModule.PREVIEW_SCONF) SConfiguration previewSConf,
                            PreviewRequestQueue previewRequestQueue, PreviewStore previewStore,
                            PreviewRunStopper previewRunStopper, MessagingService messagingService,
                            MetricsCollectionService metricsCollectionService,
                            PreviewDataCleanupService previewDataCleanupService,
                            TwillRunner twillRunner) {
    super(discoveryService, datasetFramework, transactionSystemClient, authorizerInstantiator, authorizationEnforcer,
          previewLevelDBTableService, previewCConf, previewHConf, previewSConf, previewRequestQueue, previewStore,
          previewRunStopper, messagingService, previewDataCleanupService, metricsCollectionService);

    this.cConf = cConf;
    this.hConf = hConf;
    this.twillRunner = twillRunner;
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();
    scheduler = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("preview-manager"));
    scheduler.scheduleWithFixedDelay(this, 2, 5, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    super.shutDown();
    if (scheduler != null) {
      Future<?> future = scheduler.submit(() -> {
        try {
          if (controller != null) {
            controller.terminate();
            controller.awaitTerminated(10, TimeUnit.SECONDS);
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
          Path cConfPath = runDir.resolve("cConf.xml");
          try (Writer writer = Files.newBufferedWriter(cConfPath, StandardCharsets.UTF_8)) {
            cConf.writeXml(writer);
          }
          Path hConfPath = runDir.resolve("hConf.xml");
          try (Writer writer = Files.newBufferedWriter(hConfPath, StandardCharsets.UTF_8)) {
            hConf.writeXml(writer);
          }

          ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
            .setVirtualCores(cConf.getInt(Constants.Preview.CONTAINER_CORES))
            .setMemory(cConf.getInt(Constants.Preview.CONTAINER_MEMORY_MB), ResourceSpecification.SizeUnit.MEGA)
            .setInstances(cConf.getInt(Constants.Preview.CONTAINER_COUNT))
            .build();

          LOG.info("Starting preview runners with {} instances", resourceSpec.getInstances());

          TwillPreparer twillPreparer = twillRunner.prepare(new PreviewRunnerTwillApplication(cConfPath.toUri(),
                                                                                              hConfPath.toUri(),
                                                                                              resourceSpec));
          String priorityClass = cConf.get(Constants.Preview.CONTAINER_PRIORITY_CLASS_NAME);
          if (priorityClass != null) {
            twillPreparer = twillPreparer.setSchedulerQueue(priorityClass);
          }

          if (twillPreparer instanceof StatefulTwillPreparer) {
            int diskSize = cConf.getInt(Constants.Preview.CONTAINER_DISK_SIZE_GB);
            twillPreparer = ((StatefulTwillPreparer) twillPreparer)
              .withStatefulRunnable(PreviewRunnerTwillRunnable.class.getSimpleName(), false,
                                    new StatefulDisk("preview-runner-data", diskSize, "/data"));
          }

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
      LOG.warn("Failed to cleanup directory {}", dir, e);
    }
  }
}
