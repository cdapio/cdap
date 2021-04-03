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

package io.cdap.cdap.internal.app.dispatcher;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.master.spi.twill.StatefulDisk;
import io.cdap.cdap.master.spi.twill.StatefulTwillPreparer;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class TaskDispatcherServer extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(TaskDispatcherServer.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final SConfiguration sConf;

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancelDiscovery;
  private InetSocketAddress bindAddress;
  private final TwillRunner twillRunner;
  private TwillController controller;

  @Inject
  public TaskDispatcherServer(CConfiguration cConf, SConfiguration sConf, Configuration hConf,
                              DiscoveryService discoveryService,
                              @Named(Constants.TaskDispatcher.HANDLER_NAME) Set<HttpHandler> handlers,
                              TwillRunner twillRunner) {
    this.discoveryService = discoveryService;
    this.cConf = cConf;
    this.hConf = hConf;
    this.sConf = sConf;
    this.twillRunner = twillRunner;

    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.TASK_DISPATCHER)
      .setHost(cConf.get(Constants.TaskDispatcher.ADDRESS))
      .setPort(cConf.getInt(Constants.TaskDispatcher.PORT))
      .setExecThreadPoolSize(cConf.getInt(Constants.TaskDispatcher.EXEC_THREADS))
      .setBossThreadPoolSize(cConf.getInt(Constants.TaskDispatcher.BOSS_THREADS))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.TaskDispatcher.WORKER_THREADS))
      .setHttpHandlers(handlers);

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }
    this.httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting TaskDispatcher server");

    launchHttpServer();
    launchTaskWorkerPool();

    LOG.info("Starting TaskDispatcher server has completed");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down TaskDispatcher server");
    try {
      cancelDiscovery.cancel();
    } finally {
      httpService.stop();
    }
    LOG.info("Shutting down TaskDispatcher has completed");
  }

  private void launchHttpServer() throws Exception {
    LOG.info("Starting TaskDispatcher http server");
    httpService.start();
    bindAddress = httpService.getBindAddress();
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.TASK_DISPATCHER, httpService)));
    LOG.info("Starting TaskDispatcher http server has completed on {}", httpService.getBindAddress());
  }

  private void launchTaskWorkerPool() {
    LOG.info("Starting TaskDispatcher worker pool");
    TwillController activeController = null;
    for (TwillController controller : twillRunner.lookup(TaskWorkerTwillApplication.NAME)) {
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

        Path runDir = Files.createTempDirectory(tmpDir, "task.dispatcher");
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
            .setVirtualCores(cConf.getInt(Constants.TaskDispatcher.CONTAINER_CORES))
            .setMemory(cConf.getInt(Constants.TaskDispatcher.CONTAINER_MEMORY_MB), ResourceSpecification.SizeUnit.MEGA)
            .setInstances(cConf.getInt(Constants.TaskDispatcher.CONTAINER_COUNT))
            .build();

          LOG.info("Starting TaskDispatcher worker pool with {} instances", resourceSpec.getInstances());

          TwillPreparer twillPreparer = twillRunner.prepare(new TaskWorkerTwillApplication(cConfPath.toUri(),
                                                                                           hConfPath.toUri(),
                                                                                           resourceSpec));
          String priorityClass = cConf.get(Constants.Preview.CONTAINER_PRIORITY_CLASS_NAME);
          if (priorityClass != null) {
            twillPreparer = twillPreparer.setSchedulerQueue(priorityClass);
          }

          if (twillPreparer instanceof StatefulTwillPreparer) {
            int diskSize = cConf.getInt(Constants.Preview.CONTAINER_DISK_SIZE_GB);
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
        LOG.warn("Failed to launch TaskDispatcher worker pool. It will NOT be retried", e);
      }
    }
    controller = activeController;
    LOG.info("Starting TaskDispatcher worker pool has completed");
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

  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }
}
