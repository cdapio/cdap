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

package io.cdap.cdap.common.internal.remote;

import io.cdap.http.NettyHttpService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for running the standalone Task Manager Service.
 */
public class TaskManagerMain {

  private static final Logger LOG = LoggerFactory.getLogger(TaskManagerMain.class);
  private static final int PORT = 11025;

  public static void main(String[] args) throws Exception {
    LOG.info("Starting standalone CDAP Task Manager Service on port {}...", PORT);

    NettyHttpService httpService = NettyHttpService.builder("task-manager")
        .setHttpHandlers(new TaskManagerHttpHandler())
        .setPort(PORT)
        .build();

    httpService.start();
    LOG.info("CDAP Task Manager Service started successfully.");

    // Keep the service running
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        LOG.info("Stopping CDAP Task Manager Service...");
        httpService.stop();
        LOG.info("CDAP Task Manager Service stopped.");
      } catch (Exception e) {
        LOG.error("Error stopping Task Manager Service", e);
      }
    }));

    Thread.currentThread().join();
  }
}
