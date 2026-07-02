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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.http.NettyHttpService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Guice-managed service that runs the Centralized Task Manager HTTP Server.
 */
public class TaskManagerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(TaskManagerService.class);
  private final NettyHttpService httpService;

  @Inject
  TaskManagerService(CConfiguration cConf,
                     CommonNettyHttpServiceFactory commonNettyHttpServiceFactory,
                     TaskManagerHttpHandler taskManagerHttpHandler) {
    
    int port = cConf.getInt("task.manager.port", 11025);
    String address = cConf.get("task.manager.address", "0.0.0.0");

    LOG.info("sidhdirenge - Initializing TaskManagerService on {}:{}", address, port);

    this.httpService = commonNettyHttpServiceFactory.builder("task-manager", false)
        .setHost(address)
        .setPort(port)
        .setHttpHandlers(Collections.singletonList(taskManagerHttpHandler))
        .build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("sidhdirenge - Starting TaskManagerService HTTP server...");
    httpService.start();
    LOG.info("sidhdirenge - TaskManagerService HTTP server started successfully at {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("sidhdirenge - Stopping TaskManagerService HTTP server...");
    httpService.stop();
    LOG.info("sidhdirenge - TaskManagerService HTTP server stopped.");
  }
}
