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

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A scheduled service that periodically poll for new preview request and execute it.
 */
public class TaskWorkerService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerService.class);

  private CConfiguration cConf;
  private AtomicBoolean cancelRequested;

  @Inject
  TaskWorkerService(CConfiguration cConf) {
    this.cConf = cConf;
    this.cancelRequested = new AtomicBoolean(false);
  }

  @Override
  protected void triggerShutdown() {
    cancelRequested.set(true);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting task worker service");
  }

  @Override
  protected void run() throws Exception {
    while(!cancelRequested.get()) {
      LOG.debug("TaskWorkerService::run()");
      Thread.sleep(10000);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Task worker service shutdown completed");
  }
}
