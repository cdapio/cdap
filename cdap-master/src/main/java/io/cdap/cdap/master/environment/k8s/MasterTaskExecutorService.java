/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.util.concurrent.AbstractScheduledService;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentTask;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A service for executing {@link MasterEnvironmentTask} periodically.
 */
final class MasterTaskExecutorService extends AbstractScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(MasterTaskExecutorService.class);

  private final MasterEnvironmentTask task;
  private final MasterEnvironmentContext context;

  private ScheduledExecutorService executor;
  private long delayMillis;

  MasterTaskExecutorService(MasterEnvironmentTask task, MasterEnvironmentContext context) {
    this.task = task;
    this.context = context;
  }

  @Override
  protected ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("master-env-executor"));
    return executor;
  }

  @Override
  protected void runOneIteration() {
    try {
      delayMillis = task.run(context);
      if (delayMillis < 0) {
        LOG.debug("Terminating scheduled task.");
        executor.shutdown();
      }
    } catch (Throwable t) {
      delayMillis = task.failureRetryDelay(t);
      LOG.warn("Exception raised from master environment task execution of task {}. Retrying in {} milliseconds",
               task, delayMillis, t);
    }
  }

  @Override
  protected Scheduler scheduler() {
    return new CustomScheduler() {
      @Override
      protected Schedule getNextSchedule() {
        return new Schedule(delayMillis, TimeUnit.MILLISECONDS);
      }
    };
  }

  @Override
  protected void shutDown() {
    if (executor != null) {
      executor.shutdown();
    }
  }
}
