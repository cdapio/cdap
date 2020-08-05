/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.ThreadPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Executor based ThreadPool used in quartz scheduler.
 */
public final class ExecutorThreadPool implements ThreadPool {

  private final int maxThreadPoolSize;
  private final ExecutorService executor;

  public ExecutorThreadPool(int maxThreadPoolSize) {
    this.maxThreadPoolSize = maxThreadPoolSize;
    executor = createThreadPoolExecutor();
  }

  @Override
  public boolean runInThread(Runnable runnable) {
    executor.execute(runnable);
    return true;
  }

  @Override
  public int blockForAvailableThreads() {
    //Always accept new work. Additional runnables will be in the executor queue.
    return maxThreadPoolSize;
  }


  @Override
  public void initialize() throws SchedulerConfigException {
  }

  @Override
  public void shutdown(boolean waitForJobsToComplete) {
    executor.shutdown();
  }


  @Override
  public int getPoolSize() {
    return maxThreadPoolSize;
  }


  @Override
  public void setInstanceId(String schedInstId) {
    //no-op
  }

  @Override
  public void setInstanceName(String schedName) {
    //noop
  }

  private ThreadPoolExecutor createThreadPoolExecutor() {
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
      .setNameFormat("scheduler-executor-%d")
      .setDaemon(true)
      .build();

    return new ThreadPoolExecutor(0, maxThreadPoolSize, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(),
                                  threadFactory, new ThreadPoolExecutor.AbortPolicy());
  }
}
