package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.weave.common.Threads;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.ThreadPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Executor based ThreadPool used in quartz scheduler.
 */
public final class ExecutorThreadPool implements ThreadPool {

  private static final int MAX_THREAD_POOL_SIZE = 500;
  private final ExecutorService executor;

  public ExecutorThreadPool() {
    executor = Executors.newFixedThreadPool(MAX_THREAD_POOL_SIZE,
                                            Threads.createDaemonThreadFactory("scheduler-thread-pool-%d"));
  }

  @Override
  public boolean runInThread(Runnable runnable) {
    executor.execute(runnable);
    return true;
  }

  @Override
  public int blockForAvailableThreads() {
    //Always accept new work. Additional runnables will be in the executor queue.
    return MAX_THREAD_POOL_SIZE;
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
    return MAX_THREAD_POOL_SIZE;
  }


  @Override
  public void setInstanceId(String schedInstId) {
    //no-op
  }

  @Override
  public void setInstanceName(String schedName) {
    //noop
  }
}
