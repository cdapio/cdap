package com.continuuity.internal.app.runtime.schedule;

import com.google.common.base.Throwables;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.ThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Executor based ThreadPool used in quartz scheduler.
 */

public final class ExecutorThreadPool implements ThreadPool {

  private static final int CORE_POOL_SIZE = 1;
  private static final int MAX_THREAD_POOL_SIZE = 500;
  private static final long KEEP_ALIVE_TIME = 5;

  private final ThreadPoolExecutor executor;

  public ExecutorThreadPool() {
    executor = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_THREAD_POOL_SIZE,
                                      KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                                      new SynchronousQueue<Runnable>());
  }

  @Override
  public boolean runInThread(Runnable runnable) {
    executor.execute(runnable);
    return true;
  }

  @Override
  public int blockForAvailableThreads() {

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
    return executor.getPoolSize();
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
