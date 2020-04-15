/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.common.service;

import com.google.common.util.concurrent.AbstractScheduledService;
import io.cdap.cdap.api.retry.RetriesExhaustedException;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A {@link AbstractScheduledService} that runs task periodically and with retry logic upon task failure.
 */
public abstract class AbstractRetryableScheduledService extends AbstractScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractRetryableScheduledService.class);
  private static final Logger OUTAGE_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(TimeUnit.SECONDS.toMillis(30)));

  private final RetryStrategy retryStrategy;

  private long delayMillis;
  private int failureCount;
  private long nonFailureStartTime;
  private ScheduledExecutorService executor;

  /**
   * Constructor.
   *
   * @param retryStrategy the {@link RetryStrategy} for determining how to retry when there is exception raised
   */
  protected AbstractRetryableScheduledService(RetryStrategy retryStrategy) {
    this.retryStrategy = retryStrategy;
    addListener(new ServiceListenerAdapter() {
      @Override
      public void failed(State from, Throwable failure) {
        LOG.error("Scheduled service {} terminated due to failure", getServiceName(), failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  /**
   * Returns the {@link RetryStrategy} used by this class.
   *
   * @return the {@link RetryStrategy} provided to the constructor
   */
  protected RetryStrategy getRetryStrategy() {
    return retryStrategy;
  }

  /**
   * Runs the task in one scheduled iteration.
   *
   * @return the number of milliseconds to delay until the next call to this method
   * @throws Exception if the task failed
   */
  protected abstract long runTask() throws Exception;

  /**
   * Determines if retry on the given {@link Exception}.
   *
   * @param ex the exception raised by the {@link #runTask()} call.
   * @return {@code true} to retry in the next iteration; otherwise {@code false} to fail and terminate this service
   *         with the given exception.
   */
  protected boolean shouldRetry(Exception ex) {
    return true;
  }

  /**
   * Performs startup task. This method will be called from the executor returned by the {@link #executor()} method.
   * By default this method does nothing.
   *
   * @throws Exception if startup of this service failed
   */
  protected void doStartUp() throws Exception {
    // No-op
  }

  /**
   * Performs shutdown task. This method will be called from the executor returned by the {@link #executor()} method.
   * By default this method does nothing.
   *
   * @throws Exception if shutdown of this service failed
   */
  protected void doShutdown() throws Exception {
    // No-op
  }

  /**
   * Returns the name of this service.
   */
  protected String getServiceName() {
    return getClass().getSimpleName();
  }

  @Override
  protected ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory(getServiceName()));
    return executor;
  }

  @Override
  protected final void startUp() throws Exception {
    doStartUp();
  }

  @Override
  protected final void shutDown() throws Exception {
    try {
      doShutdown();
    } finally {
      if (executor != null) {
        executor.shutdown();
      }
    }
  }

  /**
   * Called when the RetryStrategy used by this class returns a negative number, indicating to abort the operation.
   * This method allows a subclass to terminate the service (by throwing exception), or to continue retrying by
   * avoiding throwing of the exception, which resets the retry count.
   *
   * @param e the exception which exhausted retries
   * @return the number of milliseconds to delay until the next iteration of the {@link #runTask} method.
   * @throws Exception if decided to terminate the retry
   */
  protected long handleRetriesExhausted(Exception e) throws Exception {
    throw e;
  }

  @Override
  protected final void runOneIteration() throws Exception {
    try {
      try {
        if (nonFailureStartTime == 0L) {
          nonFailureStartTime = System.currentTimeMillis();
        }

        delayMillis = runTask();
        nonFailureStartTime = 0L;
        failureCount = 0;
      } catch (Throwable t) {
        logTaskFailure(t);
        if (!(t instanceof Exception)) {
          throw t;
        }
        Exception e = (Exception) t;
        if (!shouldRetry(e)) {
          throw t;
        }

        long delayMillis = retryStrategy.nextRetry(++failureCount, nonFailureStartTime);
        if (delayMillis < 0) {
          t.addSuppressed(
            new RetriesExhaustedException(String.format("Retries exhausted after %d failures and %d ms.",
                                                        failureCount,
                                                        System.currentTimeMillis() - nonFailureStartTime)));
          delayMillis = Math.max(0L, handleRetriesExhausted(e));
          nonFailureStartTime = 0L;
          failureCount = 0;
        }
        this.delayMillis = delayMillis;
      }
    } catch (Throwable t) {
      LOG.error("Aborting service {} due to non-retryable error", getServiceName(), t);
      throw t;
    }
  }

  @Override
  protected final Scheduler scheduler() {
    return new CustomScheduler() {
      @Override
      protected Schedule getNextSchedule() {
        return new Schedule(delayMillis, TimeUnit.MILLISECONDS);
      }
    };
  }

  /**
   * Logs an exception raised by {@link #runTask()}.
   */
  protected void logTaskFailure(Throwable t) {
    OUTAGE_LOG.warn("Failed to execute task for scheduled service {}", getServiceName(), t);
  }
}
