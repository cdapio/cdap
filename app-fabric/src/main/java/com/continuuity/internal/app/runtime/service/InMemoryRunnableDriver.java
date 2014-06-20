package com.continuuity.internal.app.runtime.service;

import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.logging.context.UserServiceLoggingContext;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Driver for InMemory service runnable
 */
public class InMemoryRunnableDriver extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryRunnableDriver.class);
  private TwillRunnable runnable;
  private TwillContext context;
  private UserServiceLoggingContext loggingContext;

  private ExecutorService processExecutor;

  public InMemoryRunnableDriver(TwillRunnable runnable, TwillContext context,
                                UserServiceLoggingContext loggingContext) {
    this.runnable = runnable;
    this.context = context;
    this.loggingContext = loggingContext;
  }

  @Override
  protected void startUp() throws Exception {
    processExecutor = Executors.newSingleThreadExecutor
      (Threads.createDaemonThreadFactory(getServiceName() + "-executor"));
  }

  @Override
  protected void shutDown() throws Exception {
    runnable.stop();
    processExecutor.shutdown();
    LOG.info("Runnable {} Stopped", context.getSpecification().getName());
  }

  @Override
  protected void run() throws Exception {
    LoggingContextAccessor.setLoggingContext(loggingContext);
    Future<?> processFuture = processExecutor.submit(runnable);
    while (!processFuture.isDone()) {
      try {
        // Wait uninterruptibly so that stop() won't kill the executing context
        // We need a timeout so that in case it takes too long to complete, we have chance to force quit it if
        // it is in shutdown sequence.
        Uninterruptibles.getUninterruptibly(processFuture, 5, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        LOG.error("Unexpected execution exception.", e);
      } catch (TimeoutException e) {
        // If in shutdown sequence, cancel the task by interrupting it.
        // Otherwise, just keep waiting until it completes
        if (!isRunning()) {
          LOG.info("Runnable {} took longer than 5 seconds to quit. Force quitting.", context.getRunId());
          processFuture.cancel(true);
        }
      }
    }
  }
}
