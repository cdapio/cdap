package com.continuuity.internal.app.runtime.service;

import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.logging.context.ServiceRunnableLoggingContext;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.common.Threads;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Driver for InMemory service runnable
 */
public class InMemoryRunnableDriver extends AbstractExecutionThreadService {
  private TwillRunnable runnable;
  private TwillContext context;
  private ServiceRunnableLoggingContext loggingContext;

  private Thread runnerThread;
  private ExecutorService processExecutor;


  public InMemoryRunnableDriver(TwillRunnable runnable, TwillContext context,
                                ServiceRunnableLoggingContext loggingContext) {
    this.runnable = runnable;
    this.context = context;
    this.loggingContext = loggingContext;
  }

  @Override
  protected void startUp() throws Exception {
    runnerThread = Thread.currentThread();
    processExecutor = Executors.newSingleThreadExecutor(
      Threads.createDaemonThreadFactory(getServiceName() + "-executor"));
  }

  @Override
  protected void shutDown() throws Exception {
    processExecutor.shutdown();
  }
  @Override
  protected void run() throws Exception {
    LoggingContextAccessor.setLoggingContext(loggingContext);
    runnable.run();
    // they run an while loop based on isRunning here in case of flowlet driver, if not this will exit immediately ?
  }
}
