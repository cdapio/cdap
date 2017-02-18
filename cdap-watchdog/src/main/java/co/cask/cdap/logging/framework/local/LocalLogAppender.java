/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.framework.local;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.framework.LocalAppenderContext;
import co.cask.cdap.logging.framework.LogPipelineLoader;
import co.cask.cdap.logging.framework.LogPipelineSpecification;
import co.cask.cdap.logging.pipeline.LogProcessorPipelineContext;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@link LogAppender} used in local mode.
 */
public class LocalLogAppender extends LogAppender {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LocalLogAppender.class);
  private static final int EVENT_QUEUE_SIZE = 256;

  private final CConfiguration cConf;
  private final DatasetFramework datasetFramework;
  private final TransactionSystemClient txClient;
  private final LocationFactory locationFactory;
  private final MetricsCollectionService metricsCollectionService;
  private final List<LocalLogProcessorPipeline> pipelines;
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean stopped = new AtomicBoolean();

  @Inject
  LocalLogAppender(CConfiguration cConf, DatasetFramework datasetFramework,
                   TransactionSystemClient txClient,
                   LocationFactory locationFactory,
                   MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    this.datasetFramework = datasetFramework;
    this.txClient = txClient;
    this.locationFactory = locationFactory;
    this.metricsCollectionService = metricsCollectionService;
    this.pipelines = new ArrayList<>();
    setName(getClass().getName());
  }

  @Override
  public void start() {
    if (!started.compareAndSet(false, true)) {
      return;
    }

    // Load and starts all configured log processing pipelines
    LogPipelineLoader pipelineLoader = new LogPipelineLoader(cConf);
    Map<String, LogPipelineSpecification<LoggerContext>> specs = pipelineLoader.load(new Provider<LoggerContext>() {
      @Override
      public LoggerContext get() {
        return new LocalAppenderContext(datasetFramework, txClient, locationFactory, metricsCollectionService);
      }
    });

    // Use the event delay as the sync interval
    long syncIntervalMillis = cConf.getLong(Constants.Logging.PIPELINE_EVENT_DELAY_MS);

    for (LogPipelineSpecification<LoggerContext> spec : specs.values()) {
      LogProcessorPipelineContext context = new LogProcessorPipelineContext(cConf, spec.getName(), spec.getContext());
      LocalLogProcessorPipeline pipeline = new LocalLogProcessorPipeline(context, syncIntervalMillis);
      pipeline.startAndWait();
      pipelines.add(pipeline);
    }

    super.start();
  }

  @Override
  public void stop() {
    if (!stopped.compareAndSet(false, true)) {
      return;
    }
    // Stop all pipelines
    super.stop();
    for (LocalLogProcessorPipeline pipeline : pipelines) {
      try {
        pipeline.stopAndWait();
      } catch (Throwable t) {
        LOG.error("Exception raised when stopping log processing pipeline {}", pipeline.getName(), t);
      }
    }
  }

  @Override
  protected void appendEvent(LogMessage logMessage) {
    for (LocalLogProcessorPipeline pipeline : pipelines) {
      pipeline.append(logMessage);
    }
  }

  /**
   * The log processing pipeline for writing logs to configured logger context
   */
  private final class LocalLogProcessorPipeline extends AbstractExecutionThreadService {

    private final LogProcessorPipelineContext context;
    private final long syncIntervalMillis;
    private final BlockingQueue<ILoggingEvent> eventQueue;
    private Thread runThread;
    private long lastSyncTime;

    private LocalLogProcessorPipeline(LogProcessorPipelineContext context, long syncIntervalMillis) {
      this.context = context;
      this.syncIntervalMillis = syncIntervalMillis;
      this.eventQueue = new ArrayBlockingQueue<>(EVENT_QUEUE_SIZE);
    }

    /**
     * Returns name of the pipeline.
     */
    String getName() {
      return context.getName();
    }

    @Override
    protected void startUp() throws Exception {
      LOG.info("Starting log processing pipeline {}", getName());
      runThread = Thread.currentThread();
      context.start();
      LOG.info("Log processing pipeline {} started", getName());
    }

    @Override
    protected void shutDown() throws Exception {
      LOG.info("Stopping log processing pipeline {}", getName());
      // Write all pending events out
      for (ILoggingEvent event : eventQueue) {
        context.getEffectiveLogger(event.getLoggerName()).callAppenders(event);
      }
      context.stop();
      LOG.info("Log processing pipeline {}", getName());
    }

    @Override
    protected void triggerShutdown() {
      if (runThread != null) {
        runThread.interrupt();
      }
    }

    @Override
    protected void run() {
      try {
        ILoggingEvent event = eventQueue.take();

        while (isRunning()) {
          callAppenders(event);
          long pollTimeout = syncIntervalMillis - (event.getTimeStamp() - lastSyncTime);

          // If it is not time to sync yet, poll for next event until the next sync time.
          // Otherwise just assign `null` to event to trigger sync immediately
          event = (pollTimeout > 0) ? eventQueue.poll(pollTimeout, TimeUnit.MILLISECONDS) : null;
          if (event == null) {
            sync(System.currentTimeMillis());
            // After sync'ed everything, block until there is more event
            event = eventQueue.take();
          }
        }

        // If event is not null, it means this pipeline stopped in between
        // the event was dequeue and before callAppenders.
        // We need to append this event before returning.
        if (event != null) {
          callAppenders(event);
        }
      } catch (InterruptedException e) {
        // Just ignore it. Not resetting the interrupt flag so that shutdown can operate without interruption.
      }
    }

    /**
     * Appends the given {@link ILoggingEvent} to the pipeline.
     */
    void append(ILoggingEvent event) {
      if (!isRunning()) {
        return;
      }

      Logger logger = context.getEffectiveLogger(event.getLoggerName());
      if (event.getLevel().isGreaterOrEqual(logger.getEffectiveLevel())) {
        try {
          // This will block until the queue has free space.
          eventQueue.put(event);
        } catch (InterruptedException e) {
          // Should never happen. Just ignore the exception and reset the flag
          Thread.currentThread().interrupt();
        }
      }
    }

    private void callAppenders(ILoggingEvent event) {
      Logger logger = context.getEffectiveLogger(event.getLoggerName());
      try {
        logger.callAppenders(event);
      } catch (Throwable t) {
        addError("Exception raised when appending to logger " + logger.getName() +
                   " with message " + event.getFormattedMessage(), t);
      }
    }

    private void sync(long now) {
      try {
        context.sync();
        lastSyncTime = now;
      } catch (IOException e) {
        LOG.error("Exception raised when syncing log processing pipeline {}", getName(), e);
      }
    }
  }
}
