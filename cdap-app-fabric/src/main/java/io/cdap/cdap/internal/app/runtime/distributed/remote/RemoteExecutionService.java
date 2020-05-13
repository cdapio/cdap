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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A service that periodically checks if the program is still running.
 */
class RemoteExecutionService extends AbstractRetryableScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionService.class);
  // Skip the first error log, and at most log once per 30 seconds.
  // This helps debugging errors that persist more than 30 seconds.
  private static final Logger OUTAGE_LOGGER = Loggers.sampling(LOG,
                                                               LogSamplers.limitRate(TimeUnit.SECONDS.toMillis(30)));


  private final ProgramRunId programRunId;
  private final ScheduledExecutorService scheduler;
  private final RemoteProcessController processController;
  private final ProgramStateWriter programStateWriter;
  private final long pollTimeMillis;
  private long nextCheckRunningMillis;
  private int notRunningCount;

  RemoteExecutionService(CConfiguration cConf, ProgramRunId programRunId, ScheduledExecutorService scheduler,
                         RemoteProcessController processController, ProgramStateWriter programStateWriter) {
    super(RetryStrategies.fromConfiguration(cConf, "system.runtime.monitor."));
    this.programRunId = programRunId;
    this.scheduler = scheduler;
    this.processController = processController;
    this.programStateWriter = programStateWriter;
    this.pollTimeMillis = cConf.getLong(Constants.RuntimeMonitor.POLL_TIME_MS);
  }

  /**
   * Performs periodic task. By default is a no-op
   */
  void doRunTask() throws Exception {
    // no-op
  }

  @Override
  protected void doShutdown() {
    // Make sure the remote process is gone.
    // Give 10 seconds for the remote process to shutdown. After 10 seconds, issue a kill.
    RetryStrategy retryStrategy = RetryStrategies.timeLimit(10, TimeUnit.SECONDS, getRetryStrategy());
    try {
      Retries.runWithRetries(processController::isRunning, retryStrategy, Exception.class::isInstance);
    } catch (Exception e) {
      LOG.info("Force termination of remote process for program run {}", getProgramRunId());
      try {
        processController.kill();
      } catch (Exception ex) {
        LOG.warn("Failed to force terminate remote process for program run {}", getProgramRunId());
      }
    }
  }

  final ProgramRunId getProgramRunId() {
    return programRunId;
  }

  @Override
  protected final long runTask() throws Exception {
    doRunTask();

    long now = System.currentTimeMillis();
    if (now >= nextCheckRunningMillis) {
      // Periodically check the liveness. We allow one non-running to pass to accommodate for the delay
      // of TMS messages relay from the remote runtime back to CDAP.
      if (!processController.isRunning() && ++notRunningCount > 1) {
        LOG.debug("Program {} is not running", programRunId);
        programStateWriter.error(programRunId, new IllegalStateException("Program terminated " + programRunId));
        stop();
        return 0;
      }
      nextCheckRunningMillis = now + pollTimeMillis * 10;
    }
    return pollTimeMillis;
  }

  @Override
  protected ScheduledExecutorService executor() {
    return scheduler;
  }

  @Override
  protected boolean shouldRetry(Exception ex) {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(programRunId, null);
    Cancellable cancellable = LoggingContextAccessor.setLoggingContext(loggingContext);
    try {
      OUTAGE_LOGGER.warn("Exception raised when monitoring program run {}", programRunId, ex);
      try {
        // If the program is not running, abort retry
        if (!processController.isRunning()) {
          LOG.debug("Program {} is not running", programRunId, ex);

          // Always emit an error state. If this is indeed a normal stop, there should be a completion state
          // in the TMS before this one, in which this one will get ignored by the
          programStateWriter.error(programRunId,
                                   new IllegalStateException("Program terminated " + programRunId, ex));
          return false;
        }
      } catch (Exception e) {
        OUTAGE_LOGGER.warn("Failed to check if the remote process is still running for program {}", programRunId, e);
      }
      return true;
    } finally {
      cancellable.cancel();
    }
  }

  @Override
  protected long handleRetriesExhausted(Exception e) throws Exception {
    // kill the remote process and record a program run error
    // log this in the program context so it shows up in program logs
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(programRunId, null);
    Cancellable cancellable = LoggingContextAccessor.setLoggingContext(loggingContext);
    try {
      LOG.error("Failed to monitor the remote process and exhausted retries. Terminating the program {}",
                programRunId, e);
      try {
        processController.kill();
      } catch (Exception e1) {
        LOG.warn("Failed to kill the remote process for program {}. "
                   + "The remote process may need to be killed manually.", programRunId, e1);
      }

      // If failed to fetch messages and the remote process is not running, emit a failure program state and
      // terminates the monitor
      programStateWriter.error(programRunId,
                               new IllegalStateException("Program runtime terminated due to too many failures. " +
                                                           "Please inspect logs for root cause.", e));
      throw e;
    } finally {
      cancellable.cancel();
    }
  }

  @Override
  protected String getServiceName() {
    return "runtime-service-" + programRunId.getRun();
  }
}
