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

package co.cask.cdap.logging.logbuffer;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.logging.meta.CheckpointManager;
import co.cask.cdap.logging.pipeline.logbuffer.LogBufferProcessorPipeline;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Log buffer recovery service which recovers logs upon log saver restart and sends them to log buffer pipeline for
 * further processing.
 */
class LogBufferRecoveryService extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(LogBufferRecoveryService.class);
  // For outage, only log once per 60 seconds per message.
  private static final Logger OUTAGE_LOG =
    Loggers.sampling(LOG, LogSamplers.perMessage(() -> LogSamplers.limitRate(60000)));
  private static final String SERVICE_NAME = "log.buffer.recovery";

  private final List<LogBufferProcessorPipeline> pipelines;
  private final List<CheckpointManager<LogBufferFileOffset>> checkpointManagers;
  private final String baseLogDir;
  private final int batchSize;
  private final CountDownLatch stopLatch;

  private LogBufferReader reader;
  private volatile boolean stopped;

  LogBufferRecoveryService(CConfiguration cConf, List<LogBufferProcessorPipeline> pipelines,
                           List<CheckpointManager<LogBufferFileOffset>> checkpointManagers) {
    this(pipelines, checkpointManagers, cConf.get(Constants.LogBuffer.LOG_BUFFER_BASE_DIR),
         cConf.getInt(Constants.LogBuffer.LOG_BUFFER_RECOVERY_BATCH_SIZE));
  }

  @VisibleForTesting
  LogBufferRecoveryService(List<LogBufferProcessorPipeline> pipelines,
                           List<CheckpointManager<LogBufferFileOffset>> checkpointManager,
                           String baseLogDir, int batchSize) {
    this.pipelines = pipelines;
    this.checkpointManagers = checkpointManager;
    this.baseLogDir = baseLogDir;
    this.batchSize = batchSize;
    this.stopLatch = new CountDownLatch(1);
  }

  @Override
  protected void startUp() throws Exception {
    // get the smallest offset of all the log pipelines
    LogBufferFileOffset minOffset = getSmallestOffset(checkpointManagers);
    this.reader = new LogBufferReader(baseLogDir, batchSize, minOffset.getFileId(), minOffset.getFilePos());
  }

  @Override
  protected void run() throws Exception {
    // check if the log buffer dir exists, this could happen if log saver is starting for the first time.
    if (!exists(baseLogDir)) {
      return;
    }

    List<LogBufferEvent> logBufferEvents = new LinkedList<>();
    boolean hasReadEvents = true;
    while (!stopped && hasReadEvents) {
      try {
        hasReadEvents = reader.readEvents(logBufferEvents) > 0;
        recoverLogs(logBufferEvents, pipelines);
      } catch (IOException e) {
        // even though error occurred while reading, whatever logs were read, those should be processed.
        recoverLogs(logBufferEvents, pipelines);
        OUTAGE_LOG.warn("Failed to recover logs from log buffer. Read will be retried.", e);
        // in case of failure to read, sleep and then retry
        stopLatch.await(500, TimeUnit.MILLISECONDS);
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    if (reader != null) {
      reader.close();
    }
  }

  @Override
  protected void triggerShutdown() {
    stopped = true;
    stopLatch.countDown();
  }

  @Override
  protected String getServiceName() {
    return SERVICE_NAME;
  }

  @Override
  protected Executor executor() {
    return MoreExecutors.sameThreadExecutor();
  }

  private LogBufferFileOffset getSmallestOffset(List<CheckpointManager<LogBufferFileOffset>> checkpointManagers)
    throws IOException {
    // there will be atleast one log pipeline
    LogBufferFileOffset minOffset = checkpointManagers.get(0).getCheckpoint(0).getOffset();

    for (int i = 1; i < checkpointManagers.size(); i++) {
      LogBufferFileOffset offset = checkpointManagers.get(i).getCheckpoint(0).getOffset();
      // keep track of minimum offset
      minOffset = minOffset.compareTo(offset) > 0 ? offset : minOffset;
    }

    return minOffset;
  }

  private boolean exists(String baseLogDir) {
    File baseDir = new File(baseLogDir);
    return baseDir.exists();
  }

  private void recoverLogs(List<LogBufferEvent> logBufferEvents, List<LogBufferProcessorPipeline> pipelines) {
    for (LogBufferProcessorPipeline pipeline : pipelines) {
      pipeline.processLogEvents(logBufferEvents.iterator());
    }
    logBufferEvents.clear();
  }
}
