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

package io.cdap.cdap.logging.logbuffer.recover;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.logging.logbuffer.LogBufferEvent;
import io.cdap.cdap.logging.logbuffer.LogBufferFileOffset;
import io.cdap.cdap.logging.meta.CheckpointManager;
import io.cdap.cdap.logging.pipeline.logbuffer.LogBufferProcessorPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Log buffer recovery service which recovers logs upon log saver restart and sends them to log buffer pipeline for
 * further processing. This service first scans all the files to figure out max file id till which it should recover.
 * This is because while recovery service is running, new files can be created. Recovery service should not recover
 * those logs.
 */
public class LogBufferRecoveryService extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(LogBufferRecoveryService.class);
  // For outage, only log once per 60 seconds per message.
  private static final Logger OUTAGE_LOG =
    Loggers.sampling(LOG, LogSamplers.perMessage(() -> LogSamplers.limitRate(60000)));
  private static final String SERVICE_NAME = "log.buffer.recovery";

  private final List<LogBufferProcessorPipeline> pipelines;
  private final List<CheckpointManager<LogBufferFileOffset>> checkpointManagers;
  private final String baseLogDir;
  private final boolean baseDirExists;
  private final int batchSize;
  private final CountDownLatch stopLatch;
  private final AtomicBoolean startCleanup;

  private LogBufferReader reader;
  private volatile boolean stopped;

  public LogBufferRecoveryService(CConfiguration cConf, List<LogBufferProcessorPipeline> pipelines,
                                  List<CheckpointManager<LogBufferFileOffset>> checkpointManagers,
                                  AtomicBoolean startCleanup) {
    this(pipelines, checkpointManagers, cConf.get(Constants.LogBuffer.LOG_BUFFER_BASE_DIR),
         cConf.getInt(Constants.LogBuffer.LOG_BUFFER_RECOVERY_BATCH_SIZE), startCleanup);
  }

  @VisibleForTesting
  LogBufferRecoveryService(List<LogBufferProcessorPipeline> pipelines,
                           List<CheckpointManager<LogBufferFileOffset>> checkpointManager,
                           String baseLogDir, int batchSize, AtomicBoolean startCleanup) {
    this.pipelines = pipelines;
    this.checkpointManagers = checkpointManager;
    this.baseLogDir = baseLogDir;
    this.baseDirExists = dirExists(baseLogDir);
    this.batchSize = batchSize;
    this.stopLatch = new CountDownLatch(1);
    this.startCleanup = startCleanup;
  }

  @Override
  protected void startUp() throws Exception {
    if (baseDirExists) {
      // get the smallest offset of all the log pipelines
      LogBufferFileOffset minOffset = getSmallestOffset(checkpointManagers);
      this.reader = new LogBufferReader(baseLogDir, batchSize, getMaxFileId(baseLogDir),
                                        minOffset.getFileId(), minOffset.getFilePos());
    }
  }

  @Override
  protected void run() throws Exception {
    if (baseDirExists) {
      List<LogBufferEvent> logBufferEvents = new LinkedList<>();
      boolean hasReadEvents = true;
      while (!stopped && hasReadEvents) {
        try {
          hasReadEvents = reader.readEvents(logBufferEvents) > 0;
          recoverLogs(logBufferEvents, pipelines);
        } catch (Exception e) {
          // even though error occurred while reading, whatever logs were read, those should be processed. This is
          // because recovery service should be finished quickly so that the logs are persisted in almost sorted order.
          recoverLogs(logBufferEvents, pipelines);
          OUTAGE_LOG.warn("Failed to recover logs from log buffer. Read will be retried.", e);
          // in case of failure to read, sleep and then retry
          stopLatch.await(500, TimeUnit.MILLISECONDS);
        }
      }
    }
    startCleanup.set(true);
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

  private void recoverLogs(List<LogBufferEvent> logBufferEvents, List<LogBufferProcessorPipeline> pipelines) {
    for (LogBufferProcessorPipeline pipeline : pipelines) {
      pipeline.processLogEvents(logBufferEvents.iterator());
    }
    logBufferEvents.clear();
  }

  private long getMaxFileId(String baseDir) {
    long maxFileId = -1;
    File[] files = new File(baseDir).listFiles();
    if (files != null) {
      for (File file : files) {
        String[] splitted = file.getName().split("\\.");
        long fileId = Long.parseLong(splitted[0]);
        if (maxFileId < fileId) {
          maxFileId = fileId;
        }
      }
    }

    return maxFileId;
  }

  private boolean dirExists(String baseLogDir) {
    File baseDir = new File(baseLogDir);
    return baseDir.exists();
  }
}
