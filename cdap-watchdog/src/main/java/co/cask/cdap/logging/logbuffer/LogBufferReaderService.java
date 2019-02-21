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

import co.cask.cdap.logging.pipeline.logbuffer.LogBufferProcessorPipeline;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Log reader which will read log buffer files and process them.
 */
public class LogBufferReaderService extends AbstractExecutionThreadService {
  private static final String SERVICE_NAME = "log.buffer.reader";
  private final String baseLogDir;
  private final int batchSize;
  private final List<LogBufferProcessorPipeline> pipelines;
  private final CountDownLatch stopLatch;

  private LogBufferReader reader;
  private volatile boolean stopped;

  public LogBufferReaderService(List<LogBufferProcessorPipeline> pipelines, String baseLogDir, int batchSize) {
    this.baseLogDir = baseLogDir;
    this.batchSize = batchSize;
    this.pipelines = pipelines;
    this.stopLatch = new CountDownLatch(1);
  }

  @Override
  protected void startUp() throws Exception {
    // get the smallest offset of all the log pipelines
    LogBufferFileOffset minOffset = getSmallestOffset();
    this.reader = new LogBufferReader(baseLogDir, batchSize, minOffset.getFileId(), minOffset.getFilePos());
  }

  @Override
  protected void run() throws Exception {
    if (!shouldRead()) {
      return;
    }

    List<LogBufferEvent> logBufferEvents = new LinkedList<>();

    while (!stopped) {
      if (reader.readEvents(logBufferEvents) == 0) {
        break;
      }
      for (LogBufferProcessorPipeline pipeline : pipelines) {
        // TODO the pipeline should discard events that have lower offset than what has been processed by that pipeline.
        pipeline.processLogEvents(logBufferEvents.iterator());
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    reader.close();
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

  private LogBufferFileOffset getSmallestOffset() throws IOException {
    // there will be atleast one log pipeline
    LogBufferFileOffset minOffset = pipelines.get(0).getSmallestCheckpointOffset();

    for (int i = 1; i < pipelines.size(); i++) {
      LogBufferFileOffset offset = pipelines.get(i).getSmallestCheckpointOffset();
      // keep track of minimum offset
      minOffset = minOffset.compareTo(offset) > 0 ? offset : minOffset;
    }

    return minOffset;
  }

  private boolean shouldRead() {
    // check if the log buffer dir exists, this could happen if log saver is starting for the first time.
    File baseDir = new File(baseLogDir);
    return baseDir.exists();
  }
}
