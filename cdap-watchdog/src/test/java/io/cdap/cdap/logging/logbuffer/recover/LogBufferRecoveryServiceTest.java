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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import io.cdap.cdap.logging.logbuffer.LogBufferWriter;
import io.cdap.cdap.logging.logbuffer.MockCheckpointManager;
import io.cdap.cdap.logging.pipeline.LogPipelineTestUtil;
import io.cdap.cdap.logging.pipeline.LogProcessorPipelineContext;
import io.cdap.cdap.logging.pipeline.MockAppender;
import io.cdap.cdap.logging.pipeline.logbuffer.LogBufferPipelineConfig;
import io.cdap.cdap.logging.pipeline.logbuffer.LogBufferProcessorPipeline;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link LogBufferRecoveryService}.
 */
public class LogBufferRecoveryServiceTest {
  private static final LoggingEventSerializer serializer = new LoggingEventSerializer();
  private static final MetricsContext NO_OP_METRICS_CONTEXT = new NoopMetricsContext();

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testLogBufferRecoveryService() throws Exception {
    String absolutePath = TMP_FOLDER.newFolder().getAbsolutePath();

    // create and start pipeline
    LoggerContext loggerContext = LogPipelineTestUtil.createLoggerContext("WARN",
                                                                          ImmutableMap.of("test.logger", "INFO"),
                                                                          MockAppender.class.getName());
    final MockAppender appender = LogPipelineTestUtil.getAppender(loggerContext.getLogger(Logger.ROOT_LOGGER_NAME),
                                                                  "Test", MockAppender.class);
    MockCheckpointManager checkpointManager = new MockCheckpointManager();
    LogBufferPipelineConfig config = new LogBufferPipelineConfig(1024L, 300L, 500L, 4);
    loggerContext.start();
    LogBufferProcessorPipeline pipeline = new LogBufferProcessorPipeline(
      new LogProcessorPipelineContext(CConfiguration.create(), "test", loggerContext, NO_OP_METRICS_CONTEXT, 0),
      config, checkpointManager, 0);

    // start the pipeline
    pipeline.startAsync().awaitRunning();

    // write directly to log buffer
    LogBufferWriter writer = new LogBufferWriter(absolutePath, 250, () -> { });
    ImmutableList<byte[]> events = getLoggingEvents();
    writer.write(events.iterator()).iterator();
    writer.close();

    // start log buffer reader to read log events from files. keep the batch size as 2 so that there are more than 1
    // iterations
    LogBufferRecoveryService service = new LogBufferRecoveryService(ImmutableList.of(pipeline),
                                                                    ImmutableList.of(checkpointManager),
                                                                    absolutePath, 2, new AtomicBoolean(true));
    service.startAsync().awaitRunning();

    Tasks.waitFor(5, () -> appender.getEvents().size(), 120, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    service.stopAsync().awaitTerminated();
    pipeline.stopAsync().awaitTerminated();
    loggerContext.stop();
  }

  @Test
  public void testServiceNameAndFastCompletionWithoutBaseDir() throws Exception {
    java.io.File missingDir = new java.io.File(TMP_FOLDER.newFolder(), "missing");
    AtomicBoolean startCleanup = new AtomicBoolean(false);
    final java.util.concurrent.atomic.AtomicReference<String> executionThreadName =
        new java.util.concurrent.atomic.AtomicReference<>();

    MockCheckpointManager checkpointManager = new MockCheckpointManager() {
      @Override
      public io.cdap.cdap.logging.meta.Checkpoint<io.cdap.cdap.logging.logbuffer.LogBufferFileOffset>
          getCheckpoint(int partition) throws java.io.IOException {
        executionThreadName.set(Thread.currentThread().getName());
        return super.getCheckpoint(partition);
      }
    };

    // Exercise fast completion when baseDir does not exist
    LogBufferRecoveryService fastService = new LogBufferRecoveryService(
        ImmutableList.of(), ImmutableList.of(checkpointManager),
        missingDir.getAbsolutePath(), 10, startCleanup);
    org.junit.Assert.assertTrue(fastService.toString().startsWith("log.buffer.recovery ["));
    try {
      fastService.startAsync().awaitRunning();
    } catch (IllegalStateException e) {
      if (fastService.state() != com.google.common.util.concurrent.Service.State.STOPPING
          && fastService.state() != com.google.common.util.concurrent.Service.State.TERMINATED) {
        throw e;
      }
    }
    fastService.stopAsync().awaitTerminated();
    org.junit.Assert.assertTrue(startCleanup.get());
    org.junit.Assert.assertEquals("log.buffer.recovery [TERMINATED]", fastService.toString());

    // Exercise execution thread naming when baseDir exists with a valid buffer file
    String existingBufDir = TMP_FOLDER.newFolder().getAbsolutePath();
    try (LogBufferWriter bufWriter = new LogBufferWriter(existingBufDir, 10000L, () -> { })) {
      bufWriter.write(getLoggingEvents().iterator());
    }
    LogBufferRecoveryService namedService = new LogBufferRecoveryService(
        ImmutableList.of(), ImmutableList.of(checkpointManager),
        existingBufDir, 10, new AtomicBoolean(false));
    namedService.startAsync().awaitRunning();
    namedService.stopAsync().awaitTerminated();
    org.junit.Assert.assertEquals("log.buffer.recovery", executionThreadName.get());
  }

  private ImmutableList<byte[]> getLoggingEvents() {
    WorkerLoggingContext loggingContext =
      new WorkerLoggingContext("default", "app1", "worker1", "run1", "instance1");
    long now = System.currentTimeMillis();
    return ImmutableList.of(
      serializer.toBytes(createLoggingEvent("test.logger", Level.INFO, "0", now - 1000, loggingContext)),
      serializer.toBytes(createLoggingEvent("test.logger", Level.INFO, "1", now - 900, loggingContext)),
      serializer.toBytes(createLoggingEvent("test.logger", Level.INFO, "2", now - 700, loggingContext)),
      serializer.toBytes(createLoggingEvent("test.logger", Level.DEBUG, "3", now - 600, loggingContext)),
      serializer.toBytes(createLoggingEvent("test.logger", Level.INFO, "4", now - 500, loggingContext)),
      serializer.toBytes(createLoggingEvent("test.logger", Level.INFO, "5", now - 100, loggingContext)));
  }

  private ILoggingEvent createLoggingEvent(String loggerName, Level level, String message, long timestamp,
                                           LoggingContext loggingContext) {
    LoggingEvent event = new LoggingEvent();
    event.setLevel(level);
    event.setLoggerName(loggerName);
    event.setMessage(message);
    event.setTimeStamp(timestamp);
    return new LogMessage(event, loggingContext);
  }
}
