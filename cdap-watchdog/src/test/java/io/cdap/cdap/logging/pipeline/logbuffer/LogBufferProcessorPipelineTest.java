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

package io.cdap.cdap.logging.pipeline.logbuffer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.logging.logbuffer.LogBufferEvent;
import io.cdap.cdap.logging.logbuffer.LogBufferFileOffset;
import io.cdap.cdap.logging.meta.Checkpoint;
import io.cdap.cdap.logging.meta.CheckpointManager;
import io.cdap.cdap.logging.pipeline.LogPipelineTestUtil;
import io.cdap.cdap.logging.pipeline.LogProcessorPipelineContext;
import io.cdap.cdap.logging.pipeline.MockAppender;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Tests for {@link LogBufferProcessorPipeline}.
 */
public class LogBufferProcessorPipelineTest {
  private static final LoggingEventSerializer serializer = new LoggingEventSerializer();
  private static final MetricsContext NO_OP_METRICS_CONTEXT = new NoopMetricsContext();

  @Test
  public void testSingleAppender() throws Exception {
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

    // start thread to write to incomingEventQueue
    List<ILoggingEvent> events = getLoggingEvents();
    AtomicInteger i = new AtomicInteger(0);
    List<LogBufferEvent> bufferEvents = events.stream().map(event -> {
      LogBufferEvent lbe = new LogBufferEvent(event, serializer.toBytes(event).length,
                                              new LogBufferFileOffset(0, i.get()));
      i.incrementAndGet();
      return lbe;
    }).collect(Collectors.toList());

    // start a thread to send log buffer events to pipeline
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.execute(() -> {
      for (int count = 0; count < 40; count++) {
        pipeline.processLogEvents(bufferEvents.iterator());
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // should not happen
        }
      }
    });

    // wait for pipeline to append all the logs to appender. The DEBUG message should get filtered out.
    Tasks.waitFor(200, () -> appender.getEvents().size(), 60, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    executorService.shutdown();
    pipeline.stopAsync().awaitTerminated();
    loggerContext.stop();
  }

  private ImmutableList<ILoggingEvent> getLoggingEvents() {
    long now = System.currentTimeMillis();
    return ImmutableList.of(
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "0", now - 1000),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "1", now - 900),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "2", now - 700),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.DEBUG, "3", now - 600),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "4", now - 500),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "5", now - 100));
  }

  /**
   * Checkpoint manager for unit tests.
   */
  private static final class MockCheckpointManager implements CheckpointManager<LogBufferFileOffset> {
    @Override
    public void saveCheckpoints(Map<Integer, ? extends Checkpoint<LogBufferFileOffset>> checkpoints)
      throws IOException {

    }

    @Override
    public Map<Integer, Checkpoint<LogBufferFileOffset>> getCheckpoint(Set<Integer> partitions) throws IOException {
      return Collections.emptyMap();
    }

    @Override
    public Checkpoint<LogBufferFileOffset> getCheckpoint(int partition) throws IOException {
      return new Checkpoint<>(new LogBufferFileOffset(-1, -1), -1);
    }
  }
}
