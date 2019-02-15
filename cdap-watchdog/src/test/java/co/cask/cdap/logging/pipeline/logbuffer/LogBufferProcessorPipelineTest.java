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

package co.cask.cdap.logging.pipeline.logbuffer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.metrics.NoopMetricsContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.logging.logbuffer.LogBufferEvent;
import co.cask.cdap.logging.logbuffer.LogBufferFileOffset;
import co.cask.cdap.logging.meta.Checkpoint;
import co.cask.cdap.logging.meta.CheckpointManager;
import co.cask.cdap.logging.pipeline.LogPipelineTestUtil;
import co.cask.cdap.logging.pipeline.LogProcessorPipelineContext;
import co.cask.cdap.logging.pipeline.MockAppender;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

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
      config, checkpointManager);
    // start the pipeline
    pipeline.startAndWait();

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
    pipeline.stopAndWait();
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
    public void saveCheckpoints(Map<Integer, ? extends Checkpoint<LogBufferFileOffset>> checkpoints) throws Exception {

    }

    @Override
    public Map<Integer, Checkpoint<LogBufferFileOffset>> getCheckpoint(Set<Integer> partitions) throws Exception {
      return Collections.emptyMap();
    }

    @Override
    public Checkpoint<LogBufferFileOffset> getCheckpoint(int partition) throws Exception {
      return new Checkpoint<>(new LogBufferFileOffset(-1, -1), -1);
    }
  }
}
