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

package co.cask.cdap.logging.logbuffer.handler;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.metrics.NoopMetricsContext;
import co.cask.cdap.common.HttpExceptionHandler;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.appender.remote.RemoteLogAppender;
import co.cask.cdap.logging.context.WorkerLoggingContext;
import co.cask.cdap.logging.logbuffer.ConcurrentLogBufferWriter;
import co.cask.cdap.logging.logbuffer.MockCheckpointManager;
import co.cask.cdap.logging.pipeline.LogPipelineTestUtil;
import co.cask.cdap.logging.pipeline.LogProcessorPipelineContext;
import co.cask.cdap.logging.pipeline.MockAppender;
import co.cask.cdap.logging.pipeline.logbuffer.LogBufferPipelineConfig;
import co.cask.cdap.logging.pipeline.logbuffer.LogBufferProcessorPipeline;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link LogBufferHandler}.
 */
public class LogBufferHandlerTest {
  private static final MetricsContext NO_OP_METRICS_CONTEXT = new NoopMetricsContext();

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testHandler() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    String absolutePath = TMP_FOLDER.newFolder().getAbsolutePath();
    cConf.set(Constants.LogBuffer.LOG_BUFFER_BASE_DIR, absolutePath);
    cConf.setLong(Constants.LogBuffer.LOG_BUFFER_MAX_FILE_SIZE_BYTES, 100000);

    LoggerContext loggerContext = LogPipelineTestUtil
      .createLoggerContext("WARN", ImmutableMap.of("test.logger", "INFO"), MockAppender.class.getName());
    final MockAppender appender =
      LogPipelineTestUtil.getAppender(loggerContext.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME),
                                      "Test", MockAppender.class);

    LogBufferProcessorPipeline pipeline = getLogPipeline(loggerContext);
    pipeline.startAndWait();

    ConcurrentLogBufferWriter writer = new ConcurrentLogBufferWriter(cConf, ImmutableList.of(pipeline));

    NettyHttpService httpService = NettyHttpService.builder("RemoteAppenderTest")
      .setHttpHandlers(new LogBufferHandler(writer))
      .setExceptionHandler(new HttpExceptionHandler())
      .build();

    httpService.start();

    RemoteLogAppender remoteLogAppender = getRemoteAppender(cConf, httpService);
    remoteLogAppender.start();

    List<ILoggingEvent> events = getLoggingEvents();
    WorkerLoggingContext loggingContext =
      new WorkerLoggingContext("default", "app1", "worker1", "run1", "instance1");
    for (int i = 0; i < 1000; i++) {
      remoteLogAppender.append(new LogMessage(events.get(i % events.size()), loggingContext));
    }

    Tasks.waitFor(1000, () -> appender.getEvents().size(), 120, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    remoteLogAppender.stop();
    httpService.stop();
    pipeline.stopAndWait();
    loggerContext.stop();
  }

  private RemoteLogAppender getRemoteAppender(CConfiguration cConf, NettyHttpService httpService) {
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    discoveryService.register(new Discoverable(Constants.Service.LOG_BUFFER_SERVICE, httpService.getBindAddress()));
    return new RemoteLogAppender(cConf, discoveryService);
  }

  private LogBufferProcessorPipeline getLogPipeline(LoggerContext loggerContext) {
    MockCheckpointManager checkpointManager = new MockCheckpointManager();
    LogBufferPipelineConfig config = new LogBufferPipelineConfig(1024L, 300L, 500L, 4);
    loggerContext.start();
    return new LogBufferProcessorPipeline(
      new LogProcessorPipelineContext(CConfiguration.create(), "test", loggerContext, NO_OP_METRICS_CONTEXT, 0),
      config, checkpointManager, 0);
  }

  private ImmutableList<ILoggingEvent> getLoggingEvents() {
    long now = System.currentTimeMillis();
    return ImmutableList.of(
      createLoggingEvent("test.logger", Level.INFO, "0", now - 1000),
      createLoggingEvent("test.logger", Level.INFO, "1", now - 900),
      createLoggingEvent("test.logger", Level.INFO, "2", now - 700),
      createLoggingEvent("test.logger", Level.INFO, "4", now - 500),
      createLoggingEvent("test.logger", Level.INFO, "5", now - 100));
  }

  /**
   * Creates a new {@link ILoggingEvent} with the given information.
   */
  private static ILoggingEvent createLoggingEvent(String loggerName, Level level, String message, long timestamp) {
    LoggingEvent event = new LoggingEvent();
    event.setLevel(level);
    event.setLoggerName(loggerName);
    event.setMessage(message);
    event.setTimeStamp(timestamp);
    event.setCallerData(new StackTraceElement[]{
      new StackTraceElement("com.Class1", "methodName1", "fileName1", 10),
      null,
      new StackTraceElement("com.Class2", "methodName2", "fileName2", 20),
      new StackTraceElement("com.Class3",  "methodName3", null, 30),
      null
    });
    return event;
  }
}
