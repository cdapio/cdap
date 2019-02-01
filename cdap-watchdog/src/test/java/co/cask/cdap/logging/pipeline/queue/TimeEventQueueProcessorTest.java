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

package co.cask.cdap.logging.pipeline.queue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.AppenderBase;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.metrics.NoopMetricsContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Syncable;
import co.cask.cdap.logging.meta.Checkpoint;
import co.cask.cdap.logging.pipeline.LogPipelineConfigurator;
import co.cask.cdap.logging.pipeline.LogProcessorPipelineContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import java.io.Flushable;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * Tests for {@link TimeEventQueueProcessor}.
 */
public class TimeEventQueueProcessorTest {
  private static final MetricsContext NO_OP_METRICS_CONTEXT = new NoopMetricsContext();

  @Test
  public void test() throws Exception {
    LoggerContext loggerContext = createLoggerContext("WARN", ImmutableMap.of("test.logger", "INFO"),
                                                      TestAppender.class.getName());
    LogProcessorPipelineContext context = new LogProcessorPipelineContext(CConfiguration.create(),
                                                                          "test", loggerContext, NO_OP_METRICS_CONTEXT,
                                                                          0);
    context.start();
    TimeEventQueueProcessor<TestOffset> processor = new TimeEventQueueProcessor<>(context, 50, 1,
                                                                                  ImmutableList.of(0));
    long now = System.currentTimeMillis();
    List<ILoggingEvent> events = ImmutableList.of(
      createLoggingEvent("test.logger", Level.INFO, "1", now - 1000),
      createLoggingEvent("test.logger", Level.INFO, "3", now - 700),
      createLoggingEvent("test.logger", Level.INFO, "5", now - 500),
      createLoggingEvent("test.logger", Level.INFO, "2", now - 900),
      createLoggingEvent("test.logger", Level.ERROR, "4", now - 600),
      createLoggingEvent("test.logger", Level.INFO, "6", now - 100));

    ProcessedEventMetadata<TestOffset> metadata = processor.process(0, events.iterator(), getEventConverter());
    // only 5 events should be processed because max buffer size is 50 and each event is of size 10
    Assert.assertEquals(5, metadata.getTotalEventsProcessed());
    for (Map.Entry<Integer, Checkpoint<TestOffset>> entry : metadata.getCheckpoints().entrySet()) {
      Checkpoint<TestOffset> value = entry.getValue();
      // offset should be max offset processed so far
      Assert.assertEquals(5, value.getOffset().getOffset());
    }
  }

  private Function<ILoggingEvent, ProcessorEvent<TestOffset>> getEventConverter() {
    return event -> new ProcessorEvent<>(event, 10, new TestOffset(Long.parseLong(event.getMessage())));
  }

  /**
   * Creates a new {@link ILoggingEvent} with the given information.
   */
  private ILoggingEvent createLoggingEvent(String loggerName, Level level, String message, long timestamp) {
    LoggingEvent event = new LoggingEvent();
    event.setLevel(level);
    event.setLoggerName(loggerName);
    event.setMessage(message);
    event.setTimeStamp(timestamp);
    return event;
  }

  private LoggerContext createLoggerContext(String rootLevel,
                                            Map<String, String> loggerLevels,
                                            String appenderClassName) throws Exception {
    Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    Element configuration = doc.createElement("configuration");
    doc.appendChild(configuration);

    Element appender = doc.createElement("appender");
    appender.setAttribute("name", "Test");
    appender.setAttribute("class", appenderClassName);
    configuration.appendChild(appender);

    for (Map.Entry<String, String> entry : loggerLevels.entrySet()) {
      Element logger = doc.createElement("logger");
      logger.setAttribute("name", entry.getKey());
      logger.setAttribute("level", entry.getValue());
      configuration.appendChild(logger);
    }

    Element rootLogger = doc.createElement("root");
    rootLogger.setAttribute("level", rootLevel);
    Element appenderRef = doc.createElement("appender-ref");
    appenderRef.setAttribute("ref", "Test");
    rootLogger.appendChild(appenderRef);

    configuration.appendChild(rootLogger);

    Transformer transformer = TransformerFactory.newInstance().newTransformer();
    StringWriter writer = new StringWriter();
    transformer.transform(new DOMSource(doc), new StreamResult(writer));

    LoggerContext context = new LoggerContext();
    JoranConfigurator configurator = new LogPipelineConfigurator(CConfiguration.create());
    configurator.setContext(context);
    configurator.doConfigure(new InputSource(new StringReader(writer.toString())));

    return context;
  }

  /**
   * Appender for unit-test.
   */
  public static final class TestAppender extends AppenderBase<ILoggingEvent> implements Flushable, Syncable {

    private final AtomicInteger flushCount = new AtomicInteger();
    private Queue<ILoggingEvent> pending;
    private Queue<ILoggingEvent> persisted;

    @Override
    protected void append(ILoggingEvent event) {
      pending.add(event);
    }

    Queue<ILoggingEvent> getEvents() {
      return persisted;
    }

    int getFlushCount() {
      return flushCount.get();
    }

    @Override
    public void start() {
      persisted = new ConcurrentLinkedQueue<>();
      pending = new LinkedList<>();
      super.start();
    }

    @Override
    public void stop() {
      persisted = null;
      pending = null;
      super.stop();
    }

    @Override
    public void flush() throws IOException {
      flushCount.incrementAndGet();
    }

    @Override
    public void sync() throws IOException {
      persisted.addAll(pending);
      pending.clear();
    }
  }

  /**
   * Offset for unit-test.
   */
  public static final class TestOffset implements Comparable<TestOffset> {
    private final long offset;

    public TestOffset(long offset) {
      this.offset = offset;
    }

    public long getOffset() {
      return offset;
    }

    @Override
    public int compareTo(TestOffset o) {
      return Long.compare(this.offset, o.offset);
    }
  }
}
