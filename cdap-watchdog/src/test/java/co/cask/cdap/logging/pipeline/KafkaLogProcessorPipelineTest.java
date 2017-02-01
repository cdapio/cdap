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

package co.cask.cdap.logging.pipeline;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.AppenderBase;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.kafka.KafkaTester;
import co.cask.cdap.logging.appender.kafka.LoggingEventSerializer;
import co.cask.cdap.logging.context.GenericLoggingContext;
import co.cask.cdap.logging.save.Checkpoint;
import co.cask.cdap.logging.save.CheckpointManager;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import java.io.Flushable;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * Unit-test for {@link KafkaLogProcessorPipeline}.
 */
public class KafkaLogProcessorPipelineTest {

  @ClassRule
  public static final KafkaTester KAFKA_TESTER = new KafkaTester();

  @Test
  public void testBasicSort() throws Exception {
    String topic = "testPipeline";
    LoggerContext loggerContext = createLoggerContext("WARN", ImmutableMap.of("test.logger", "INFO"),
                                                      TestAppender.class.getName());
    final TestAppender appender = (TestAppender) loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).getAppender("Test");
    TestCheckpointManager checkpointManager = new TestCheckpointManager();
    KafkaPipelineConfig config = new KafkaPipelineConfig(topic, Collections.singleton(0), 1024, 300, 1048576, 500);
    KAFKA_TESTER.createTopic(topic, 1);

    KafkaLogProcessorPipeline pipeline = new KafkaLogProcessorPipeline(new EffectiveLevelProvider(loggerContext, 10),
                                                                       appender, checkpointManager,
                                                                       KAFKA_TESTER.getBrokerService(), config);
    // Publish some log messages to Kafka
    pipeline.startAndWait();

    long now = System.currentTimeMillis();
    publishLog(topic, ImmutableList.of(
      createLoggingEvent("test.logger", Level.INFO, "0", now - 1000),
      createLoggingEvent("test.logger", Level.INFO, "2", now - 700),
      createLoggingEvent("test.logger", Level.INFO, "3", now - 500),
      createLoggingEvent("test.logger", Level.INFO, "1", now - 900),
      createLoggingEvent("test.logger", Level.DEBUG, "hidden", now - 600),
      createLoggingEvent("test.logger", Level.INFO, "4", now - 100))
    );

    // Since the messages are published in one batch, the processor should be able to fetch all of them,
    // hence the sorting order should be deterministic.
    // The DEBUG message should get filtered out
    Tasks.waitFor(5, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return appender.getEvents().size();
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(Integer.toString(i), appender.getEvents().poll().getMessage());
    }

    // Now publish large messages that exceed the maximum queue size (1024). It should trigger writing regardless of
    // the event timestamp
    List<ILoggingEvent> events = new ArrayList<>(500);
    now = System.currentTimeMillis();
    for (int i = 0; i < 500; i++) {
      // The event timestamp is 10 seconds in future.
      events.add(createLoggingEvent("test.large.logger", Level.WARN, "Large logger " + i, now + 10000));
    }
    publishLog(topic, events);

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return !appender.getEvents().isEmpty();
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    events.clear();
    events.addAll(appender.getEvents());

    for (int i = 0; i < events.size(); i++) {
      Assert.assertEquals("Large logger " + i, events.get(i).getMessage());
    }

    pipeline.stopAndWait();
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

  /**
   * Publishes multiple log events.
   */
  private void publishLog(String topic, Iterable<ILoggingEvent> events) {
    LoggingContext context = new GenericLoggingContext(NamespaceId.DEFAULT.getNamespace(), "app", "entity");

    KafkaPublisher.Preparer preparer = KAFKA_TESTER.getKafkaClient()
      .getPublisher(KafkaPublisher.Ack.LEADER_RECEIVED, Compression.NONE)
      .prepare(topic);

    LoggingEventSerializer serializer = new LoggingEventSerializer();
    for (ILoggingEvent event : events) {
      preparer.add(ByteBuffer.wrap(serializer.toBytes(event, context)), context.getLogPartition());
    }
    preparer.send();
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
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);
    configurator.doConfigure(new InputSource(new StringReader(writer.toString())));

    return context;
  }

  /**
   * Appender for unit-test.
   */
  public static final class TestAppender extends AppenderBase<ILoggingEvent> implements Flushable {

    private final Queue<ILoggingEvent> events = new ConcurrentLinkedQueue<>();
    private final Queue<ILoggingEvent> pending = new LinkedList<>();

    @Override
    protected void append(ILoggingEvent event) {
      pending.add(event);
    }

    Queue<ILoggingEvent> getEvents() {
      return events;
    }

    @Override
    public void flush() throws IOException {
      events.addAll(pending);
      pending.clear();
    }
  }

  private static final class TestCheckpointManager implements CheckpointManager {

    @Override
    public void saveCheckpoints(Map<Integer, ? extends Checkpoint> checkpoints) throws Exception {

    }

    @Override
    public Map<Integer, Checkpoint> getCheckpoint(Set<Integer> partitions) throws Exception {
      return Collections.emptyMap();
    }

    @Override
    public Checkpoint getCheckpoint(int partition) throws Exception {
      return new Checkpoint(-1, -1);
    }
  }
}
