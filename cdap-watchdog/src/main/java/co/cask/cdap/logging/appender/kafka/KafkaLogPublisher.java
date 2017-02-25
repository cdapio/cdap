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

package co.cask.cdap.logging.appender.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.context.WorkflowLoggingContext;
import co.cask.cdap.proto.id.NamespaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Publish logs to Kafka
 **/
public class KafkaLogPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaLogPublisher.class);

  private static KafkaLogAppender kafkaAppender;

  private static CConfiguration cConf;

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Need two arguments: start event time, end event time to run");
    }
    new KafkaLogPublisher().publishLogs(Integer.valueOf(args[0]), Integer.valueOf(args[1]));
  }

  public void publishLogs(int startTime, int endTime) throws Exception {
    init();
    List<ILoggingEvent> events = new ArrayList<>();
    for (int eventTime = startTime; eventTime <= endTime; eventTime++) {
      events.add(createLoggingEvent("test.logger", Level.INFO, "Waiting for Kafka server to startup...", eventTime));
    }
    publishLog(events);
  }

  private static void init() throws Exception {
    cConf = CConfiguration.create();
    kafkaAppender = new KafkaLogAppender(cConf);
    LOG.info("Waiting for Kafka server to startup...");
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
    return event;
  }

  /**
   * Publishes multiple log events.
   */
  private static void publishLog(Iterable<ILoggingEvent> events) {
    publishLog(events, new WorkflowLoggingContext(NamespaceId.DEFAULT.getNamespace(), "FAKEAPP", "WF", "FAKERUN"));
  }

  private static void publishLog(Iterable<ILoggingEvent> events, LoggingContext context) {
    for (ILoggingEvent event : events) {
      kafkaAppender.append(new LogMessage(event, context));
    }
  }
}
