/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.logging.appender.standalone;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.api.log.LogProcessor;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.kafka.KafkaLogEvent;
import co.cask.cdap.logging.save.KafkaLogProcessor;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Wrapper around LogAppender and LogProcessor plugins to make plugins work in standalone.
 */
public class StandaloneLogAppender extends LogAppender {

  private final LogAppender appender;
  private final Set<KafkaLogProcessor> plugins;
  private final List<LogProcessor> logProcessorExtensions;
  private final Properties logProcessorExtensionsProperties;

  public StandaloneLogAppender(LogAppender appender, Set<KafkaLogProcessor> plugins,
                               List<LogProcessor> logProcessorExtensions,
                               Properties logProcessorExtensionsProperties) {
    this.appender = appender;
    this.plugins = plugins;
    this.logProcessorExtensions = logProcessorExtensions;
    this.logProcessorExtensionsProperties = logProcessorExtensionsProperties;
    setName("standalone-log-appender");
  }

  @Override
  protected void append(LogMessage logMessage) {
    appender.append(logMessage);
    Iterator<KafkaLogEvent> eventIterator = Iterators.singletonIterator(getKafkaLogEvent(logMessage));
    for (KafkaLogProcessor plugin : plugins) {
      plugin.process(eventIterator);
    }

    for (LogProcessor logProcessorExtension : logProcessorExtensions) {
      logProcessorExtension.process(Iterators.transform(eventIterator, new Function<KafkaLogEvent, ILoggingEvent>() {
        @Nullable
        @Override
        public ILoggingEvent apply(@Nullable KafkaLogEvent input) {
          return input.getLogEvent();
        }
      }));
    }
  }

  @Override
  public void doAppend(ILoggingEvent eventObject) {
    append(eventObject);
  }

  @Override
  public void start() {
    for (LogProcessor logProcessorExtension : logProcessorExtensions) {
      logProcessorExtension.initialize(logProcessorExtensionsProperties);
    }
    appender.start();
  }

  @Override
  public void stop() {
    for (LogProcessor logProcessorExtension : logProcessorExtensions) {
      logProcessorExtension.stop();
    }
    appender.stop();
  }

  private KafkaLogEvent getKafkaLogEvent(LogMessage message) {
    // Create a Kafkalog event based on ILoggingEvent, dummy values for GenericRecord, partition and offset.
    return new KafkaLogEvent(null, message, message.getLoggingContext(), 0, -1);
  }
}
