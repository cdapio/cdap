/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Log appender that publishes log messages to Kafka.
 */
public final class KafkaLogAppender extends LogAppender {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaLogAppender.class);

  private static final String APPENDER_NAME = "KafkaLogAppender";
  private final SimpleKafkaProducer producer;
  private final LoggingEventSerializer loggingEventSerializer;

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  @Inject
  KafkaLogAppender(CConfiguration cConf) {
    setName(APPENDER_NAME);
    addInfo("Initializing KafkaLogAppender...");

    this.producer = new SimpleKafkaProducer(cConf);
    try {
      this.loggingEventSerializer = new LoggingEventSerializer();
    } catch (IOException e) {
      addError("Error initializing KafkaLogAppender.", e);
      throw Throwables.propagate(e);
    }
    addInfo("Successfully initialized KafkaLogAppender.");
  }

  @Override
  protected void append(LogMessage logMessage) {
    try {
      byte [] bytes = loggingEventSerializer.toBytes(logMessage.getLoggingEvent(), logMessage.getLoggingContext());
      producer.publish(logMessage.getLoggingContext().getLogPartition(), bytes);
    } catch (Throwable t) {
      LOG.error("Got exception while serializing log event {}.", logMessage.getLoggingEvent(), t);
    }
  }

  @Override
  public void stop() {
    if (!stopped.compareAndSet(false, true)) {
      return;
    }

    super.stop();
    producer.stop();
  }
}
