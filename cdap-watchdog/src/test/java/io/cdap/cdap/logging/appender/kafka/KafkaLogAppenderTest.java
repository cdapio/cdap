/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.logging.LoggingConfiguration;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link KafkaLogAppender} start/stop lifecycle.
 */
public class KafkaLogAppenderTest {

  @Test
  public void testStartRestartAndStopLifecycle() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "127.0.0.1:1");
    cConf.setInt(Constants.Logging.NUM_PARTITIONS, 8);
    cConf.set(Constants.Logging.KAFKA_TOPIC, "logs");
    cConf.set(Constants.Logging.LOG_PUBLISH_PARTITION_KEY, "program");
    cConf.set("system.log.process.retry.policy.type", "none");

    LoggerContext loggerContext = new LoggerContext();
    KafkaLogAppender appender = new KafkaLogAppender(cConf);
    appender.setContext(loggerContext);

    LoggingEvent event = new LoggingEvent();
    event.setLevel(Level.INFO);
    event.setLoggerName("io.cdap.KafkaAppenderTest");
    event.setMessage("wakeup");
    event.setTimeStamp(1700000000000L);
    event.setCallerData(new StackTraceElement[0]);
    LogMessage logMessage = new LogMessage(
        event, new WorkerLoggingContext("default", "app1", "w1", "r1", "0"));

    Assert.assertFalse(appender.isStarted());
    appender.start();
    Assert.assertTrue(appender.isStarted());
    // Wake the publisher's poll(10s) so restart stops the previous publisher without waiting 10s
    appender.append(logMessage);

    // Starting again stops the previous publisher via Optional.ifPresent and starts a new one.
    appender.start();
    Assert.assertTrue(appender.isStarted());
    appender.append(logMessage);

    appender.stop();
    Assert.assertFalse(appender.isStarted());

    // Stopping a second time when publisher is already null should be a safe no-op.
    appender.stop();
    Assert.assertFalse(appender.isStarted());
  }
}
