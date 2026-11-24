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

package io.cdap.cdap.logging.appender.tms;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.proto.id.NamespaceId;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link TMSLogAppender} MD5 partitioning and start/stop lifecycle.
 */
public class TMSLogAppenderTest {

  @Test
  public void testDeterministicPartitionsForEightAndSixteenPartitions() {
    // ASCII keys
    Assert.assertEquals(1, TMSLogAppender.partition("default:myApp:worker:myWorker", 8));
    Assert.assertEquals(9, TMSLogAppender.partition("default:myApp:worker:myWorker", 16));

    Assert.assertEquals(1, TMSLogAppender.partition("system:services:appfabric", 8));
    Assert.assertEquals(1, TMSLogAppender.partition("system:services:appfabric", 16));

    Assert.assertEquals(3, TMSLogAppender.partition("prod_ns:etl_pipeline:spark:driver", 8));
    Assert.assertEquals(11, TMSLogAppender.partition("prod_ns:etl_pipeline:spark:driver", 16));

    // Non-ASCII and supplementary Unicode keys
    Assert.assertEquals(0, TMSLogAppender.partition("ns_üñîçødé:app:worker:w1", 8));
    Assert.assertEquals(8, TMSLogAppender.partition("ns_üñîçødé:app:worker:w1", 16));

    Assert.assertEquals(7, TMSLogAppender.partition("ns_日本語:app_λ:mapreduce:mr1", 8));
    Assert.assertEquals(15, TMSLogAppender.partition("ns_日本語:app_λ:mapreduce:mr1", 16));

    Assert.assertEquals(3, TMSLogAppender.partition("ns_🚀:app_é:workflow:wf1", 8));
    Assert.assertEquals(11, TMSLogAppender.partition("ns_🚀:app_é:workflow:wf1", 16));
  }

  @Test
  public void testStartRestartPublishAndStopLifecycle() throws Exception {
    final List<String> publishedTopics = new CopyOnWriteArrayList<>();
    MessagingService messagingService = Mockito.mock(MessagingService.class);
    Mockito.when(messagingService.publish(Mockito.any(StoreRequest.class)))
        .thenAnswer(invocation -> {
          StoreRequest request = invocation.getArgument(0);
          Assert.assertEquals(NamespaceId.SYSTEM, request.getTopicId().getNamespaceId());
          if (request.iterator().hasNext()) {
            publishedTopics.add(request.getTopicId().getTopic());
            java.util.concurrent.TimeUnit.MILLISECONDS.sleep(40);
          }
          return null;
        });

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Logging.TMS_TOPIC_PREFIX, "logs");
    cConf.setInt(Constants.Logging.NUM_PARTITIONS, 16);
    cConf.set(Constants.Logging.LOG_PUBLISH_PARTITION_KEY, "program");

    TMSLogAppender appender = new TMSLogAppender(cConf, messagingService);
    appender.setContext(new LoggerContext());

    LoggingEvent unicodeEvent = new LoggingEvent();
    unicodeEvent.setLevel(Level.INFO);
    unicodeEvent.setLoggerName("io.cdap.TmsTest");
    unicodeEvent.setMessage("unicode message");
    unicodeEvent.setTimeStamp(1700000000000L);
    unicodeEvent.setCallerData(new StackTraceElement[0]);

    Assert.assertFalse(appender.isStarted());
    appender.start();
    appender.append(new LogMessage(
        unicodeEvent, new WorkerLoggingContext("default", "myApp", "worker:myWorker", "run1", "0")));
    // Restarting exercises Optional.ofNullable(...).ifPresent(...) stopping the old publisher
    appender.start();
    Assert.assertTrue(appender.isStarted());

    appender.append(new LogMessage(
        unicodeEvent, new WorkerLoggingContext("ns_üñîçødé", "app", "worker:w1", "run1", "0")));

    appender.stop();
    Assert.assertFalse(appender.isStarted());
    // Second stop when publisher reference is null is a safe no-op
    appender.stop();

    Assert.assertTrue(publishedTopics.contains("logs8"));
  }
}
