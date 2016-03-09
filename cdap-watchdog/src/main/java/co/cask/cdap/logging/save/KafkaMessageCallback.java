/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.logging.save;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.appender.kafka.LoggingEventSerializer;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.kafka.KafkaLogEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Kafka Message callback that fetches log from kafka and calls the process method for all the plugins configured.
 */
public class KafkaMessageCallback implements KafkaConsumer.MessageCallback {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageCallback.class);

  private final Set<KafkaLogProcessor> kafkaLogProcessors;
  private final LoggingEventSerializer serializer;
  private final CountDownLatch stopLatch;
  private final MetricsContext metricsContext;

  public KafkaMessageCallback(CountDownLatch stopLatch,
                              Set<KafkaLogProcessor> kafkaLogProcessors,
                              MetricsContext metricsContext) throws Exception {
    this.kafkaLogProcessors = kafkaLogProcessors;
    this.serializer = new LoggingEventSerializer();
    this.stopLatch = stopLatch;
    this.metricsContext = metricsContext;
  }

  @Override
  public void onReceived(Iterator<FetchedMessage> messages) {

    try {
      if (stopLatch.await(50, TimeUnit.MICROSECONDS)) {
        // if count down occurred return
        LOG.info("Returning since callback is cancelled.");
        return;
      }
    } catch (InterruptedException e) {
      LOG.error("Exception: ", e);
      Thread.currentThread().interrupt();
      return;
    }

    int count = 0;

    long oldestProcessed = Long.MAX_VALUE;
    while (messages.hasNext()) {
      FetchedMessage message = messages.next();
      try {
        GenericRecord genericRecord = serializer.toGenericRecord(message.getPayload());
        ILoggingEvent event = serializer.fromGenericRecord(genericRecord);

        LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(event.getMDCPropertyMap());
        KafkaLogEvent logEvent = new KafkaLogEvent(genericRecord, event, loggingContext,
                                                   message.getTopicPartition().getPartition(),
                                                   message.getNextOffset());

        for (KafkaLogProcessor processor : kafkaLogProcessors) {
          try {
            processor.process(logEvent);
          } catch (Throwable th) {
            LOG.error("Error processing kafka log event in processor {}",
                      processor.getClass().getSimpleName());
          }
        }

        if (event.getTimeStamp() < oldestProcessed) {
          oldestProcessed = event.getTimeStamp();
        }
      } catch (Throwable th) {
        LOG.error("Error processing message at topic {} parition {}",
                  message.getTopicPartition().getTopic(),
                  message.getTopicPartition().getPartition());
      }

      count++;
    }

    if (count > 0) {
      // todo: use hostogram when available (CDAP-3120)
      metricsContext.gauge(Constants.Metrics.Name.Log.PROCESS_DELAY, System.currentTimeMillis() - oldestProcessed);
      metricsContext.increment(Constants.Metrics.Name.Log.PROCESS_MESSAGES_COUNT, count);
    }

    LOG.trace("Got {} messages from kafka", count);
  }

  @Override
  public void finished() {
    LOG.info("KafkaMessageCallback finished.");
  }
}
