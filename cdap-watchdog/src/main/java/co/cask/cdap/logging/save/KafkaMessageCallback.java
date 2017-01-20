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
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Kafka Message callback that fetches log from kafka and calls the process method for all the plugins configured.
 */
public class KafkaMessageCallback implements KafkaConsumer.MessageCallback {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageCallback.class);

  private final int partition;
  private final Set<KafkaLogProcessor> kafkaLogProcessors;
  private final LoggingEventSerializer serializer;
  private final CountDownLatch stopLatch;
  private final MetricsContext metricsContext;
  private final String delayMetric;

  public KafkaMessageCallback(int partition, CountDownLatch stopLatch,
                              Set<KafkaLogProcessor> kafkaLogProcessors,
                              MetricsContext metricsContext) throws Exception {
    this.partition = partition;
    this.kafkaLogProcessors = kafkaLogProcessors;
    this.serializer = new LoggingEventSerializer();
    this.stopLatch = stopLatch;
    this.metricsContext = metricsContext;
    this.delayMetric = Constants.Metrics.Name.Log.PROCESS_DELAY + "." + partition;
  }

  @Override
  public long onReceived(Iterator<FetchedMessage> messages) {

    try {
      if (stopLatch.await(1, TimeUnit.NANOSECONDS)) {
        // if count down occurred return
        LOG.debug("Returning since callback is cancelled.");
        return 0L;
      }
    } catch (InterruptedException e) {
      LOG.error("Exception: ", e);
      Thread.currentThread().interrupt();
      return 0L;
    }

    long nextOffset = 0L;
    long oldestProcessed = Long.MAX_VALUE;
    List<KafkaLogEvent> events = Lists.newArrayList();
    while (messages.hasNext()) {
      FetchedMessage message = messages.next();
      nextOffset = message.getNextOffset();
      try {
        GenericRecord genericRecord = serializer.toGenericRecord(message.getPayload());
        ILoggingEvent event = serializer.fromGenericRecord(genericRecord);
        LOG.trace("Got event {} for partition {}", event, partition);

        LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(event.getMDCPropertyMap());
        KafkaLogEvent logEvent = new KafkaLogEvent(genericRecord, event, loggingContext,
                                                   message.getTopicPartition().getPartition(),
                                                   message.getNextOffset());
        events.add(logEvent);
        if (event.getTimeStamp() < oldestProcessed) {
          oldestProcessed = event.getTimeStamp();
        }
      } catch (Throwable th) {
        LOG.warn("Message with next offset {} ignored due to exception for topic {} parition {}",
                 message.getNextOffset(),
                 message.getTopicPartition().getTopic(),
                 message.getTopicPartition().getPartition(), th);
      }
    }

    int count = events.size();
    if (!events.isEmpty()) {
      for (KafkaLogProcessor processor : kafkaLogProcessors) {
        try {
          processor.process(events.iterator());
        } catch (Throwable th) {
          LOG.warn("Exception processing {} kafka log events in processor {}, ignoring the exception",
                   events.size(), processor.getClass().getSimpleName(), th);
        }
      }

      // todo: use hostogram when available (CDAP-3120)
      metricsContext.gauge(delayMetric, System.currentTimeMillis() - oldestProcessed);
      metricsContext.increment(Constants.Metrics.Name.Log.PROCESS_MESSAGES_COUNT, count);
    }

    LOG.trace("Got {} messages from kafka", count);
    return nextOffset;
  }

  @Override
  public void finished() {
    LOG.info("KafkaMessageCallback finished for partition {}.", partition);
  }
}
