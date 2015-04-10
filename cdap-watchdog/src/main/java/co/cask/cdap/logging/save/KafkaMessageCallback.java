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

import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Kafka callback to fetch log messages and store them in time buckets per logging context.
 */
public class KafkaMessageCallback implements KafkaConsumer.MessageCallback {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageCallback.class);

  private final CountDownLatch kafkaCancelCallbackLatch;
  private final Set<LogMessageProcessor> logMessageProcessors;

  public KafkaMessageCallback(CountDownLatch kafkaCancelCallbackLatch,
                              Set<LogMessageProcessor> logMessageProcessors) {
    this.kafkaCancelCallbackLatch = kafkaCancelCallbackLatch;
    this.logMessageProcessors = logMessageProcessors;
  }

  @Override
  public void onReceived(Iterator<FetchedMessage> messages) {

    try {
      if (kafkaCancelCallbackLatch.await(50, TimeUnit.MICROSECONDS)) {
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

    while (messages.hasNext()) {
      FetchedMessage message = messages.next();

      for (LogMessageProcessor processor : logMessageProcessors) {
        processor.process(message);
      }

      count++;
    }
    LOG.trace("Got {} messages from kafka", count);
  }

  @Override
  public void finished() {
    LOG.info("KafkaMessageCallback finished.");
  }
}
