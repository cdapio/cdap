/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.data2.transaction.stream;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.DequeueResult;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Represents a transaction stream consumer. On each call to {@link #poll(int, long, java.util.concurrent.TimeUnit)},
 * it will try ro read from stream starting from where it get initialized with or from what last poll left off.
 *
 * On transaction rollback, all events that are read by poll calls will be reverted so that when poll is issued on a
 * new transaction afterwards, it will start giving stream events from the where last committed poll ended.
 */
public interface StreamConsumer extends Closeable, TransactionAware {

  /**
   * @return Id of the stream this consumer is consuming.
   */
  Id.Stream getStreamId();

  /**
   * @return Configuration of this consumer.
   */
  ConsumerConfig getConsumerConfig();

  /**
   * Retrieves up to {@code maxEvents} of {@link StreamEvent} from the stream.
   *
   * @param maxEvents Maximum number of events to retrieve
   * @param timeout Maximum of time to spend on trying to read up to maxEvents
   * @param timeoutUnit Unit for the timeout
   *
   * @return An instance of {@link DequeueResult} which carries {@link StreamEvent}s inside.
   *
   * @throws IOException If there is error while reading events
   * @throws InterruptedException If interrupted while waiting
   */
  DequeueResult<StreamEvent> poll(int maxEvents,
                                  long timeout, TimeUnit timeoutUnit) throws IOException, InterruptedException;
}
