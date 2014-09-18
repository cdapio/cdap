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
package co.cask.cdap.internal.app.queue;

import co.cask.cdap.api.flow.flowlet.InputContext;
import co.cask.cdap.app.queue.InputDatum;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.DequeueResult;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation for {@link InputDatum} for common operations across queue and stream
 *
 * @param <T> Type of input.
 */
final class BasicInputDatum<S, T> implements InputDatum<T> {

  private final DequeueResult<S> result;
  private final AtomicInteger retry;
  private final InputContext inputContext;
  private final QueueName queueName;
  private final Iterable<T> events;

  BasicInputDatum(final QueueName queueName, DequeueResult<S> result, Function<S, T> decoder) {
    this.result = result;
    this.retry = new AtomicInteger(0);
    this.queueName = queueName;
    // Memorize the transformed Iterable so that decoder would only invoked once for each event no matter
    // how many times iterator() is called. This is to save time as well as a need for the case where
    // metrics has been logged inside the decoder.
    this.events = result.isEmpty() ? ImmutableList.<T>of() : ImmutableList.copyOf(Iterables.transform(result, decoder));
    this.inputContext = new InputContext() {
      @Override
      public String getOrigin() {
        return queueName.getSimpleName();
      }

      @Override
      public int getRetryCount() {
        return retry.get();
      }

      @Override
      public String toString() {
        return Objects.toStringHelper(InputContext.class)
          .add("queue", queueName)
          .toString();
      }
    };
  }

  @Override
  public boolean needProcess() {
    return !result.isEmpty();
  }

  @Override
  public Iterator<T> iterator() {
    return events.iterator();
  }

  @Override
  public void incrementRetry() {
    retry.incrementAndGet();
  }

  @Override
  public int getRetry() {
    return retry.get();
  }

  @Override
  public InputContext getInputContext() {
    return inputContext;
  }

  @Override
  public QueueName getQueueName() {
    return queueName;
  }

  @Override
  public void reclaim() {
    result.reclaim();
  }

  @Override
  public int size() {
    return result.size();
  }

  @Override
  public String toString() {
    return String.format("%s %d", result, retry.get());
  }
}
