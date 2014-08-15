/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.app.queue.InputDatum;
import co.cask.cdap.app.queue.QueueReader;

import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link QueueReader} that always returns the same {@link InputDatum}.
 * Each {@link #dequeue(long, java.util.concurrent.TimeUnit)} call would also increment
 * the retry count of the given {@link InputDatum} by 1.
 *
 * @param <T> Type of input dequeued from this reader.
 */
public class SingleItemQueueReader<T> implements QueueReader<T> {

  private final InputDatum<T> input;

  public SingleItemQueueReader(InputDatum<T> input) {
    this.input = input;
  }

  @Override
  public InputDatum<T> dequeue(long timeout, TimeUnit timeoutUnit) {
    input.incrementRetry();
    return input;
  }
}
